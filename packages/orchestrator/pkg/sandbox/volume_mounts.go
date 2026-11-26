package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	// ErrVolumeMountConflict reports an appended mount whose path equals,
	// contains or lies inside the path of another mount of the sandbox.
	ErrVolumeMountConflict = errors.New("volume mount path conflicts with another mount")
	// ErrEnvdVolumeAppendUnsupported reports an envd that has no /volumes/mount.
	ErrEnvdVolumeAppendUnsupported = errors.New("envd does not support mounting volumes into a running sandbox")
	// ErrCheckpointInFlight reports a sandbox whose checkpoint is in progress.
	ErrCheckpointInFlight = errors.New("sandbox checkpoint is in progress")
	// ErrVolumeMountUnsettled reports a failed append that may have left some
	// of its mounts in the guest; they stay in the config. A retry of the same
	// mounts settles it.
	ErrVolumeMountUnsettled = errors.New("the outcome of mounting the volumes is unknown")
)

// MaxAppendVolumeMounts bounds the mounts of one append, so that its envd call,
// bounded by volumeAppendBaseTimeout + n * volumeAppendPerVolume and holding the
// update lock throughout, ends before the API's request deadline.
const MaxAppendVolumeMounts = 2

const (
	// volumeAppendBaseTimeout covers the request itself; envd's per-volume budget,
	// creating the mount point and both mount attempts, is added for each volume.
	volumeAppendBaseTimeout = 10 * time.Second
	volumeAppendPerVolume   = 25 * time.Second
)

// AppendVolumeMounts mounts volumes into the running sandbox next to the ones it
// has. A mount identical to one it has (same path, same volume) is kept and
// mounted again only if envd lacks it, so a retried call succeeds.
//
// It runs as an update: a stop or pause that takes the sandbox waits for it, so
// a snapshot never sees half an append. It holds the checkpoint marker for the
// same reason, as an in-place checkpoint takes no part in update admission.
//
// The mounts are added to the config before envd mounts them, because the NFS
// proxy refuses a volume the config does not list. They are removed again only
// when envd answers that it failed and left nothing mounted. Without such an
// answer, or with a 500 (a rollback that left mounts), the guest may hold them,
// so they stay: a retry of the same mounts then succeeds, and another volume at
// the same path is refused as a conflict instead of being skipped by envd,
// which would still hold the first. The caller going away does not end the
// call, for the same reason.
//
// Such mounts are remembered as unsettled. A later append that lists one sends
// it to envd as unsettled, so envd mounts it afresh instead of skipping it as
// mounted and its answer covers it: on a failure that left nothing it is
// dropped from the config too, and on success it is settled.
func (s *Sandbox) AppendVolumeMounts(ctx context.Context, mounts []VolumeMountConfig) error {
	return s.RunUpdate(func() error {
		if !s.checkpointInFlight.CompareAndSwap(false, true) {
			return ErrCheckpointInFlight
		}
		defer s.checkpointInFlight.Store(false)

		added, err := newVolumeMounts(s.Config.GetVolumeMounts(), mounts)
		if err != nil {
			return err
		}

		unsettled := s.Config.unsettledAmong(mounts)

		s.Config.addVolumeMounts(added)
		settled, err := s.callEnvdVolumesMount(context.WithoutCancel(ctx), mounts, unsettled)
		switch {
		case err != nil && !settled:
			paths := make([]string, 0, len(added))
			for _, m := range added {
				paths = append(paths, m.Path)
			}
			s.Config.setVolumeMountsUnsettled(paths, true)

			return fmt.Errorf("%w: %w", ErrVolumeMountUnsettled, err)
		case err != nil:
			paths := make(map[string]struct{}, len(added)+len(unsettled))
			for _, m := range added {
				paths[m.Path] = struct{}{}
			}
			for _, path := range unsettled {
				paths[path] = struct{}{}
			}
			s.Config.removeVolumeMounts(paths)
			s.Config.setVolumeMountsUnsettled(unsettled, false)
			if s.sandboxes != nil {
				s.sandboxes.volumeMountsRemoved(context.WithoutCancel(ctx), s)
			}

			return err
		default:
			paths := make([]string, 0, len(mounts))
			for _, m := range mounts {
				paths = append(paths, m.Path)
			}
			s.Config.setVolumeMountsUnsettled(paths, false)

			return nil
		}
	})
}

// newVolumeMounts returns the mounts not yet in existing, and an error when any
// requested path conflicts with another mount, existing or requested.
func newVolumeMounts(existing, requested []VolumeMountConfig) ([]VolumeMountConfig, error) {
	var added []VolumeMountConfig

	for i, m := range requested {
		duplicate := false
		for _, e := range existing {
			// The guest mounts a volume by name and the NFS proxy serves the
			// first mount of that name, so one name must mean one volume. A
			// deleted volume's name can be taken again at once.
			if e.Name == m.Name && e.ID != m.ID {
				return nil, fmt.Errorf("%w: volume %q is a different volume than the one the sandbox mounted under that name", ErrVolumeMountConflict, m.Name)
			}
			if e.Path == m.Path && e.ID == m.ID {
				duplicate = true

				continue
			}
			if pathsOverlap(e.Path, m.Path) {
				return nil, fmt.Errorf("%w: %q and existing %q", ErrVolumeMountConflict, m.Path, e.Path)
			}
		}
		for _, other := range requested[:i] {
			if other.Name == m.Name && other.ID != m.ID {
				return nil, fmt.Errorf("%w: volume %q is listed as two different volumes", ErrVolumeMountConflict, m.Name)
			}
			if pathsOverlap(other.Path, m.Path) {
				return nil, fmt.Errorf("%w: %q and %q", ErrVolumeMountConflict, m.Path, other.Path)
			}
		}

		if !duplicate {
			added = append(added, m)
		}
	}

	return added, nil
}

// pathsOverlap reports whether two clean absolute paths are equal or one lies
// inside the other.
func pathsOverlap(a, b string) bool {
	if a == b {
		return true
	}

	return strings.HasPrefix(b, strings.TrimSuffix(a, "/")+"/") || strings.HasPrefix(a, strings.TrimSuffix(b, "/")+"/")
}

// callEnvdVolumesMount calls envd's POST /volumes/mount for the mounts.
// settled reports whether a failure is known to have left nothing of this call
// mounted: only on a 400, or on an envd without the endpoint.
func (s *Sandbox) callEnvdVolumesMount(ctx context.Context, mounts []VolumeMountConfig, unsettled []string) (settled bool, _ error) {
	ctx, cancel := context.WithTimeout(ctx, volumeAppendBaseTimeout+time.Duration(len(mounts))*volumeAppendPerVolume)
	defer cancel()

	body, err := json.Marshal(envd.PostVolumesMountJSONBody{
		LifecycleID:    s.LifecycleID,
		VolumeMounts:   s.convertMounts(mounts),
		UnsettledPaths: unsettled,
	})
	if err != nil {
		return true, fmt.Errorf("encode volumes mount request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.envdServerURL()+"/volumes/mount", bytes.NewReader(body))
	if err != nil {
		return true, fmt.Errorf("build volumes mount request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.Config.Envd.AccessToken != nil {
		req.Header.Set("X-Access-Token", *s.Config.Envd.AccessToken)
	}

	// The shared transport without sandboxHttpClient's 10s client timeout, which
	// a mount retried against a stalled NFS server outlasts; ctx bounds it.
	resp, err := (&http.Client{Transport: sandboxHttpClient.Transport}).Do(req)
	if err != nil {
		return false, fmt.Errorf("volumes mount request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return true, nil
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return true, ErrEnvdVolumeAppendUnsupported
	}

	// Only a 400 says nothing of this call is left: envd answers it after a
	// rollback that detached everything. Any other status, a 500 included,
	// leaves the outcome unknown.
	settled = resp.StatusCode == http.StatusBadRequest
	err = fmt.Errorf("volumes mount returned %d: %s", resp.StatusCode, envdErrorBody(resp.Body))
	if path := resp.Header.Get(envdFailedVolumeHeader); path != "" {
		return settled, fmt.Errorf("%w: %w", VolumeMountError{Path: utils.Truncate(path, maxVolumePathBytes)}, err)
	}

	return settled, err
}
