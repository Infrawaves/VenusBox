package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"
	"time"

	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/logs"
)

// maxVolumesMountBodyBytes bounds the /volumes/mount body; it carries only paths
// and NFS targets.
const maxVolumesMountBodyBytes = 1 << 20

// PostVolumesMount mounts volumes into a running sandbox, next to the ones /init
// mounted. It is all or nothing, and unlike /init it never records a failure for
// the lifecycle: a bad path the user asked for must not fail the next /init.
func (a *API) PostVolumesMount(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	logger := a.logger.With().Str(string(logs.OperationIDKey), logs.AssignOperationID()).Logger()

	var body PostVolumesMountJSONBody
	if err := json.NewDecoder(io.LimitReader(r.Body, maxVolumesMountBodyBytes)).Decode(&body); err != nil {
		// Not a 400: a body this envd cannot read may list unsettled paths, and a
		// 400 would tell the orchestrator they are not mounted.
		jsonError(w, http.StatusInternalServerError, fmt.Errorf("decode request: %w", err))

		return
	}
	// A refused request settles nothing, so with unsettled paths it is a 500: a
	// 400 would tell the orchestrator they are not mounted.
	refusedCode := http.StatusBadRequest
	if body.UnsettledPaths != nil && len(*body.UnsettledPaths) > 0 {
		refusedCode = http.StatusInternalServerError
	}
	if err := validateVolumeMounts(body.VolumeMounts); err != nil {
		jsonError(w, refusedCode, err)

		return
	}
	unsettled, err := unsettledPathSet(body.VolumeMounts, body.UnsettledPaths)
	if err != nil {
		jsonError(w, refusedCode, err)

		return
	}

	// The lock /init holds while it mounts: both read and write mountedPaths, and a
	// mount racing /init's unmount of a stale one would reuse a dead NFS client.
	if err := a.initLock.Acquire(r.Context(), 1); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)

		return
	}
	defer a.initLock.Release(1)

	if err := a.mountVolumes(r.Context(), logger, derefString(body.LifecycleID), body.VolumeMounts, unsettled); err != nil {
		if volumeErr, ok := errors.AsType[*nfsVolumeError](err); ok {
			w.Header().Set(failedVolumeHeader, volumeErr.path)
		}
		logger.Error().Err(err).Msg("Failed to mount volumes")

		// A 400 tells the orchestrator nothing this call mounted is left, so it
		// can drop the volumes. A 500 tells it some still are: it keeps them,
		// and with them the NFS proxy's service of the mounts.
		code := http.StatusBadRequest
		if errors.Is(err, errVolumesLeftMounted) {
			code = http.StatusInternalServerError
		}
		jsonError(w, code, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func validateVolumeMounts(volumes []VolumeMount) error {
	if len(volumes) == 0 {
		return errors.New("volumeMounts must not be empty")
	}

	seen := make(map[string]struct{}, len(volumes))
	for _, volume := range volumes {
		if volume.NfsTarget == "" {
			return fmt.Errorf("volume at %q has no nfs_target", volume.Path)
		}
		if !filepath.IsAbs(volume.Path) || filepath.Clean(volume.Path) != volume.Path || volume.Path == "/" {
			return fmt.Errorf("volume path %q must be a clean absolute path other than /", volume.Path)
		}
		if _, ok := seen[volume.Path]; ok {
			return fmt.Errorf("volume path %q is listed twice", volume.Path)
		}
		seen[volume.Path] = struct{}{}
	}

	return nil
}

// unsettledPathSet returns the unsettled paths as a set, each one the path of
// one of volumes.
func unsettledPathSet(volumes []VolumeMount, paths *[]string) (map[string]struct{}, error) {
	set := make(map[string]struct{})
	if paths == nil {
		return set, nil
	}

	for _, path := range *paths {
		if !slices.ContainsFunc(volumes, func(v VolumeMount) bool { return v.Path == path }) {
			return nil, fmt.Errorf("unsettled path %q is not the path of a listed volume", path)
		}
		set[path] = struct{}{}
	}

	return set, nil
}

// mountVolumes mounts each volume not yet mounted for the lifecycle, parents before
// children, and unmounts the ones it mounted when any fails or ctx ends. The
// outcome also covers each unsettled path: one the ledger has for the lifecycle
// is kept and rolled back with the rest on a failure, any other is detached and
// mounted afresh. It runs under initLock.
func (a *API) mountVolumes(ctx context.Context, logger zerolog.Logger, lifecycleID string, volumes []VolumeMount, unsettled map[string]struct{}) error {
	start := time.Now()

	// Unlike setupNFS, this works within the request: a caller that gave up has
	// dropped the mounts from its config, so they are rolled back below rather
	// than left in the ledger, where a later call would skip the path.

	// mounted is what a failure rolls back. An unsettled path the ledger has for
	// this lifecycle starts on it: the ledger records a mount only once its mount
	// ID is verified, and a rollback drops or blanks the entry, so the mount is in
	// place. Remounting it would abort its I/O; rolling it back with the rest keeps
	// a 400 meaning nothing listed as unsettled is left.
	var pending, mounted []VolumeMount
	for _, volume := range volumes {
		mountedLifecycle, isMounted := a.mountedPaths.Load(volume.Path)
		_, isUnsettled := unsettled[volume.Path]
		switch {
		case isUnsettled && lifecycleID != "" && isMounted && asString(mountedLifecycle) == lifecycleID:
			mounted = append(mounted, volume)
		case isUnsettled || shouldRemountNFS(isMounted, asString(mountedLifecycle), lifecycleID):
			pending = append(pending, volume)
		}
	}

	// Every other unsettled path may hold a mount the ledger never recorded. They
	// are settled before anything else can fail, as a 400 must mean they are not
	// mounted; one that cannot be detached is a 500.
	for _, volume := range pending {
		if _, isUnsettled := unsettled[volume.Path]; !isUnsettled {
			continue
		}
		symlink, err := a.isSymlink(volume.Path)
		if err != nil {
			return a.rollBack(logger, mounted, errors.Join(
				&nfsVolumeError{path: volume.Path, err: fmt.Errorf("tell whether the path is a symlink: %w", err)},
				errVolumesLeftMounted,
			))
		}
		if symlink {
			return a.rollBack(logger, mounted, errors.Join(
				&nfsVolumeError{path: volume.Path, err: errors.New("the path is a symlink, so a mount there cannot be detached safely")},
				errVolumesLeftMounted,
			))
		}
		leftover, err := a.unrecordedMountAt(volume.Path)
		if err != nil {
			return a.rollBack(logger, mounted, errors.Join(
				&nfsVolumeError{path: volume.Path, err: fmt.Errorf("tell what is mounted at the path: %w", err)},
				errVolumesLeftMounted,
			))
		}
		if !leftover {
			continue
		}
		if err := a.nfs.Unmount(volume.Path); err != nil {
			a.mountedPaths.Store(volume.Path, "")

			return a.rollBack(logger, mounted, errors.Join(
				&nfsVolumeError{path: volume.Path, err: fmt.Errorf("unmount: %w", err)},
				errVolumesLeftMounted,
			))
		}
		a.mountedPaths.Delete(volume.Path)
	}

	for _, volume := range pending {
		// Detaching a path whose last component is a symlink detaches whatever
		// it leads to, so a mount there could not be rolled back safely.
		symlink, err := a.isSymlink(volume.Path)
		if err != nil {
			return a.rollBack(logger, mounted, &nfsVolumeError{path: volume.Path, err: fmt.Errorf("tell whether the path is a symlink: %w", err)})
		}
		if symlink {
			return a.rollBack(logger, mounted, &nfsVolumeError{path: volume.Path, err: errors.New("the path is a symlink")})
		}
	}

	// A mount left from an earlier lifecycle holds a connection that is dead after a
	// resume, and a new mount would reuse it; see setupNFS.
	for _, volume := range pending {
		if _, isMounted := a.mountedPaths.Load(volume.Path); !isMounted {
			continue
		}
		// Keep it in the ledger until detached, so the next /init still finds it.
		if err := a.nfs.Unmount(volume.Path); err != nil {
			return a.rollBack(logger, mounted, errors.Join(
				&nfsVolumeError{path: volume.Path, err: fmt.Errorf("unmount: %w", err)},
				errVolumesLeftMounted,
			))
		}
		a.mountedPaths.Delete(volume.Path)
	}

	pending, unresolved, err := a.sortByResolvedDepth(pending)
	if err != nil {
		return a.rollBack(logger, mounted, &nfsVolumeError{path: unresolved.Path, err: err})
	}

	// The orchestrator compares paths as strings, so a new path that leads to a
	// mounted volume through a symlink gets this far. Mounting it would hide that
	// volume, and the next /init would fail on it.
	existing, err := a.liveMountIDs(lifecycleID)
	if err != nil {
		return a.rollBack(logger, mounted, err)
	}

	mountIDs := make(map[string]uint64, len(pending))
	for _, volume := range pending {
		id, err := a.mountNFS(ctx, logger, volume, lifecycleID)
		if err != nil {
			if unsettled, ok := errors.AsType[*unsettledMountError](err); ok && a.mayBeMounted(volume.Path, unsettled.before) {
				// Possibly in place, so roll it back with the others.
				mounted = append(mounted, volume)
			}

			return a.rollBack(logger, mounted, &nfsVolumeError{path: volume.Path, err: err})
		}
		mounted = append(mounted, volume)
		mountIDs[volume.Path] = id

		if hidden := a.hiddenMount(existing); hidden != "" {
			return a.rollBack(logger, mounted, &nfsVolumeError{path: volume.Path, err: fmt.Errorf("would hide the volume mounted at %q", hidden)})
		}
	}

	// A later mount can still cover an earlier one, so check each path once more.
	for _, volume := range pending {
		if id, err := a.nfs.MountID(volume.Path); err != nil || id != mountIDs[volume.Path] {
			return a.rollBack(logger, mounted, &nfsVolumeError{path: volume.Path, err: errors.New("hidden by another volume mounted over it")})
		}
	}

	if err := context.Cause(ctx); err != nil {
		return a.rollBack(logger, mounted, fmt.Errorf("the caller went away: %w", err))
	}

	logger.Info().
		Int("volumes", len(pending)).
		Int("already_mounted", len(volumes)-len(pending)).
		Dur("duration", time.Since(start)).
		Msg("Appended NFS volumes")

	return nil
}

// unrecordedMountAt reports whether a mount the ledger does not know of sits at
// path, the one an earlier call may have left. Detaching the path detaches the
// mount it resolves to, which through a symlinked parent can be a recorded
// volume's mount (/alias/data with /alias -> / is /data), so that is not one:
// nor is a path that is no mount point at all.
func (a *API) unrecordedMountAt(path string) (bool, error) {
	resolved, err := a.resolveExistingBounded(path)
	if err != nil {
		return false, err
	}
	if _, err := a.lstatBounded(resolved); errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if errors.Is(err, errPathProbeTimeout) {
		return false, err
	}

	id, err := a.nfs.MountID(resolved)
	if err != nil {
		return false, err
	}
	parentID, err := a.nfs.MountID(filepath.Dir(resolved))
	if err != nil {
		return false, err
	}
	if id == parentID {
		return false, nil
	}

	owned := false
	var probeErr error
	a.mountedPaths.Range(func(k, _ any) bool {
		other, _ := k.(string)
		if other == path {
			return true
		}
		otherID, err := a.nfs.MountID(other)
		if err != nil {
			probeErr = fmt.Errorf("read the mount ID of tracked volume %q: %w", other, err)

			return false
		}
		if otherID == id {
			owned = true

			return false
		}

		return true
	})
	if probeErr != nil {
		return false, probeErr
	}

	return !owned, nil
}

// mayBeMounted reports whether a mount attempt may have left a mount at path:
// whenever the path no longer resolves to the mount it did before.
func (a *API) mayBeMounted(path string, before uint64) bool {
	id, err := a.nfs.MountID(path)

	return err != nil || id != before
}

// liveMountIDs returns the mount ID of each path mounted for the lifecycle.
func (a *API) liveMountIDs(lifecycleID string) (map[string]uint64, error) {
	ids := make(map[string]uint64)

	var err error
	a.mountedPaths.Range(func(k, v any) bool {
		path, _ := k.(string)
		if asString(v) != lifecycleID {
			return true
		}

		var id uint64
		if id, err = a.nfs.MountID(path); err != nil {
			err = &nfsVolumeError{path: path, err: fmt.Errorf("read the mount ID of a mounted volume: %w", err)}

			return false
		}
		ids[path] = id

		return true
	})

	return ids, err
}

// hiddenMount returns a path whose mount is no longer the one it resolved to
// before, or "" when every one is still in place.
func (a *API) hiddenMount(before map[string]uint64) string {
	for path, id := range before {
		if now, err := a.nfs.MountID(path); err != nil || now != id {
			return path
		}
	}

	return ""
}

// errVolumesLeftMounted reports a rollback that could not detach every mount
// it made.
var errVolumesLeftMounted = errors.New("some volumes this call mounted are still mounted")

// rollBack detaches the mounts this call made, children first, and returns
// cause. A mount that cannot be detached stays in the ledger with an empty
// lifecycle ID. That makes every later /init or append treat it as stale,
// including after envd hands the ledger over during a live upgrade.
func (a *API) rollBack(logger zerolog.Logger, mounted []VolumeMount, cause error) error {
	var left []error
	for _, volume := range slices.Backward(mounted) {
		a.mountedPaths.Delete(volume.Path)
		if err := a.nfs.Unmount(volume.Path); err != nil {
			logger.Warn().Err(err).Str("path", volume.Path).Msg("Failed to roll back an appended NFS volume")
			a.mountedPaths.Store(volume.Path, "")
			left = append(left, fmt.Errorf("%q: %w", volume.Path, err))
		}
	}

	if len(left) > 0 {
		return errors.Join(cause, fmt.Errorf("%w: %w", errVolumesLeftMounted, errors.Join(left...)))
	}

	return cause
}
