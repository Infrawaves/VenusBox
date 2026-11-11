// Package volumequota bounds the combined size of a team's persistent volumes.
//
// A team's volumes live under <volume-root>/team-<id>. A fileset at the
// corresponding path inside vePFS bounds their shared capacity. Database row
// locks serialize quota applications and pin the team to that one fileset.
package volumequota

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/volcengine/volcengine-go-sdk/service/vepfs"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/request"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	dbclient "github.com/e2b-dev/infra/packages/db/client"
	quotadb "github.com/e2b-dev/infra/packages/db/pkg/volumequota"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// ErrQuotaTooLarge is returned for a limit vePFS cannot represent.
var ErrQuotaTooLarge = errors.New("volume quota exceeds what vePFS accepts")

const (
	// How long a fileset may stay in a transitional state before the call gives
	// up. Volume creation is a user-facing request, so this stays short; a later
	// create of the same team picks up where this one stopped.
	settleTimeout  = 30 * time.Second
	settleInterval = time.Second
)

// filesetAPI is the part of the vePFS client the manager uses.
type filesetAPI interface {
	DescribeFilesetsWithContext(volcengine.Context, *vepfs.DescribeFilesetsInput, ...request.Option) (*vepfs.DescribeFilesetsOutput, error)
	CreateFilesetWithContext(volcengine.Context, *vepfs.CreateFilesetInput, ...request.Option) (*vepfs.CreateFilesetOutput, error)
	SetFilesetQuotaWithContext(volcengine.Context, *vepfs.SetFilesetQuotaInput, ...request.Option) (*vepfs.SetFilesetQuotaOutput, error)
}

// Manager applies team quotas on the vePFS file systems behind volume types.
// A nil *Manager manages nothing, so callers need not check for one.
type Manager struct {
	api         filesetAPI
	fileSystems map[string]string
	paths       map[string]string
	store       *quotadb.Store

	settleTimeout  time.Duration
	settleInterval time.Duration
}

// New returns nil when no volume type is mapped to a vePFS file system.
func New(config cfg.VolumeQuotaConfig, db *dbclient.Client) (*Manager, error) {
	if !config.IsConfigured() {
		return nil, nil
	}
	if db == nil {
		return nil, errors.New("volume quotas require a primary database client")
	}

	volcConfig := volcengine.NewConfig().
		WithRegion(config.Region).
		WithCredentials(credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""))
	if config.Endpoint != "" {
		volcConfig = volcConfig.WithEndpoint(config.Endpoint)
	}

	sess, err := session.NewSession(volcConfig)
	if err != nil {
		return nil, fmt.Errorf("creating volcengine session: %w", err)
	}

	m := newManager(vepfs.New(sess), config.FileSystems)
	m.paths = config.Paths
	m.store = quotadb.New(db)

	return m, nil
}

func newManager(api filesetAPI, fileSystems map[string]string) *Manager {
	return &Manager{
		api:            api,
		fileSystems:    fileSystems,
		settleTimeout:  settleTimeout,
		settleInterval: settleInterval,
	}
}

// Manages reports whether volumes of volumeType are bounded by this manager.
func (m *Manager) Manages(volumeType string) bool {
	if m == nil {
		return false
	}

	_, ok := m.fileSystems[volumeType]

	return ok
}

// TeamPath is the team's path beneath its volume root. It mirrors
// chrooted.Builder.BuildVolumePath in the orchestrator. vePFS
// requires a fileset path to start and end with "/" and refuses one without the
// trailing slash as InvalidParameter.FilesetPath.
func TeamPath(teamID uuid.UUID) string {
	return fmt.Sprintf("/team-%s/", teamID)
}

// Fileset identifies the storage directory covered by a managed volume type.
// The node verifies both its filesystem identity and this path before creating
// the volume, including when the current quota is zero.
func (m *Manager) Fileset(volumeType string, teamID uuid.UUID) (fileSystemID, filesetPath string) {
	if !m.Manages(volumeType) {
		return "", ""
	}

	return m.fileSystems[volumeType], path.Join("/", m.paths[volumeType], TeamPath(teamID)) + "/"
}

// EnsureTeamQuota reads the current quota from the primary database while
// holding the team's row lock through the cloud operation. Requests never carry
// an authentication-cache snapshot of a quota into this write.
func (m *Manager) EnsureTeamQuota(ctx context.Context, volumeType string, teamID uuid.UUID) error {
	if !m.Manages(volumeType) {
		return nil
	}
	if m.store == nil {
		return errors.New("volume quota store is not configured")
	}

	fileSystemID, filesetPath := m.Fileset(volumeType, teamID)
	// Persist the binding before cloud work. Even an unsuccessful create must
	// not let another replica allocate a second full quota on another fileset.
	if err := m.store.WithTeam(ctx, teamID, func(ctx context.Context, team *quotadb.LockedTeam) error {
		for _, existingType := range team.VolumeTypes {
			if !m.Manages(existingType) {
				continue
			}
			existingFS, existingPath := m.Fileset(existingType, teamID)
			if existingFS != fileSystemID || existingPath != filesetPath {
				return fmt.Errorf("%w: existing volume type %q does not share fileset %s on %s", quotadb.ErrConflictingFileSystem, existingType, filesetPath, fileSystemID)
			}
		}

		return team.BindFileset(ctx, fileSystemID, filesetPath)
	}); err != nil {
		return err
	}

	return m.store.WithTeam(ctx, teamID, func(ctx context.Context, team *quotadb.LockedTeam) error {
		if err := team.BindFileset(ctx, fileSystemID, filesetPath); err != nil {
			return err
		}

		return m.ensureFileset(ctx, volumeType, teamID, team.QuotaGiB)
	})
}

// samePath compares fileset paths ignoring a trailing slash, since vePFS may
// report a fileset created in the console without one.
func samePath(a, b string) bool {
	return strings.TrimSuffix(a, "/") == strings.TrimSuffix(b, "/")
}

// ensureFileset makes the team's fileset on the file system behind
// volumeType exist with a capacity quota of quotaGiB. It runs before the
// orchestrator creates a volume so the volume is bounded from its first write.
// A team directory that already exists, with data, can still become a fileset;
// its data then counts against the quota.
//
// A quotaGiB of 0 means unbounded: nothing is created, and an existing fileset
// is left as it is, since vePFS has no way to clear a quota through this API.
// An unmanaged volume type is a no-op.
func (m *Manager) ensureFileset(ctx context.Context, volumeType string, teamID uuid.UUID, quotaGiB int64) error {
	if !m.Manages(volumeType) || quotaGiB <= 0 {
		return nil
	}

	if quotaGiB > math.MaxInt32 {
		return fmt.Errorf("%w: %d GiB", ErrQuotaTooLarge, quotaGiB)
	}
	limit := int32(quotaGiB)

	fileSystemID, path := m.Fileset(volumeType, teamID)

	fileset, err := m.findFileset(ctx, fileSystemID, path)
	if err != nil {
		return err
	}

	if fileset == nil {
		filesetID, err := m.createFileset(ctx, fileSystemID, teamID, path)
		if err != nil {
			return err
		}

		logger.L().Info(ctx, "created vePFS fileset for team volumes",
			logger.WithTeamID(teamID.String()),
			zap.String("file_system_id", fileSystemID),
			zap.String("fileset_id", filesetID),
			zap.String("path", path))

		fileset = &vepfs.FilesetForDescribeFilesetsOutput{FilesetId: volcengine.String(filesetID)}
	}

	fileset, err = m.awaitRunning(ctx, fileSystemID, path, fileset)
	if err != nil {
		return err
	}

	if volcengine.Int32Value(fileset.CapacityLimit) == limit {
		return nil
	}

	_, err = m.api.SetFilesetQuotaWithContext(ctx, &vepfs.SetFilesetQuotaInput{
		FileSystemId:  volcengine.String(fileSystemID),
		FilesetId:     fileset.FilesetId,
		CapacityLimit: volcengine.Int32(limit),
	})
	if err != nil {
		return fmt.Errorf("setting quota of vePFS fileset %s on %s: %w", volcengine.StringValue(fileset.FilesetId), fileSystemID, err)
	}

	logger.L().Info(ctx, "set vePFS fileset quota for team volumes",
		logger.WithTeamID(teamID.String()),
		zap.String("file_system_id", fileSystemID),
		zap.String("fileset_id", volcengine.StringValue(fileset.FilesetId)),
		zap.Int32("previous_capacity_limit_gib", volcengine.Int32Value(fileset.CapacityLimit)),
		zap.Int32("capacity_limit_gib", limit))

	return nil
}

func (m *Manager) createFileset(ctx context.Context, fileSystemID string, teamID uuid.UUID, path string) (string, error) {
	name := fmt.Sprintf("team-%s", teamID)
	if !samePath(path, TeamPath(teamID)) {
		// A deployment previously applying quotas at the filesystem root may
		// already have a fileset with the legacy name at the wrong path.
		digest := sha256.Sum256([]byte(path))
		name += fmt.Sprintf("-%x", digest[:4])
	}
	out, err := m.api.CreateFilesetWithContext(ctx, &vepfs.CreateFilesetInput{
		FileSystemId: volcengine.String(fileSystemID),
		FilesetName:  new(name),
		FilesetPath:  volcengine.String(path),
	})
	if err != nil {
		// Another API replica may have created it between the lookup and here;
		// the lock above only covers this process.
		if existing, findErr := m.findFileset(ctx, fileSystemID, path); findErr == nil && existing != nil {
			return volcengine.StringValue(existing.FilesetId), nil
		}

		return "", fmt.Errorf("creating vePFS fileset %s on %s: %w", path, fileSystemID, err)
	}

	if out == nil || volcengine.StringValue(out.FilesetId) == "" {
		return "", fmt.Errorf("creating vePFS fileset %s on %s: no fileset ID returned", path, fileSystemID)
	}

	return volcengine.StringValue(out.FilesetId), nil
}

// findFileset returns the live fileset at path, or nil when there is none. It
// looks the path up with and without its trailing slash, since a fileset made
// in the console may be stored either way.
func (m *Manager) findFileset(ctx context.Context, fileSystemID, path string) (*vepfs.FilesetForDescribeFilesetsOutput, error) {
	for _, spelling := range []string{path, strings.TrimSuffix(path, "/")} {
		fileset, err := m.findFilesetSpelled(ctx, fileSystemID, path, spelling)
		if err != nil || fileset != nil {
			return fileset, err
		}
	}

	return nil, nil
}

func (m *Manager) findFilesetSpelled(ctx context.Context, fileSystemID, path, spelling string) (*vepfs.FilesetForDescribeFilesetsOutput, error) {
	out, err := m.api.DescribeFilesetsWithContext(ctx, &vepfs.DescribeFilesetsInput{
		FileSystemId: volcengine.String(fileSystemID),
		Filters: []*vepfs.FilterForDescribeFilesetsInput{{
			Key:   volcengine.String(vepfs.EnumOfKeyForDescribeFilesetsInputFilesetPath),
			Value: volcengine.String(spelling),
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("describing vePFS filesets of %s at %s: %w", fileSystemID, spelling, err)
	}
	if out == nil {
		return nil, nil
	}

	for _, fileset := range out.Filesets {
		if fileset == nil || !samePath(volcengine.StringValue(fileset.FilesetPath), path) {
			// The filter may match by prefix; only the team's own path counts.
			continue
		}

		switch volcengine.StringValue(fileset.Status) {
		case vepfs.EnumOfStatusForDescribeFilesetsOutputDeleting,
			vepfs.EnumOfStatusForDescribeFilesetsOutputDeleted:
			continue
		}

		return fileset, nil
	}

	return nil, nil
}

// awaitRunning waits until the fileset can take a quota change and returns its
// current state.
func (m *Manager) awaitRunning(
	ctx context.Context,
	fileSystemID, path string,
	fileset *vepfs.FilesetForDescribeFilesetsOutput,
) (*vepfs.FilesetForDescribeFilesetsOutput, error) {
	deadline := time.Now().Add(m.settleTimeout)

	for {
		switch status := volcengine.StringValue(fileset.Status); status {
		case vepfs.EnumOfStatusForDescribeFilesetsOutputRunning:
			return fileset, nil
		case vepfs.EnumOfStatusForDescribeFilesetsOutputCreateError,
			vepfs.EnumOfStatusForDescribeFilesetsOutputUpdateError,
			vepfs.EnumOfStatusForDescribeFilesetsOutputDeleteError,
			vepfs.EnumOfStatusForDescribeFilesetsOutputError:
			return nil, fmt.Errorf("vePFS fileset %s on %s is in state %s", volcengine.StringValue(fileset.FilesetId), fileSystemID, status)
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("vePFS fileset %s on %s did not become Running within %s (state %q)",
				volcengine.StringValue(fileset.FilesetId), fileSystemID, m.settleTimeout, volcengine.StringValue(fileset.Status))
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.settleInterval):
		}

		current, err := m.findFileset(ctx, fileSystemID, path)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, fmt.Errorf("vePFS fileset at %s on %s disappeared while waiting for it", path, fileSystemID)
		}

		fileset = current
	}
}
