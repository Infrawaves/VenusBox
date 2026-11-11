// Package volumequota owns persistent-volume quota settings independently of
// the project limits projection and the authentication cache.
package volumequota

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
)

var (
	ErrTeamNotFound          = errors.New("team not found")
	ErrInvalidQuota          = errors.New("volume quota must be between 0 and 2147483647 GiB")
	ErrInvalidRevision       = errors.New("volume quota revision must be positive")
	ErrInvalidFileset        = errors.New("invalid volume quota fileset")
	ErrConflictingFileSystem = errors.New("team volumes are already bound to a different fileset")
)

type Store struct {
	db *client.Client
}

func New(db *client.Client) *Store {
	return &Store{db: db}
}

// LockedTeam is valid only inside WithTeam's callback. QuotaGiB is resolved
// from the team's override or the locked deployment default, never from auth.
type LockedTeam struct {
	QuotaGiB     int64
	Revision     int64
	FileSystemID *string
	FilesetPath  *string
	VolumeTypes  []string

	teamID       uuid.UUID
	defaultQuota int64
	queries      *queries.Queries
}

// WithTeam serializes quota updates and applications across API replicas.
// Waiting for either row lock obeys ctx. The callback runs once: retrying a
// transaction could repeat external side effects whose outcome is unknown.
//
// A new fileset binding must be committed in its own WithTeam before doing
// cloud work. A subsequent call reads that binding and the latest quota under
// the same lock; cancellation of cloud work cannot then erase the binding.
func (s *Store) WithTeam(ctx context.Context, teamID uuid.UUID, fn func(context.Context, *LockedTeam) error) error {
	txDB, tx, err := s.db.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("begin volume quota transaction: %w", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	defaultQuota, err := txDB.LockVolumeQuotaDefault(ctx)
	if err != nil {
		return fmt.Errorf("lock volume quota default: %w", err)
	}
	if err := txDB.EnsureTeamVolumeQuota(ctx, teamID); err != nil {
		return fmt.Errorf("ensure team volume quota: %w", err)
	}
	row, err := txDB.LockTeamVolumeQuota(ctx, teamID)
	if dberrors.IsNotFoundError(err) {
		return ErrTeamNotFound
	}
	if err != nil {
		return fmt.Errorf("lock team volume quota: %w", err)
	}
	volumeTypes, err := txDB.ListTeamVolumeQuotaTypes(ctx, teamID)
	if err != nil {
		return fmt.Errorf("list team volume types: %w", err)
	}

	locked := &LockedTeam{
		QuotaGiB:     defaultQuota,
		Revision:     row.Revision,
		FileSystemID: row.FileSystemID,
		FilesetPath:  row.FilesetPath,
		VolumeTypes:  volumeTypes,
		teamID:       teamID,
		defaultQuota: defaultQuota,
		queries:      txDB.Queries,
	}
	if row.QuotaGib != nil {
		locked.QuotaGiB = *row.QuotaGib
	}
	if err := fn(ctx, locked); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit volume quota transaction: %w", err)
	}

	return nil
}

// BindFileset pins the team's shared capacity to one filesystem and path.
// It never moves a binding, even while the quota is unbounded.
func (t *LockedTeam) BindFileset(ctx context.Context, fileSystemID, filesetPath string) error {
	if strings.TrimSpace(fileSystemID) == "" || !strings.HasPrefix(filesetPath, "/") ||
		!strings.HasSuffix(filesetPath, "/") || filesetPath == "/" {
		return ErrInvalidFileset
	}
	if t.FileSystemID != nil {
		if *t.FileSystemID != fileSystemID || t.FilesetPath == nil || *t.FilesetPath != filesetPath {
			return ErrConflictingFileSystem
		}

		return nil
	}
	if err := t.queries.BindTeamVolumeQuotaFileset(ctx, queries.BindTeamVolumeQuotaFilesetParams{
		TeamID:       t.teamID,
		FileSystemID: fileSystemID,
		FilesetPath:  filesetPath,
	}); err != nil {
		return fmt.Errorf("bind team volume quota fileset: %w", err)
	}
	t.FileSystemID = &fileSystemID
	t.FilesetPath = &filesetPath

	return nil
}

// SetQuota accepts only a newer revision in the independent volume-quota
// stream. A nil quota restores inheritance and still advances its revision.
// Its boolean reports whether this transaction changed the setting.
func (t *LockedTeam) SetQuota(ctx context.Context, revision int64, quotaGiB *int64) (bool, error) {
	if revision <= 0 {
		return false, ErrInvalidRevision
	}
	if quotaGiB != nil && (*quotaGiB < 0 || *quotaGiB > math.MaxInt32) {
		return false, ErrInvalidQuota
	}
	if revision <= t.Revision {
		return false, nil
	}
	if err := t.queries.SetTeamVolumeQuota(ctx, queries.SetTeamVolumeQuotaParams{
		TeamID:   t.teamID,
		Revision: revision,
		QuotaGib: quotaGiB,
	}); err != nil {
		return false, fmt.Errorf("set team volume quota: %w", err)
	}
	t.Revision = revision
	t.QuotaGiB = t.defaultQuota
	if quotaGiB != nil {
		t.QuotaGiB = *quotaGiB
	}

	return true, nil
}
