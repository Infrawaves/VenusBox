package volumequota_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/volumequota"
)

func TestQuotaRevisionsAndInheritanceAreIndependentOfProjectLimits(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store := volumequota.New(db.SqlcClient)
	ctx := t.Context()
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.volume_quota_defaults SET quota_gib = 12`))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.default_limits SET volume_quota_gib = 99`))
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO projection.project_limits (project_id, revision) VALUES ($1, 999)`, teamID))

	for _, step := range []struct {
		revision int64
		quota    *int64
		applied  bool
		want     int64
	}{
		{1, new(int64(20)), true, 20},
		{1, new(int64(30)), false, 20},
		{3, new(int64(0)), true, 0},
		{2, new(int64(40)), false, 0},
		{4, nil, true, 12},
	} {
		require.NoError(t, store.WithTeam(ctx, teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
			applied, err := team.SetQuota(ctx, step.revision, step.quota)
			assert.Equal(t, step.applied, applied)
			assert.Equal(t, step.want, team.QuotaGiB)

			return err
		}))
	}
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.volume_quota_defaults SET quota_gib = 15`))
	require.NoError(t, store.WithTeam(ctx, teamID, func(_ context.Context, team *volumequota.LockedTeam) error {
		assert.EqualValues(t, 15, team.QuotaGiB)
		assert.EqualValues(t, 4, team.Revision)

		return nil
	}))
	var count, projectRevision int64
	require.NoError(t, db.SqlcClient.Pool().QueryRow(ctx,
		`SELECT count(*) FROM public.project_limits WHERE team_id = $1`, teamID).Scan(&count))
	assert.Zero(t, count, "setting only a volume quota must not pin unrelated project defaults")
	require.NoError(t, db.SqlcClient.Pool().QueryRow(ctx,
		`SELECT revision FROM projection.project_limits WHERE project_id = $1`, teamID).Scan(&projectRevision))
	assert.EqualValues(t, 999, projectRevision)
}

func TestQuotaStoreValidatesBoundsAndTeam(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store := volumequota.New(db.SqlcClient)
	for _, quota := range []int64{-1, math.MaxInt32 + 1} {
		err := store.WithTeam(t.Context(), teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
			_, err := team.SetQuota(ctx, 1, &quota)

			return err
		})
		require.ErrorIs(t, err, volumequota.ErrInvalidQuota)
	}
	err := store.WithTeam(t.Context(), teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
		_, err := team.SetQuota(ctx, 0, nil)

		return err
	})
	require.ErrorIs(t, err, volumequota.ErrInvalidRevision)
	err = store.WithTeam(t.Context(), uuid.New(), func(context.Context, *volumequota.LockedTeam) error {
		t.Error("callback called for a nonexistent team")

		return nil
	})
	require.ErrorIs(t, err, volumequota.ErrTeamNotFound)
}

func TestQuotaBindingSurvivesSettingsAndRejectsAnotherFileset(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store := volumequota.New(db.SqlcClient)
	ctx := t.Context()
	for _, volumeType := range []string{"vepfs-b", "vepfs-a", "vepfs-a"} {
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			`INSERT INTO public.volumes (team_id, name, volume_type) VALUES ($1, $2, $3)`, teamID, uuid.NewString(), volumeType))
	}
	path := "/volumes/team-" + teamID.String() + "/"
	require.NoError(t, store.WithTeam(ctx, teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
		assert.Equal(t, []string{"vepfs-a", "vepfs-b"}, team.VolumeTypes)

		return team.BindFileset(ctx, "fs-one", path)
	}))
	require.NoError(t, store.WithTeam(ctx, teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
		assert.Equal(t, "fs-one", *team.FileSystemID)
		assert.Equal(t, path, *team.FilesetPath)
		_, err := team.SetQuota(ctx, 1, new(int64(0)))

		return err
	}))
	for _, pair := range [][2]string{{"fs-two", path}, {"fs-one", "/elsewhere/team/"}} {
		err := store.WithTeam(ctx, teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
			return team.BindFileset(ctx, pair[0], pair[1])
		})
		require.ErrorIs(t, err, volumequota.ErrConflictingFileSystem)
	}
	require.NoError(t, store.WithTeam(ctx, teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
		return team.BindFileset(ctx, "fs-one", path)
	}))
}

func TestQuotaLockWaitIsCancellableAndDoesNotBlockOtherTeams(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID, otherTeam := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	first, second := volumequota.New(db.SqlcClient), volumequota.New(db.SqlcClient)
	entered, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- first.WithTeam(t.Context(), teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
			close(entered)
			<-release
			_, err := team.SetQuota(ctx, 2, new(int64(10)))

			return err
		})
	}()
	<-entered
	// Always release the holder, even if an assertion below fails.
	defer func() {
		close(release)
		require.NoError(t, <-finished)
		require.NoError(t, second.WithTeam(t.Context(), teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
			assert.EqualValues(t, 10, team.QuotaGiB, "the next holder reads the committed setting")
			applied, err := team.SetQuota(ctx, 1, new(int64(100)))
			assert.False(t, applied, "a delayed old revision cannot undo the new setting")
			assert.EqualValues(t, 10, team.QuotaGiB)

			return err
		}))
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	err := second.WithTeam(ctx, teamID, func(context.Context, *volumequota.LockedTeam) error {
		t.Error("callback entered before the first transaction released the lock")

		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	otherCtx, otherCancel := context.WithTimeout(t.Context(), time.Second)
	defer otherCancel()
	require.NoError(t, second.WithTeam(otherCtx, otherTeam, func(context.Context, *volumequota.LockedTeam) error { return nil }))

	defaultCtx, defaultCancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer defaultCancel()
	err = db.SqlcClient.TestsRawSQL(defaultCtx, `UPDATE public.volume_quota_defaults SET quota_gib = 30`)
	require.ErrorIs(t, err, context.DeadlineExceeded, "default changes must not race an in-flight application")
}

func TestQuotaTransactionRollsBackWithoutRepeatingCallback(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store := volumequota.New(db.SqlcClient)
	wantErr := errors.New("external operation failed")
	calls := 0
	err := store.WithTeam(t.Context(), teamID, func(ctx context.Context, team *volumequota.LockedTeam) error {
		calls++
		if _, err := team.SetQuota(ctx, 1, new(int64(20))); err != nil {
			return err
		}

		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, calls)
	require.NoError(t, store.WithTeam(t.Context(), teamID, func(_ context.Context, team *volumequota.LockedTeam) error {
		assert.Zero(t, team.Revision)
		assert.Zero(t, team.QuotaGiB)

		return nil
	}))
}
