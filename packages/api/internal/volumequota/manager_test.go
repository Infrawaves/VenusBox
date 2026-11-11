package volumequota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/volcengine/volcengine-go-sdk/service/vepfs"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/request"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	quotadb "github.com/e2b-dev/infra/packages/db/pkg/volumequota"
)

func databaseManager(db *testutils.Database, api filesetAPI) *Manager {
	m := testManager(api)
	m.store = quotadb.New(db.SqlcClient)
	m.paths = map[string]string{testVolumeType: "/volumes"}

	return m
}

func setDatabaseQuota(t *testing.T, store *quotadb.Store, teamID uuid.UUID, revision, quota int64) {
	t.Helper()
	require.NoError(t, store.WithTeam(t.Context(), teamID, func(ctx context.Context, team *quotadb.LockedTeam) error {
		_, err := team.SetQuota(ctx, revision, &quota)

		return err
	}))
}

type blockingQuotaAPI struct {
	*fakeVePFS

	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (f *blockingQuotaAPI) SetFilesetQuotaWithContext(ctx volcengine.Context, in *vepfs.SetFilesetQuotaInput, opts ...request.Option) (*vepfs.SetFilesetQuotaOutput, error) {
	f.once.Do(func() { close(f.entered) })
	select {
	case <-f.release:
		return f.fakeVePFS.SetFilesetQuotaWithContext(ctx, in, opts...)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestManagerSerializesReplicasAndQuotaChangesDuringCloudWrite(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	fake := newFake()
	release := make(chan struct{})
	slow := &blockingQuotaAPI{fakeVePFS: fake, entered: make(chan struct{}), release: release}
	first, second := databaseManager(db, slow), databaseManager(db, fake)
	setDatabaseQuota(t, first.store, teamID, 1, 10)
	finished := make(chan error, 1)
	go func() { finished <- first.EnsureTeamQuota(t.Context(), testVolumeType, teamID) }()
	<-slow.entered
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	err := second.EnsureTeamQuota(waitCtx, testVolumeType, teamID)
	waitCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	updateCtx, updateCancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	err = second.store.WithTeam(updateCtx, teamID, func(ctx context.Context, team *quotadb.LockedTeam) error {
		_, err := team.SetQuota(ctx, 2, new(int64(20)))

		return err
	})
	updateCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "settings cannot change while an older cloud write is in flight")
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-finished)
	setDatabaseQuota(t, second.store, teamID, 2, 20)
	require.NoError(t, second.EnsureTeamQuota(t.Context(), testVolumeType, teamID))
	_, path := first.Fileset(testVolumeType, teamID)
	assert.Equal(t, int32(20), fake.limitAt(path))
	assert.Equal(t, []int32{10, 20}, fake.quotaSets)
	assert.Equal(t, 1, fake.creates)
}

func TestManagerWaitingRequestReadsNewCommittedQuota(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	fake := newFake()
	m := databaseManager(db, fake)
	setDatabaseQuota(t, m.store, teamID, 1, 100)
	// A settings transaction holds the quota row while a request that began
	// under the old setting waits. The request must read 10 after the commit.
	entered, release := make(chan struct{}), make(chan struct{})
	updateFinished := make(chan error, 1)
	go func() {
		updateFinished <- m.store.WithTeam(t.Context(), teamID, func(ctx context.Context, team *quotadb.LockedTeam) error {
			if _, err := team.SetQuota(ctx, 2, new(int64(10))); err != nil {
				return err
			}
			close(entered)
			<-release

			return nil
		})
	}()
	<-entered
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	requestFinished := make(chan error, 1)
	go func() { requestFinished <- m.EnsureTeamQuota(t.Context(), testVolumeType, teamID) }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := db.SqlcClient.Pool().QueryRow(t.Context(), `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
			AND wait_event_type = 'Lock' AND query LIKE '%team_volume_quotas%')`).Scan(&waiting)

		return err == nil && waiting
	}, time.Second, 10*time.Millisecond)
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-updateFinished)
	require.NoError(t, <-requestFinished)
	_, path := m.Fileset(testVolumeType, teamID)
	assert.Equal(t, int32(10), fake.limitAt(path))
	assert.Equal(t, []int32{10}, fake.quotaSets, "no old authentication snapshot can be written back")
}

func TestManagerBindingSurvivesCloudFailureAndUnboundedQuota(t *testing.T) {
	t.Parallel()

	for _, quota := range []int64{0, 10} {
		t.Run(map[int64]string{0: "unbounded", 10: "cloud failure"}[quota], func(t *testing.T) {
			t.Parallel()
			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			fake := newFake()
			fake.quotaErr = errors.New("cloud unavailable")
			first := databaseManager(db, fake)
			setDatabaseQuota(t, first.store, teamID, 1, quota)
			err := first.EnsureTeamQuota(t.Context(), testVolumeType, teamID)
			if quota == 0 {
				require.NoError(t, err)
				assert.Zero(t, fake.creates)
			} else {
				require.ErrorContains(t, err, "cloud unavailable")
			}
			require.NoError(t, first.store.WithTeam(t.Context(), teamID, func(_ context.Context, team *quotadb.LockedTeam) error {
				assert.Equal(t, testFileSystemID, *team.FileSystemID)
				assert.Equal(t, "/volumes/team-"+teamID.String()+"/", *team.FilesetPath)

				return nil
			}))
			other := databaseManager(db, fake)
			other.fileSystems = map[string]string{testVolumeType: "another-filesystem"}
			err = other.EnsureTeamQuota(t.Context(), testVolumeType, teamID)
			require.ErrorIs(t, err, quotadb.ErrConflictingFileSystem)
			fake.quotaErr = nil
			require.NoError(t, first.EnsureTeamQuota(t.Context(), testVolumeType, teamID))
		})
	}
}

func TestManagerRejectsAnotherFilesetButAllowsAliases(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	fake := newFake()
	m := databaseManager(db, fake)
	m.fileSystems["different-fs"] = "another-filesystem"
	m.fileSystems["different-path"] = testFileSystemID
	m.fileSystems["same-fileset"] = testFileSystemID
	m.paths["different-path"] = "/other"
	m.paths["same-fileset"] = "/volumes/"
	setDatabaseQuota(t, m.store, teamID, 1, 10)
	require.NoError(t, m.EnsureTeamQuota(t.Context(), testVolumeType, teamID))
	for _, volumeType := range []string{"different-fs", "different-path"} {
		require.ErrorIs(t, m.EnsureTeamQuota(t.Context(), volumeType, teamID), quotadb.ErrConflictingFileSystem)
	}
	require.NoError(t, m.EnsureTeamQuota(t.Context(), "same-fileset", teamID))
	assert.Equal(t, 1, fake.creates)
	assert.Equal(t, []int32{10}, fake.quotaSets)
}

func TestManagerRejectsConflictingHistoricalManagedVolumes(t *testing.T) {
	t.Parallel()

	for _, sameFS := range []bool{false, true} {
		t.Run(map[bool]string{false: "different filesystem", true: "different path"}[sameFS], func(t *testing.T) {
			t.Parallel()
			db := testutils.SetupDatabase(t)
			teamID := testutils.CreateTestTeam(t, db)
			fake := newFake()
			m := databaseManager(db, fake)
			m.fileSystems["historical"] = "another-filesystem"
			if sameFS {
				m.fileSystems["historical"] = testFileSystemID
				m.paths["historical"] = "/old"
			}
			require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
				`INSERT INTO public.volumes (team_id, name, volume_type) VALUES ($1, 'existing', 'historical')`, teamID))
			setDatabaseQuota(t, m.store, teamID, 1, 10)
			require.ErrorIs(t, m.EnsureTeamQuota(t.Context(), testVolumeType, teamID), quotadb.ErrConflictingFileSystem)
			assert.Zero(t, fake.creates)
			assert.Empty(t, fake.quotaSets)
			require.NoError(t, m.store.WithTeam(t.Context(), teamID, func(_ context.Context, team *quotadb.LockedTeam) error {
				assert.Nil(t, team.FileSystemID)

				return nil
			}))
		})
	}
}

func TestManagerUsesIndependentDefaultAndAllowsLocalVolumes(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	fake := newFake()
	m := databaseManager(db, fake)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`INSERT INTO public.volumes (team_id, name, volume_type) VALUES ($1, 'local', 'local')`, teamID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.volume_quota_defaults SET quota_gib = 13`))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.default_limits SET volume_quota_gib = 99`))
	require.NoError(t, m.EnsureTeamQuota(t.Context(), testVolumeType, teamID))
	require.NoError(t, m.EnsureTeamQuota(t.Context(), "local", teamID))
	_, path := m.Fileset(testVolumeType, teamID)
	assert.Equal(t, int32(13), fake.limitAt(path))
	assert.Equal(t, 1, fake.creates)
	assert.Equal(t, []int32{13}, fake.quotaSets)
}
