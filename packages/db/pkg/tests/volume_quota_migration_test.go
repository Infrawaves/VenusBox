package tests

import (
	"context"
	"database/sql"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/volumequota"
)

const isolatedVolumeQuotaMigration = 20261111031408

func TestVolumeQuotaMigrationPreservesSettingsAndDowngrades(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	provider := migrationProvider(t, sqlDB)
	_, err = provider.DownTo(ctx, isolatedVolumeQuotaMigration-1)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.default_limits SET volume_quota_gib = 11`)
	require.NoError(t, err)
	inherited := testutils.CreateTestTeam(t, db)
	explicitZero := testutils.CreateTestTeam(t, db)
	explicitQuota := testutils.CreateTestTeam(t, db)
	for _, item := range []struct {
		id    uuid.UUID
		quota int64
	}{{explicitZero, 0}, {explicitQuota, 37}} {
		_, err = sqlDB.ExecContext(ctx, `
			INSERT INTO public.project_limits (
				team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
				max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb,
				max_disk_size_mb, max_free_disk_size_mb, api_team_rps_list, api_team_rps_delete,
				max_secrets, volume_quota_gib
			)
			SELECT $1, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
			       max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb,
			       max_disk_size_mb, max_disk_size_mb, api_team_rps_list, api_team_rps_delete,
			       max_secrets, $2
			FROM public.default_limits`, item.id, item.quota)
		require.NoError(t, err)
	}
	_, err = sqlDB.ExecContext(ctx, `INSERT INTO projection.project_limits (project_id, revision) VALUES ($1, 200)`, explicitQuota)
	require.NoError(t, err)
	_, err = provider.Up(ctx)
	require.NoError(t, err)
	store := volumequota.New(db.SqlcClient)
	for _, item := range []struct {
		id   uuid.UUID
		want int64
	}{{inherited, 11}, {explicitZero, 0}, {explicitQuota, 37}} {
		require.NoError(t, store.WithTeam(ctx, item.id, func(_ context.Context, team *volumequota.LockedTeam) error {
			assert.Equal(t, item.want, team.QuotaGiB)
			assert.Zero(t, team.Revision, "the quota revision stream starts independently")

			return nil
		}))
	}
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.volume_quota_defaults SET quota_gib = 21`)
	require.NoError(t, err)
	require.NoError(t, store.WithTeam(ctx, explicitQuota, func(ctx context.Context, team *volumequota.LockedTeam) error {
		_, err := team.SetQuota(ctx, 1, nil)

		return err
	}))
	newOverride := testutils.CreateTestTeam(t, db)
	require.NoError(t, store.WithTeam(ctx, newOverride, func(ctx context.Context, team *volumequota.LockedTeam) error {
		_, err := team.SetQuota(ctx, 1, new(int64(29)))

		return err
	}))
	_, err = provider.DownTo(ctx, isolatedVolumeQuotaMigration-1)
	require.NoError(t, err)
	var defaultQuota int64
	require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT volume_quota_gib FROM public.default_limits`).Scan(&defaultQuota))
	assert.EqualValues(t, 21, defaultQuota)
	for _, item := range []struct {
		id   uuid.UUID
		want *int64
	}{{explicitZero, new(int64(0))}, {explicitQuota, nil}, {newOverride, new(int64(29))}} {
		var quota *int64
		require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT volume_quota_gib FROM public.project_limits WHERE team_id = $1`, item.id).Scan(&quota))
		assert.Equal(t, item.want, quota)
	}
	var count int
	require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM public.project_limits WHERE team_id = $1`, inherited).Scan(&count))
	assert.Zero(t, count, "downgrade must not pin teams with no explicit override")
}

func TestVolumeQuotaMigrationRejectsAnUnrepresentableLegacyLimit(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	provider := migrationProvider(t, sqlDB)
	_, err = provider.DownTo(ctx, isolatedVolumeQuotaMigration-1)
	require.NoError(t, err)
	_, err = sqlDB.ExecContext(ctx, `UPDATE public.default_limits SET volume_quota_gib = $1`, int64(math.MaxInt32)+1)
	require.NoError(t, err)
	_, err = provider.Up(ctx)
	require.ErrorContains(t, err, "volume quota exceeds the vePFS maximum")
	var quota int64
	require.NoError(t, sqlDB.QueryRowContext(ctx, `SELECT volume_quota_gib FROM public.default_limits`).Scan(&quota))
	assert.Equal(t, int64(math.MaxInt32)+1, quota, "migration must not truncate settings")
}
