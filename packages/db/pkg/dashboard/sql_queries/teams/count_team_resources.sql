-- Counts the resources that block a team deletion. envs, snapshots and volumes
-- all reference teams with ON DELETE NO ACTION, so a team owning any of them
-- cannot be deleted until those are removed (which requires the orchestrator).
-- The super-admin "delete clean team" flow refuses deletion when any is > 0.

-- name: CountTeamBlockingResources :one
SELECT
    (SELECT COUNT(*) FROM public.envs WHERE team_id = sqlc.arg(team_id)::uuid)::bigint AS env_count,
    (SELECT COUNT(*) FROM public.snapshots WHERE team_id = sqlc.arg(team_id)::uuid)::bigint AS snapshot_count,
    (SELECT COUNT(*) FROM public.volumes WHERE team_id = sqlc.arg(team_id)::uuid)::bigint AS volume_count;
