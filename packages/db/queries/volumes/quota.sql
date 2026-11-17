-- All quota transactions take the default lock first, then the team lock.
-- FOR SHARE lets independent teams proceed together while preventing a
-- default change halfway through a quota application.
-- name: LockVolumeQuotaDefault :one
SELECT quota_gib FROM public.volume_quota_defaults WHERE id = true FOR SHARE;

-- name: EnsureTeamVolumeQuota :exec
INSERT INTO public.team_volume_quotas (team_id)
SELECT id FROM public.teams WHERE id = sqlc.arg(team_id)::uuid
ON CONFLICT (team_id) DO NOTHING;

-- name: LockTeamVolumeQuota :one
SELECT team_id, quota_gib, revision, file_system_id, fileset_path
FROM public.team_volume_quotas
WHERE team_id = sqlc.arg(team_id)::uuid
FOR UPDATE;

-- name: ListTeamVolumeQuotaTypes :many
SELECT DISTINCT volume_type FROM public.volumes
WHERE team_id = sqlc.arg(team_id)::uuid
ORDER BY volume_type;

-- Called only while holding the team's quota row lock. A filesystem binding
-- survives quota revisions and inheritance changes.
-- name: BindTeamVolumeQuotaFileset :exec
UPDATE public.team_volume_quotas
SET file_system_id = sqlc.arg(file_system_id)::text,
    fileset_path = sqlc.arg(fileset_path)::text,
    updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid;

-- name: SetTeamVolumeQuota :exec
UPDATE public.team_volume_quotas
SET quota_gib = sqlc.narg(quota_gib)::bigint,
    revision = sqlc.arg(revision)::bigint,
    updated_at = now()
WHERE team_id = sqlc.arg(team_id)::uuid;
