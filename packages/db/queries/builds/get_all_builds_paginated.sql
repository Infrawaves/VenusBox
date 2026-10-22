-- name: GetAllBuildsPage :many
-- Global (cross-team) build listing for the super-admin dashboard. Same
-- projection as GetTeamBuildsPage but without the team filter, and each row
-- carries its owning team's id and name for attribution.
SELECT
  b.id,
  b.status_group,
  b.reason,
  b.created_at,
  b.finished_at,
  b.vcpu,
  b.ram_mb,
  b.total_disk_size_mb,
  b.envd_version,
  b.team_id,
  t.name AS team_name,
  eba.env_id AS template_id,
  COALESCE(ea.alias, '') AS template_alias
FROM public.env_builds b
JOIN public.teams t ON t.id = b.team_id
JOIN LATERAL (
  SELECT a.env_id
  FROM public.env_build_assignments a
  -- active_envs (not envs): the build's env must exist and not be soft-deleted
  JOIN public.active_envs e ON e.id = a.env_id
  WHERE a.build_id = b.id
  ORDER BY a.created_at DESC, a.id DESC
  LIMIT 1
) eba ON TRUE
LEFT JOIN LATERAL (
  SELECT x.alias
  FROM public.env_aliases x
  WHERE x.env_id = eba.env_id
  ORDER BY x.alias ASC
  LIMIT 1
) ea ON TRUE
WHERE (b.created_at, b.id) < (
    sqlc.arg(cursor_created_at)::timestamptz,
    sqlc.arg(cursor_id)::uuid
  )
  AND b.status_group = ANY(sqlc.arg(statuses)::text[])
  AND (
    sqlc.arg(search)::text = ''
    OR b.id::text ILIKE '%' || sqlc.arg(search)::text || '%'
    OR eba.env_id ILIKE '%' || sqlc.arg(search)::text || '%'
    OR COALESCE(ea.alias, '') ILIKE '%' || sqlc.arg(search)::text || '%'
    OR t.name ILIKE '%' || sqlc.arg(search)::text || '%'
  )
ORDER BY b.created_at DESC, b.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;
