-- name: ListAllSnapshots :many
-- Global (cross-team) listing of paused sandboxes (snapshots) for the
-- super-admin dashboard. A row in snapshots means the sandbox is currently
-- paused; resuming consumes the row, so presence == "paused now". Keyset
-- paginated by (created_at, id) DESC where created_at is the pause time. Each
-- row carries its owning team's name, the base template alias, and the
-- resolved default build's resources for display.
SELECT
    s.id,
    s.sandbox_id,
    s.env_id,
    s.base_env_id,
    s.team_id,
    s.sandbox_started_at,
    s.created_at,
    s.auto_pause,
    s.origin_node_id,
    s.env_secure,
    s.allow_internet_access,
    s.metadata,
    t.name AS team_name,
    COALESCE(ea.alias, '') AS base_template_alias,
    COALESCE(eb.vcpu, 0)::bigint AS cpu_count,
    COALESCE(eb.ram_mb, 0)::bigint AS memory_mb,
    eb.total_disk_size_mb AS disk_size_mb,
    eb.envd_version AS envd_version
FROM public.snapshots s
JOIN public.teams t ON t.id = s.team_id
LEFT JOIN LATERAL (
    SELECT x.alias
    FROM public.env_aliases x
    WHERE x.env_id = s.base_env_id
    ORDER BY x.alias ASC
    LIMIT 1
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT eb.vcpu, eb.ram_mb, eb.total_disk_size_mb, eb.envd_version
    FROM public.env_build_assignments eba
    JOIN public.env_builds eb ON eb.id = eba.build_id
    WHERE eba.env_id = s.env_id AND eba.tag = 'default' AND eb.status_group = 'ready'
    ORDER BY eba.created_at DESC
    LIMIT 1
) eb ON TRUE
WHERE
    (
        sqlc.narg(team_id)::uuid IS NULL
        OR s.team_id = sqlc.narg(team_id)::uuid
    )
    AND (
        sqlc.arg(search)::text = ''
        OR s.sandbox_id ILIKE '%' || sqlc.arg(search)::text || '%'
        OR s.base_env_id ILIKE '%' || sqlc.arg(search)::text || '%'
        OR t.name ILIKE '%' || sqlc.arg(search)::text || '%'
        OR COALESCE(ea.alias, '') ILIKE '%' || sqlc.arg(search)::text || '%'
    )
    AND (s.created_at, s.id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::uuid)
ORDER BY s.created_at DESC, s.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;
