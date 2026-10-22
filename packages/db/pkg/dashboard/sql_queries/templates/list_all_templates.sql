-- Global (cross-team) template listing for the super-admin dashboard.
--
-- Unlike ListTeamTemplatesBy*, this is not scoped to a team and injects no
-- per-team default templates: it returns every team-owned template
-- (source = 'template') across the platform, each carrying its owning team's
-- id and name for attribution. Keyset-paginated by (created_at, id) DESC,
-- reusing the same display-only build/alias laterals as the team listing.

-- name: ListAllTemplatesByCreatedAtDesc :many
WITH ranked AS (
    SELECT
        e.id,
        e.created_at,
        e.updated_at,
        e.public,
        e.build_count,
        e.spawn_count,
        e.last_spawned_at,
        e.created_by,
        e.team_id
    FROM public.active_envs AS e
    WHERE
        e.source = 'template'
        AND (
            sqlc.arg(search)::text = ''
            OR e.id ILIKE '%' || sqlc.arg(search)::text || '%'
            OR EXISTS (
                SELECT 1 FROM public.env_aliases sa
                WHERE sa.env_id = e.id
                  AND (
                    sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                    OR COALESCE(sa.namespace, '') || '/' || sa.alias ILIKE '%' || sqlc.arg(search)::text || '%'
                  )
            )
        )
        AND (
            sqlc.narg(public)::bool IS NULL
            OR e.public = sqlc.narg(public)::bool
        )
        AND (e.created_at, e.id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::text)
    ORDER BY e.created_at DESC, e.id DESC
    LIMIT sqlc.arg(limit_plus_one)::int
)
SELECT
        r.id AS template_id,
        r.created_at,
        r.updated_at,
        r.public,
        r.build_count,
        r.spawn_count,
        r.last_spawned_at,
        r.created_by AS creator_id,
        r.team_id,
        t.name AS team_name,
        COALESCE(eb.id, '00000000-0000-0000-0000-000000000000'::uuid) AS build_id,
        COALESCE(eb.vcpu, 0)::bigint AS cpu_count,
        COALESCE(eb.ram_mb, 0)::bigint AS memory_mb,
        eb.total_disk_size_mb AS disk_size_mb,
        eb.envd_version AS envd_version,
        COALESCE(ea.aliases, ARRAY[]::text[])::text[] AS aliases,
        COALESCE(ea.names, ARRAY[]::text[])::text[] AS names
FROM ranked r
JOIN public.teams AS t ON t.id = r.team_id
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(alias ORDER BY alias) AS aliases,
        ARRAY_AGG(CASE WHEN namespace IS NOT NULL THEN namespace || '/' || alias ELSE alias END ORDER BY alias) AS names
    FROM public.env_aliases
    WHERE env_id = r.id
) ea ON TRUE
LEFT JOIN LATERAL (
    SELECT b.id, b.vcpu, b.ram_mb, b.total_disk_size_mb, b.envd_version
    FROM public.env_build_assignments AS ba
    JOIN public.env_builds AS b ON b.id = ba.build_id
    WHERE ba.env_id = r.id AND ba.tag = 'default' AND b.status_group = 'ready'
    ORDER BY ba.created_at DESC
    LIMIT 1
) eb ON TRUE
ORDER BY r.created_at DESC, r.id DESC;
