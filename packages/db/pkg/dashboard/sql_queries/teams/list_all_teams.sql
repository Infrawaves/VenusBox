-- name: ListAllTeams :many
-- Global (cross-team) team listing for the super-admin dashboard. Unlike
-- GetDashboardTeamsWithUsersTeamsWithTier it is not scoped to a user's
-- memberships: it returns every team with its limits. Keyset-paginated by
-- (created_at, id) DESC.
SELECT sqlc.embed(t), sqlc.embed(tl)
FROM "public"."teams" t
JOIN "public"."team_limits" tl ON tl.id = t.id
WHERE (t.created_at, t.id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::uuid)
  AND (
    sqlc.arg(search)::text = ''
    OR t.name ILIKE '%' || sqlc.arg(search)::text || '%'
    OR t.slug ILIKE '%' || sqlc.arg(search)::text || '%'
  )
  AND (
    -- Status precedence mirrors the UI: banned > blocked > active.
    cardinality(sqlc.arg(statuses)::text[]) = 0
    OR ('banned' = ANY(sqlc.arg(statuses)::text[]) AND t.is_banned)
    OR ('blocked' = ANY(sqlc.arg(statuses)::text[]) AND t.is_blocked AND NOT t.is_banned)
    OR ('active' = ANY(sqlc.arg(statuses)::text[]) AND NOT t.is_blocked AND NOT t.is_banned)
  )
ORDER BY t.created_at DESC, t.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;
