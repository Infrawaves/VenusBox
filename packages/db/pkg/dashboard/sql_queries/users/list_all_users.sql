-- name: ListAllUsers :many
-- Global user listing for the super-admin dashboard: every platform user with
-- the teams they belong to. Email is resolved separately via the identity
-- service. Keyset-paginated by (created_at, id) DESC.
SELECT
    u.id,
    u.created_at,
    COALESCE(
        ARRAY_AGG(t.name ORDER BY t.name) FILTER (WHERE t.name IS NOT NULL),
        '{}'
    )::text[] AS team_names,
    COALESCE(
        ARRAY_AGG(t.id ORDER BY t.name) FILTER (WHERE t.id IS NOT NULL),
        '{}'
    )::uuid[] AS team_ids
FROM "public"."users" u
LEFT JOIN "public"."users_teams" ut ON ut.user_id = u.id
LEFT JOIN "public"."teams" t ON t.id = ut.team_id
WHERE (u.created_at, u.id) < (sqlc.arg(cursor_created_at)::timestamptz, sqlc.arg(cursor_id)::uuid)
GROUP BY u.id, u.created_at
ORDER BY u.created_at DESC, u.id DESC
LIMIT sqlc.arg(limit_plus_one)::int;

-- name: ListUsersByIDs :many
-- Same projection as ListAllUsers but restricted to a set of user ids. Used by
-- the super-admin search path, where matching user ids come from the identity
-- directory scan. Ordered by (created_at, id) DESC to match the list view.
SELECT
    u.id,
    u.created_at,
    COALESCE(
        ARRAY_AGG(t.name ORDER BY t.name) FILTER (WHERE t.name IS NOT NULL),
        '{}'
    )::text[] AS team_names,
    COALESCE(
        ARRAY_AGG(t.id ORDER BY t.name) FILTER (WHERE t.id IS NOT NULL),
        '{}'
    )::uuid[] AS team_ids
FROM "public"."users" u
LEFT JOIN "public"."users_teams" ut ON ut.user_id = u.id
LEFT JOIN "public"."teams" t ON t.id = ut.team_id
WHERE u.id = ANY(sqlc.arg(user_ids)::uuid[])
GROUP BY u.id, u.created_at
ORDER BY u.created_at DESC, u.id DESC;
