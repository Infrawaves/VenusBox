-- name: GetSuperAdminTeam :one
-- Single team with its limits for the super-admin team detail view. Same
-- projection as ListAllTeams, looked up by id.
SELECT sqlc.embed(t), sqlc.embed(tl)
FROM "public"."teams" t
JOIN "public"."team_limits" tl ON tl.id = t.id
WHERE t.id = sqlc.arg(team_id)::uuid;
