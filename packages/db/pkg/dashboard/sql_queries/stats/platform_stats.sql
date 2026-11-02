-- name: GetPlatformStats :one
-- Platform-wide totals for the super-admin overview. Each count matches the
-- source the corresponding list view paginates over.
SELECT
    (SELECT COUNT(*) FROM "public"."teams")::bigint AS team_count,
    (SELECT COUNT(*) FROM "public"."users")::bigint AS user_count,
    (
        SELECT COUNT(*)
        FROM "public"."active_envs"
        WHERE source = 'template'
    )::bigint AS template_count,
    (SELECT COUNT(*) FROM "public"."env_builds")::bigint AS build_count;
