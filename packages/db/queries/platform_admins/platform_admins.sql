-- name: GetPlatformRoleByUserID :one
-- Returns the platform role granted to a user, or no row when the user has none.
SELECT role
FROM public.platform_admins
WHERE user_id = sqlc.arg(user_id)::uuid;

-- name: UpsertPlatformAdmin :exec
-- Seeds or updates a user's platform role. Used by the env-var seeding on startup.
INSERT INTO public.platform_admins (user_id, role)
VALUES (sqlc.arg(user_id)::uuid, sqlc.arg(role)::text)
ON CONFLICT (user_id) DO UPDATE SET role = EXCLUDED.role;

-- name: ListPlatformAdmins :many
-- Lists every platform admin with its role, newest first, for the super-admin admin-management view.
SELECT user_id, role, created_at
FROM public.platform_admins
ORDER BY created_at DESC, user_id DESC;

-- name: DeletePlatformAdmin :exec
-- Revokes a user's platform role by removing their row.
DELETE FROM public.platform_admins
WHERE user_id = sqlc.arg(user_id)::uuid;
