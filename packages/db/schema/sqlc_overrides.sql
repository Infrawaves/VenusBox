-- This file provides schema definitions for sqlc that can't be parsed from migrations.
-- Migrations wrapped in DO $$ blocks are not visible to sqlc's static parser.
-- These statements are not executed - they're only read by sqlc for code generation.

ALTER TABLE teams ADD COLUMN slug TEXT NOT NULL;

-- profile_picture_url was dropped from the main migrations but restored directly
-- in the database; the super-admin team queries select it via sqlc.embed(t).
ALTER TABLE teams ADD COLUMN profile_picture_url TEXT;

CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE IF NOT EXISTS billing.sandbox_logs (
    sandbox_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    vcpu BIGINT NOT NULL,
    ram_mb BIGINT NOT NULL,
    total_disk_size_mb BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    team_id UUID NOT NULL
);

-- platform_admins grants a user a platform-wide role (platform_admin /
-- super_admin), orthogonal to team membership. The real table is created
-- directly in the database; this definition exists only so sqlc can generate
-- the platform_admins queries.
CREATE TABLE IF NOT EXISTS public.platform_admins (
    user_id    uuid        NOT NULL,
    role       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id),
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT platform_admins_role_check CHECK (role IN ('platform_admin', 'super_admin'))
);
