-- +goose Up
-- +goose StatementBegin

-- platform_admins grants a user a platform-wide role (platform_admin /
-- super_admin), orthogonal to team membership. A user absent from this table
-- has no platform role.
CREATE TABLE IF NOT EXISTS public.platform_admins (
    user_id    uuid        NOT NULL,
    role       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id),
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT platform_admins_role_check CHECK (role IN ('platform_admin', 'super_admin'))
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS public.platform_admins;
-- +goose StatementEnd
