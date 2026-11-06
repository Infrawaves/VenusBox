-- +goose Up
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- Capacity, in GiB, that all persistent volumes of a team may hold together.
-- Zero leaves the team's volumes unbounded, which is what every team had before
-- this column existed.
ALTER TABLE public.default_limits
    ADD COLUMN volume_quota_gib bigint NOT NULL DEFAULT 0
        CHECK (volume_quota_gib >= 0);

-- Nullable: the management push does not have to carry it, and a row without a
-- value falls back to default_limits.
ALTER TABLE public.project_limits
    ADD COLUMN volume_quota_gib bigint
        CHECK (volume_quota_gib IS NULL OR volume_quota_gib >= 0);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

ALTER TABLE public.project_limits DROP COLUMN volume_quota_gib;
ALTER TABLE public.default_limits DROP COLUMN volume_quota_gib;
-- +goose StatementEnd
