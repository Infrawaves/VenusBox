-- +goose Up
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

DO $validate$
BEGIN
    IF EXISTS (SELECT 1 FROM public.default_limits WHERE volume_quota_gib > 2147483647)
       OR EXISTS (SELECT 1 FROM public.project_limits WHERE volume_quota_gib > 2147483647) THEN
        RAISE EXCEPTION 'volume quota exceeds the vePFS maximum of 2147483647 GiB; correct the legacy quota before migrating';
    END IF;
END
$validate$;

-- Volume quotas have their own owner and revision, independently of the
-- upstream project limits projection. Keep its old columns for rollback.
CREATE TABLE public.volume_quota_defaults (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    quota_gib bigint NOT NULL CHECK (quota_gib BETWEEN 0 AND 2147483647)
);

INSERT INTO public.volume_quota_defaults (quota_gib)
SELECT volume_quota_gib FROM public.default_limits;

CREATE FUNCTION public.volume_quota_defaults_keep_row()
    RETURNS trigger
    LANGUAGE plpgsql
AS $keep_row$
BEGIN
    RAISE EXCEPTION 'public.volume_quota_defaults must keep its row; update it instead';
END
$keep_row$;

CREATE TRIGGER volume_quota_defaults_keep_row
    BEFORE DELETE OR TRUNCATE ON public.volume_quota_defaults
    FOR EACH STATEMENT EXECUTE FUNCTION public.volume_quota_defaults_keep_row();

CREATE TABLE public.team_volume_quotas (
    team_id uuid PRIMARY KEY REFERENCES public.teams(id) ON DELETE CASCADE,
    quota_gib bigint CHECK (quota_gib BETWEEN 0 AND 2147483647),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    file_system_id text CHECK (file_system_id IS NULL OR file_system_id <> ''),
    fileset_path text CHECK (fileset_path IS NULL OR (fileset_path LIKE '/%/' AND fileset_path <> '/')),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((file_system_id IS NULL) = (fileset_path IS NULL))
);

-- Preserve explicit zero as well as positive overrides. A missing/NULL
-- override continues to inherit the deployment's independent default.
INSERT INTO public.team_volume_quotas (team_id, quota_gib)
SELECT team_id, volume_quota_gib FROM public.project_limits
WHERE volume_quota_gib IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

UPDATE public.default_limits d
SET volume_quota_gib = q.quota_gib
FROM public.volume_quota_defaults q;

UPDATE public.project_limits p
SET volume_quota_gib = q.quota_gib
FROM public.team_volume_quotas q
WHERE p.team_id = q.team_id;

-- A team first given an override after the split may have no legacy limits
-- row. Preserve its quota for an older binary, using its currently effective
-- defaults for the required legacy fields. Only the downgrade pins those
-- unrelated defaults; normal operation never creates a project_limits row.
INSERT INTO public.project_limits (
    team_id, max_length_hours, concurrent_sandboxes, concurrent_template_builds,
    max_vcpu, max_ram_mb, disk_mb, events_ttl_days, default_free_disk_size_mb,
    max_disk_size_mb, max_free_disk_size_mb, api_team_rps_list, api_team_rps_delete,
    max_secrets, volume_quota_gib
)
SELECT q.team_id, d.max_length_hours, d.concurrent_sandboxes, d.concurrent_template_builds,
       d.max_vcpu, d.max_ram_mb, d.disk_mb, d.events_ttl_days, d.default_free_disk_size_mb,
       d.max_disk_size_mb, d.max_disk_size_mb, d.api_team_rps_list, d.api_team_rps_delete,
       d.max_secrets, q.quota_gib
FROM public.team_volume_quotas q
CROSS JOIN public.default_limits d
WHERE q.quota_gib IS NOT NULL
ON CONFLICT (team_id) DO NOTHING;

DROP TABLE public.team_volume_quotas;
DROP TABLE public.volume_quota_defaults;
DROP FUNCTION public.volume_quota_defaults_keep_row();
-- +goose StatementEnd
