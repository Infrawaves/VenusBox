-- +goose Up
-- +goose StatementBegin
-- Not yet run anywhere: this rollout still hand-applies packages/orchestrator/cmd/tos-ref-count-consumer/schema.sql, which these statements mirror. Kept here, ready to run once this feature moves off manual schema management -- CREATE TABLE IF NOT EXISTS/CREATE OR REPLACE VIEW make it a no-op against a database that already has the hand-applied tables.
--
-- Table 1: edges, one row per "build_id depends on ancestor_id". Only makes the ref_count +1/-1 below idempotent under at-least-once delivery -- not queried to decide whether a build can be deleted.
--
-- env_id (envs.id / env_builds.env_id) lets a template soft-delete clear every edge it ever produced in one pass, without re-querying env_builds/env_build_assignments for build history.
CREATE TABLE IF NOT EXISTS public.tos_build_edges (
    build_id             uuid NOT NULL,
    ancestor_id          uuid NOT NULL,
    env_id               text NOT NULL DEFAULT '',
    source               text NOT NULL,
    recorded_at          timestamptz NOT NULL DEFAULT now(),
    deleted_at           timestamptz,
    PRIMARY KEY (build_id, ancestor_id)
);

CREATE INDEX IF NOT EXISTS idx_tos_build_edges_ancestor_id
    ON public.tos_build_edges (ancestor_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tos_build_edges_env_id
    ON public.tos_build_edges (env_id)
    WHERE deleted_at IS NULL;

-- Table 2: the only table a deletion decision should query. ref_count = 0
-- means no currently-live edge references this build as an ancestor.
CREATE TABLE IF NOT EXISTS public.tos_ancestor_reference_count (
    ancestor_id   uuid PRIMARY KEY,
    ref_count     int NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Convenience view for manual inspection: joins envs.last_spawned_at live at query time rather than a stale snapshot column. Purely informational, not read by any Go code or deletion logic.
CREATE OR REPLACE VIEW public.tos_build_edges_with_env AS
SELECT e.*, v.last_spawned_at AS env_last_spawned_at
FROM public.tos_build_edges e
LEFT JOIN public.envs v ON v.id = e.env_id;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS public.tos_build_edges_with_env;
DROP TABLE IF EXISTS public.tos_ancestor_reference_count;
DROP TABLE IF EXISTS public.tos_build_edges;
-- +goose StatementEnd
