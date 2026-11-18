-- TOS layer reference-counting: dev validation schema. Manual-only, not managed by packages/db/migrations.
--
-- Run with: psql "$POSTGRES_CONNECTION_STRING" -f schema.sql
--
-- If tos_build_edges already exists from before env_last_spawned_at was replaced by the tos_build_edges_with_env view below, drop the now-unused column once:
--   ALTER TABLE tos_build_edges DROP COLUMN IF EXISTS env_last_spawned_at;

-- Table 1: edges, one row per "build_id depends on ancestor_id". Only makes the ref_count +1/-1 below idempotent under at-least-once delivery — not queried to decide whether a build can be deleted.
--
-- env_id (envs.id / env_builds.env_id) lets a template soft-delete clear every edge it ever produced in one pass, without re-querying env_builds/env_build_assignments for build history.
CREATE TABLE IF NOT EXISTS tos_build_edges (
    build_id             uuid NOT NULL,
    ancestor_id          uuid NOT NULL,
    env_id               text NOT NULL DEFAULT '',
    source               text NOT NULL,
    recorded_at          timestamptz NOT NULL DEFAULT now(),
    deleted_at           timestamptz,
    PRIMARY KEY (build_id, ancestor_id)
);

CREATE INDEX IF NOT EXISTS idx_tos_build_edges_ancestor_id
    ON tos_build_edges (ancestor_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tos_build_edges_env_id
    ON tos_build_edges (env_id)
    WHERE deleted_at IS NULL;

-- Table 2: the only table a deletion decision should query. ref_count = 0
-- means no currently-live edge references this build as an ancestor.
CREATE TABLE IF NOT EXISTS tos_ancestor_reference_count (
    ancestor_id   uuid PRIMARY KEY,
    ref_count     int NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Convenience view for manual inspection: joins envs.last_spawned_at live at query time rather than a stale snapshot column. Purely informational, not read by any Go code or deletion logic.
CREATE OR REPLACE VIEW tos_build_edges_with_env AS
SELECT e.*, v.last_spawned_at AS env_last_spawned_at
FROM tos_build_edges e
LEFT JOIN envs v ON v.id = e.env_id;
