-- TOS layer reference-counting: dev validation schema.
--
-- Manual-only. Not managed by packages/db/migrations. Run by hand against a
-- dev Postgres instance to validate the write path in
-- tos-ref-count-consumer before deciding whether this graduates into a real
-- migration.
--
-- Run with: psql "$POSTGRES_CONNECTION_STRING" -f schema.sql

-- Table 1: edges. Records "build_id depends on ancestor_id", one row per
-- dependency relationship. The sole purpose of this table is to make the
-- ref_count +1/-1 below idempotent under at-least-once event delivery — it
-- is NOT queried to decide whether a build can be deleted.
--
-- env_id carries the owning template's id (envs.id / env_builds.env_id,
-- e.g. "ihvujp327d7kt4rox2l5") straight from the upload's objectMetadata, so
-- a template soft-delete can clear every edge it ever produced in one pass
-- without re-querying env_builds/env_build_assignments for build history.
-- Empty when the uploader had no template_id in objectMetadata (dev tooling).
CREATE TABLE IF NOT EXISTS tos_build_edges (
    build_id      uuid NOT NULL,
    ancestor_id   uuid NOT NULL,
    env_id        text NOT NULL DEFAULT '',
    source        text NOT NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz,
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
