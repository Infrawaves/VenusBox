-- +goose Up
-- River's job tables live in their own schema, out of public. River's own
-- migrator creates them; this only gives it a place to put them.
-- IF NOT EXISTS 兜底：river schema 由 River 自己的 migrator 创建，可能在本迁移
-- 之前已存在（如残留/重建环境），用 IF NOT EXISTS 避免重复建 schema 失败。
CREATE SCHEMA IF NOT EXISTS river;

-- +goose Down
-- CASCADE because River migrates into this schema on its own stream, which
-- goose cannot unwind. Dropping the schema without its tables would fail once
-- River has run.
DROP SCHEMA river CASCADE;
