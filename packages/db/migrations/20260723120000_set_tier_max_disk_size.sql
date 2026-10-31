-- +goose Up
-- max_disk_size_mb 必须 >= default_free_disk_size_mb（表级 CHECK 约束）。
-- 用 GREATEST 兜底：若某行已有的 default_free 比目标值还大（如 base_v1 被调过
-- 的部署，default_free=61400），则 max 取 default_free，避免迁移违反约束而失败。
UPDATE public.tiers
SET max_disk_size_mb = GREATEST(
    CASE WHEN id = 'base_v1' THEN 25600 ELSE 51200 END,
    default_free_disk_size_mb
);

-- +goose Down
UPDATE public.tiers
SET max_disk_size_mb = default_free_disk_size_mb + 25000;
