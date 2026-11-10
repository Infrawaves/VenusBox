# TOS 层引用计数方案（实时维护版）

状态：核心写入链路已在 dev 环境（`hk-dev-worker1`/`hk-dev-api`，分支 `tos-clear`）跑通并用真实构建验证，细节见第 9 节。消费逻辑已从独立二进制改为跟随 orchestrator 进程内置运行，详见第 2.1 节。本文档记录当前实现、已验证的结论，以及两个尚未拍板的事项：第 7 节的设计分叉点（已选定路线 B）和第 11.3 节两张表要不要转正式 migration（待确认，表结构还在复核中），留给评审会决定。

## 1. 和之前方案的关系

`tos-cleanup-plan.md` 描述的是"扫描式"清理：每次要清理，都重新从 PG 算 live roots、重新读 TOS header 展开依赖闭包，算出 KEEP 集合再做差集。这套方法已经验证可行，但代价是每次都要对几十万到百万级的 build 重新发请求读 header，耗时按小时计，而且只能是"事后批量跑一次"，不是持续可用的状态。

本方案的目标：把"每次清理前现扫 TOS"，换成"build/pause 发生时实时维护一张引用计数表"，让"这个层还有没有人用"变成一次毫秒级查表，不用每次都重新扫描全量。两者不是替代关系——计数归零只是"候选信号"，真正执行物理删除前，仍然要走 `tos-cleanup-plan.md` 里已经定好的宽限期、二次复核、小批量放量这些安全步骤。本方案解决的是"怎么高效找出候选"，不改变"候选之后怎么安全删除"这部分。

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│  orchestrator（每个 worker 节点）                                     │
│                                                                       │
│  构建/pause 完成 → header 写入 TOS 成功                               │
│       │                                                              │
│       ├─ runV3（build_upload_v3.go，当前环境实测的默认路径）           │
│       └─ uploadFramed/runV4（build_upload_v4.go，压缩/V5 开启时走这条） │
│       │                                                              │
│       └─→ publishTOSLayerEvent（uploads.go）                         │
│             取 header.Mapping.Builds() + objectMetadata[template_id]  │
│             组装 JSON，best-effort LPUSH 到 Redis List                │
│             （失败只记日志，绝不影响 build 本身返回成功）               │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                  Redis List: tos.layer.ref.events
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  消费逻辑（pkg/tosrefcount），现在跑在 orchestrator 进程内部，           │
│  template-manager 角色下（factories/run.go, setupTOSRefCount）         │
│  ——不再需要单独手动起一个进程，见本节下方"部署形态"说明                  │
│                                                                       │
│  循环 1：BRPOP 事件 → 解析 → 写两张表（见第 3 节），一个 PG 事务内完成  │
│                                                                       │
│  循环 2：每 30s 轮询 envs.deleted_at（新增软删） →                     │
│          按 env_id 清掉该模板历史上产生的全部边 → 对应 ancestor -1     │
│          （不改 API 代码，纯读取生产路径已经维护的字段）                │
│                                                                       │
│  两个循环都套了一层 Redis 分布式锁（pkg/tosrefcount.RunElected）才真正  │
│  执行写入——build 节点上 template-manager 角色是可扩缩容的多实例池，     │
│  不是单例，这层锁保证全局任意时刻只有一个实例在写这两张表               │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PostgreSQL（dev-e2b-pg，和业务库同一实例，独立两张表，不进正式迁移）    │
│                                                                       │
│  tos_build_edges              —— 谁依赖谁，驱动计数幂等，不被删除判断查 │
│  tos_ancestor_reference_count —— 删除判断唯一要查的表                 │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼（未来，本方案不包含，衔接 tos-cleanup-plan.md）
              候选可清理 = ref_count=0 的 ancestor_id
                     ↓
         宽限期过滤 → 二次复核 → 小批量物理删除 + 观察
```

**没有改动 API 代码**：软删除信号是消费者定期读 `envs.deleted_at`（生产删除路径早已维护的字段），不是从 `template_delete.go` 里新开一个事件钩子。这是刻意的选择——详见第 5 节。

### 2.1 部署形态：跑在 orchestrator 进程内部，不再需要手动启动

最初验证阶段，消费逻辑是 `cmd/tos-ref-count-consumer` 一个独立二进制,需要手动在某台机器上常驻运行。这对生产不现实——没有人会记得每次重启都要去手动拉起它。现在已经改成跟着 orchestrator 主进程一起跑:

- **代码位置**：消费逻辑本身（BRPOP 事件循环、软删除扫描循环、两张表的写入）抽到了 `packages/orchestrator/pkg/tosrefcount` 包。`packages/orchestrator/pkg/factories/run.go` 的 `setupTOSRefCount` 在 `services.RunsTemplateManager()` 为真的节点上调用这个包,作为两个后台 goroutine（事件消费、软删除扫描）随 orchestrator 一起启动、一起随 `SIGTERM` 优雅关闭。
- **开关粒度**：只看新加的配置项 `TOS_REF_COUNT_POSTGRES_CONNECTION_STRING` 是否设置（环境变量，对应 `cfg.Config.TOSRefCountPostgresConnectionString`）。为空就是功能完全关闭——不尝试连 Postgres,不起任何 goroutine,等价于这段代码不存在。配了才启动,和 `ClickhouseConnectionString` 现在的写法是同一个约定,不需要再叠一层 feature flag。
- **为什么需要分布式锁**：build 节点上 `template-manager` 角色本身是可扩缩容的多实例池（见 `docs/ARCHITECTURE.md`"build nodes run the same binary in template-manager mode"),不是单例。如果每个实例都各自跑一份消费循环,对两张表的 `+1`/`-1` 虽然幂等不会算错,但软删除扫描会变成多实例同时轮询 `envs.deleted_at`,冗余且没有意义。为此加了 `pkg/tosrefcount.RunElected`,用现成的 `packages/shared/pkg/redis/lock.go`（`bsm/redislock`,API 侧已经在用同一套）抢一个全局唯一的 leader 锁（key `tos-ref-count-consumer-leader`,TTL 30s,10s 续期一次),只有抢到锁的那个实例真正执行读写;锁丢失（比如该实例到 Redis 的网络中断导致续期失败）会立刻停止写入,释放锁后，另一个实例的下一轮尝试会在数秒内抢到并接管，不需要人工介入。
- **Postgres 连接失败不是致命错误**：和 `openClickhouseEndpoints` 不同,`setupTOSRefCount` 如果连不上 Postgres,只记一条 error 日志,orchestrator 本身继续正常启动、继续跑 sandbox/build——这条链路从头到尾都是旁路,不应该因为它连不上库就拖垮主业务。
- **独立二进制 `tos-ref-count-consumer` 保留，改成薄封装**：不再是生产运行方式,但仍有两个用途:
  1. `tos-ref-count-consumer consume`——本地开发时,不想起一整个 orchestrator,只想对着 Redis/Postgres 验证这段逻辑,或者 orchestrator 的in-process 消费者出问题时临时手动起一份作为替代。它走的是同一个 `RunElected` 锁,所以可以和 orchestrator 的 in-process 消费者同时存在而不冲突——谁抢到锁谁写,不会出现双写。
  2. `tos-ref-count-consumer backfill -input result.jsonl`——对应第 6 节的历史回填:读 `tos-layer-scan` 输出的 JSON Lines,对每个 build 从 `env_builds.env_id` 查出所属模板,写入边,`source` 标成 `backfill_scan`。这条路径不需要跑在 orchestrator 里,是一次性批处理,保留独立二进制正好合适。

## 3. 当前表结构（已在 dev 建表验证，`packages/orchestrator/cmd/tos-ref-count-consumer/schema.sql`）

```sql
-- 表1：边表。记录"build_id 依赖 ancestor_id"，一行一条依赖关系。
-- 唯一作用是让表2的 +1/-1 在事件重复投递下保持幂等——不是删除判断要查的表。
-- ancestor_id 包含 build_id 自己（自引用边，不过滤）——见第7节，这是让 ref_count==0
-- 可以单独作为删除依据、不需要再查 live roots 的关键设计。
CREATE TABLE tos_build_edges (
    build_id             uuid NOT NULL,
    ancestor_id          uuid NOT NULL,
    env_id               text NOT NULL DEFAULT '',   -- envs.id，来自 objectMetadata["template_id"]
    env_last_spawned_at  timestamptz,                 -- envs.last_spawned_at 的一次性快照，纯展示用
    source               text NOT NULL,               -- 'backfill_scan' | 'live_upload'
    recorded_at          timestamptz NOT NULL DEFAULT now(),
    deleted_at           timestamptz,                 -- 软删标记，不物理删行，保留审计轨迹
    PRIMARY KEY (build_id, ancestor_id)
);

-- 表2：删除判断唯一要查的表。ref_count = 0 表示没有任何活跃边把这个 build 当祖先。
CREATE TABLE tos_ancestor_reference_count (
    ancestor_id   uuid PRIMARY KEY,
    ref_count     int NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
```

设计要点：

- **放 PostgreSQL，不放 Redis**：这是清理删除的权威依据，不是缓存，丢了会导致误删；`+1`/`-1` 需要事务保证，PG 天然支持。
- **`env_id` 记在边上，不是单独反查**：一个模板可以有多次历史构建（`envs.build_count` 已验证可以 > 1），软删除时要清掉它*历史上所有*构建产生的边，不只是当前指向的那个。把 `env_id` 直接写在每条边上，软删除时一条 `UPDATE ... WHERE env_id = $1` 就能清完，不需要反查 `env_builds`/`env_build_assignments` 展开历史 build 列表。
- **`CHECK (ref_count >= 0)`**：一旦出现负数就是明确的 bug 信号（某次 `-1` 被执行了两次），让写入直接报错，比默默产生脏数据更安全。

## 4. 增量写入：边的幂等插入如何驱动计数

```
消费者收到一条事件：{build_id, env_id, ancestor_ids: [X, Y, Z]}

1. 在表2里确保 build_id 自己有一行（ref_count 默认 0，ON CONFLICT DO NOTHING）
   —— 这一步解决了"刚诞生、还没有任何下游依赖的 build，查表2应该查得到、而不是查无此行"

2. 对 X, Y, Z 各自执行：
   INSERT INTO tos_build_edges (build_id, ancestor_id, env_id, ...)
   VALUES (...) ON CONFLICT (build_id, ancestor_id) DO NOTHING
   RETURNING true

   只有这条 INSERT 真的插入了新行（之前这条边不存在），才对该 ancestor_id 执行 ref_count +1
```

这个"先插边、看插入是否成功、再决定要不要 +1"的写法，是整套方案里保证计数正确性的核心机制：消息队列普遍是"至少一次投递"，同一个事件可能因为消费者重启、ack 丢失被处理两次；边表的主键天然幂等（同一条边重复插入第二次会被 `DO NOTHING` 挡住），用这个结果去控制要不要 `+1`，就把"至少一次投递"转换成了"恰好一次生效"，不需要额外的去重表。

**只有模板最终层触发推送，构建过程中的中间缓存层不触发**（dev 环境真实构建中发现并修正）：一次模板构建，Dockerfile 的每个阶段（`base`/`USER root`/`resize-disk`/`finalize`/`optimize` 等）各自生成一个 build_id，各自调用一次 `UploadSnapshot`。每一层的 `Mapping.Builds()` 都包含从它自己往下数到基础镜像的**完整祖先链**，不是只有"直接上一层"。如果每一层都各自推送一次事件，一个 5 层的构建会产生 `1+2+3+4+5=15` 条边（同一条链被重复展开记录 5 遍），`ref_count` 实际度量的会变成"这次构建内部有多少层把它当祖先"，不是"有多少个模板依赖它"。

修复方式是在 `uploads.go` 的 `publishTOSLayerEvent` 里加一道判断：只有 `buildOrigin != ObjectOriginTemplateBuildCache` 才推送（即排除中间缓存层，模板最终层 `ObjectOriginTemplateBuild`、pause 快照 `ObjectOriginPause`、独立快照模板 `ObjectOriginSnapshotTemplate` 都正常推送）。`buildOrigin` 从调用方已经持有的 `objectMetadata[ObjectMetadataBuildOrigin]` 读取，三个调用点（`build_upload_v3.go` 两处、`build_upload_v4.go` 一处）改成把这个值传进去，不需要改函数之外的任何调用链。改完之后一次 5 层构建只会写 5 条边（最终层自己的完整祖先链，不含自己则是 4 条祖先 + 1 条自引用），不再是 15 条。

减计数走同样的幂等保护：软删除时 `UPDATE tos_build_edges SET deleted_at = now() WHERE env_id = $1 AND deleted_at IS NULL`，这条语句本身的 `WHERE deleted_at IS NULL` 保证重复执行时第二次匹配不到任何行，不会重复 `-1`。

## 5. 软删除怎么触发：读 `envs.deleted_at`，不改 API 代码

最初设想是在 `template_delete.go` 里加一个事件钩子，软删除时同步发一条消息。**实测代码后放弃了这个设计**：

- `envs.deleted_at` 已经是生产删除路径（`packages/api/internal/handlers/template_delete.go`）会设置的字段，这件事已经在做、已经可靠。
- 在 API 代码里插钩子，意味着改动一条生产关键路径（哪怕是 best-effort 的事件推送），需要更谨慎的评审和回归测试。
- 换成"消费者定期轮询 `envs.deleted_at`"，是一个纯读取、零侵入的方案：不改 API 一行代码，只是多了一个最长 30 秒的延迟（当前扫描间隔），这个延迟对于"回收存储空间"这个场景完全可以接受。

消费者维护一个时间水位线，每次只查 `deleted_at > 水位线` 的新增软删记录，避免每次全表扫描；水位线丢失（比如进程重启）只会导致下一轮扫描范围变大，不影响正确性（因为清边操作本身是幂等的）。

## 6. 历史回填（尚未实施，设计已定）

用已有的只读工具 `packages/orchestrator/cmd/tos-layer-scan/main.go`：

```
1. 确定回填截止时间点 T0
2. 导出 T0 时刻的 live roots（env_build_assignments 等，复用 tos-cleanup-plan.md 验证过的 SQL）
3. tos-layer-scan 批量扫描，展开祖先闭包，输出 JSON Lines
4. 写入表1，source='backfill_scan'，只处理 created_at < T0 的 build
5. 对表1聚合一次，批量初始化表2的 ref_count
```

**上线顺序：增量机制必须先于历史回填上线**。增量机制一旦上线，从那一刻起所有新产生的 build 都会被正确记录；回填只需要处理 `created_at < T0`（T0 = 增量机制实际上线时刻）的历史数据，和增量路径不会重叠、不会重复计数。

## 7. 自引用边：已确定方案（路线 B）

**问题背景**：一个刚构建完成、从未被任何后续 build 依赖过的 build（比如它当前就是某个模板唯一指向的那个），如果不处理自引用，现在 `ref_count = 0`。这个 0 没法区分"这个 0 是因为它刚诞生、现在还是活跃的 live root"，还是"这个 0 是因为它已经彻底没人要了，可以删"。

**已确定的方案**：不过滤 `id == buildID` 这条特殊情况，让 build 自己的 id 和其他祖先一样走正常插边流程（`handleEvent` 里已去掉这段过滤），使 build 刚诞生时 `ref_count` 从 0 变成 1。软删除时，`UPDATE tos_build_edges SET deleted_at = now() WHERE env_id = $1` 这条已有语句**同时覆盖这个模板历史上所有 build 的自引用边**（不只是当前指向的那个顶层 build），对应 `-1` 一起执行。

判断能否删除只需要查一个地方：

```
能删除 = ref_count == 0（查表2，不需要再查 live roots）
```

**安全性论证**（曾经讨论过"模板换版本但未软删除"场景会不会导致误判，结论是不会，论证如下）：

消费者代码里唯一会执行 `-1` 的路径是 `applyTemplateSoftDelete`，且只在 `envs.deleted_at` 被设置（模板被软删除）这一个事件触发，按 `env_id` 批量清掉该模板历史上所有 build 的边。没有第二条路径会做减法。

"模板重新构建、换了指向"这件事本身**不会触发任何 `-1`**——旧 build 不再是当前指向的那个，但只要它所属的模板没有被删除，它的自引用边不会被清掉，`ref_count` 会一直停留在 1（或更高），这是"虚高"，不是"提前归零"。虚高只会让系统更保守（继续判断为不能删），不会让系统更危险（误判为能删）。唯一能让 `ref_count` 归零的事件就是模板被删除那一刻的批量 `-1`，没有任何中间状态会提前把它清零。所以：**只要观察到 `ref_count == 0`，就说明这个 build 所属的每一条自引用边都已经被某次软删除清掉过，即它必然已经没有任何活跃模板指向它了，这个 0 是可信的，不需要额外核对 live roots。**

- **优点**：删除判断只查表2一个地方，不需要再现查 live roots，查询和实现都更简单。
- **已接受的代价**：在"模板换版本但未被软删除"的窗口期内，旧 build 的 `ref_count` 会偏高（不是 0），导致它暂时不会进入可删除候选——这只是让候选清单出现得比理论上更晚，不影响安全性。
- **已完成的改动**：`tos-ref-count-consumer/main.go` 的 `handleEvent` 已去掉 `if id == buildID { continue }` 过滤；`applyUploadEvent` 开头"预先插入 ref_count=0"那一步保留作为防御性兜底（`ON CONFLICT DO NOTHING`，正常情况下不会触发，因为自引用边会先把它插出来）。

## 7a. live roots 怎么查（当前方案不需要，留作备用/核对用）

日常判断候选只看 `ref_count == 0`，不需要每次都查 live roots——但作为双保险，**真正执行删除前**，对这批候选跑一次下面这条查询，当场核对，不持久化、不单独加字段：

- 不在表2加 `ref_status` 之类的列：live roots 本身随时在变（模板随时可能被创建/删除/换版本），存成列需要额外的定时同步机制保证不过期，等于又引入一个新的一致性问题,得不偿失。
- 改成在删除执行的那一刻现查、现判断，查询结果只用这一次，跟候选清单一起走完这次删除流程就丢弃。
- 预期结果是：`ref_count == 0` 的候选，正常情况下也不会出现在 live roots 里（这是路线 B 安全性论证的推论）。如果真的查到某个候选同时在 live roots 里，说明 `ref_count` 和数据库权威状态不一致，这是 bug 信号，必须跳过这个候选并报警排查，不能删——不能假设这种情况"可以理解"而直接放行。

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

SELECT DISTINCT build_id FROM (
    -- 来源1：活跃模板/快照，通过 assignment 关联的、状态是 pending/in_progress/ready 的 build。
    -- 不能只查 tag='default'，要查全部 tag，因为模板支持按 build UUID 直接启动历史版本。
    SELECT eba.build_id
    FROM active_envs e
    JOIN env_build_assignments eba ON eba.env_id = e.id
    JOIN env_builds eb ON eb.id = eba.build_id
    WHERE eb.status_group IN ('pending', 'in_progress', 'ready')

    UNION

    -- 来源2：正在构建中的（可能还没来得及建立 assignment）。
    SELECT build_id FROM active_template_builds

    UNION

    -- 来源3：snapshot_templates 自己独立记录的 build_id（防止和 assignment 不一致）。
    SELECT st.build_id
    FROM snapshot_templates st
    JOIN active_envs e ON e.id = st.env_id
    WHERE st.build_id IS NOT NULL
) AS roots;

ROLLBACK;
```

要点：

- `active_envs` 是视图，等价于 `SELECT * FROM envs WHERE deleted_at IS NULL`，自动排除软删除的模板。
- `REPEATABLE READ READ ONLY` 保证这次查询看到一致快照，且是只读事务，不锁任何东西，不影响线上读写。
- 三个来源缺一个都可能漏判真实活跃的 build——这条查询已经在之前的扫描式方案（`tos-cleanup-plan.md`）里验证过，这里直接复用，不是新设计。

拿到这批 `build_id` 之后，和 `ref_count = 0` 的候选清单做差集：

```sql
-- 真正可以进入候选的，是 ref_count=0 且不在 live roots 里的
SELECT r.ancestor_id FROM tos_ancestor_reference_count r
WHERE r.ref_count = 0
  AND r.ancestor_id NOT IN (<live roots 查询结果>);
```

这条查询目前只是设计好的 SQL，**还没有写进任何代码**——需要在实现"候选可清理集合"那一步（架构图里黄褐色虚线框）时，把这两段查询接起来，自动生成候选清单，而不是靠人工分别跑两次、手动做差集。

需要注意：`r.ancestor_id NOT IN (<live roots 查询结果>)` 这一步是双保险检查，不是候选筛选的主要依据——主要依据永远是 `ref_count = 0`。这条 `NOT IN` 的作用是在真正删除前兜底确认没有不一致，预期命中率应该是0（如果不是0要停下来查原因），不是用来"再过滤掉一批"。

## 8. 删除判断和之前方案的衔接

无论选哪条路线，`ref_count = 0`（路线 A）或 `ref_count = 0 且确认无自引用残留`（路线 B 完整版）都只是**候选信号**，不是"可以立即物理删除"的判断。真正执行删除前，仍然要走 `tos-cleanup-plan.md` 已经定义的安全流程：

1. 宽限期过滤（排除 TOS 对象 `LastModified` 过近的候选，防止并发竞态误删正在写入的数据）。
2. 删除前二次复核（重新确认候选没有在复核窗口期间被新的构建重新引用）。
3. 小批量物理删除 + 观察 + 逐步放量。

## 9. 已经验证过的结论（dev 环境真实数据）

- **核心前提已验证**：不需要客户端配合、不需要事后工具介入，orchestrator 在构建完成的那一刻，直接在内存里读 `header.Mapping.Builds()` 就能拿到完整、准确的依赖链，和独立工具事后重新读 TOS 得到的结果完全一致（build `9d24b2f8`/`079f5f28` 等多次真实构建验证）。
- **两条独立上传路径都要覆盖**：`build_upload_v3.go`（`runV3`）和 `build_upload_v4.go`（`uploadFramed`/`runV4`）是完全独立、互不调用的实现，走哪条取决于压缩开关和 feature flag，与 `skip_cache` 参数无关。最初只改了 V4 路径，导致当前测试环境（默认走 V3）完全没有触发任何写入，排查后两条路径都已补上验证逻辑。
- **写入链路端到端跑通**：真实构建触发 → `uploads.go` 的 `publishTOSLayerEvent` 推送事件 → `tos-ref-count-consumer` 消费并写入两张表 → 查表得到的 `ref_count` 符合预期（多次构建共享同一祖先时正确累加）。
- **"build_id 自己要不要在表2登记"已经发现并修复**：最初版本只在 ancestor_id 上登记行，导致刚诞生、还没有下游依赖的 build 查表2查不到任何记录（而不是查到 `ref_count=0`）。已修复为每次构建都先确保 build_id 自己在表2有一行。
- **软删除批量清边端到端验证通过**：同一模板两次构建（`a940dbed`、`5f856b1f`）各自依赖同三层祖先，`ref_count` 正确累加到 2；模板软删除后，这两个 build 产生的全部 6 条边在同一次扫描里被一次性清掉，三层祖先的 `ref_count` 正确归零。证明"按 `env_id` 批量清边"这个设计（路线 B 依赖的关键机制）已经用真实数据跑通，不是只停留在设计阶段。
- **`env_id` 一度为空的问题已排查并修复**：早期测试中 `tos_build_edges.env_id` 持续为空，逐层核实 API 请求日志、`TemplateConfig` 构造、`NewUpload` 参数传递均无问题后，确认是 `layer_executor.go` 构造 `objectMetadata` 这一处代码的问题，修复后新构建的 `env_id` 正确写入，软删除扫描也能正常按 `env_id` 匹配清边。
- **自引用边（路线 B）已用真实构建验证**：去掉 `handleEvent` 里的自引用过滤后，真实构建（`83df0e13` 等）在边表产生了预期的 `(build_id, build_id)` 自引用边，对应 `ref_count` 从 0 变成 1；软删除按 `env_id` 批量清边时，这条自引用边和其他祖先边一起被清掉，`ref_count` 正确归零。
- **中间缓存层去重已用真实构建发现并修复**：真实构建日志显示一次模板构建会产生多个 build_id（Dockerfile 每个阶段各一个，例如一次构建产生 `f5e99c11`→`df39e8bc`→`969c69a3`→`b980f84d`→`91745aaa` 共5层），每层的 `Mapping.Builds()` 都是完整祖先链，若每层都推送事件会把同一条链重复展开记录（1+2+3+4+5=15 条边，而不是预期的 5 条）。已修复为只有 `buildOrigin != ObjectOriginTemplateBuildCache` 时才推送（详见第4节），避免 `ref_count` 把"同一次构建内部的层数"误算成"依赖的模板数"。

## 10. 已知的局限和取舍

- **按 header 整体粒度，不按 memfile/rootfs 拆分**：无法做到文件级别精确回收，只能按整个 build 前缀判断，比理论最优少回收一部分空间，当前阶段接受。
- **Redis List 不保证投递可靠性**：当前 dev 验证用的是普通 Redis List（`BRPOP`），进程崩溃可能丢事件，这是刻意接受的验证期简化，不是生产设计；生产化需要换成有重投递保证的队列（Redis Streams/消费组，或其他队列系统）。
- **事件丢失只导致"少算、延迟清理"，不会导致误删**：方向上安全，但需要定期用独立扫描工具抽样校验，主动发现并修复。
- **计数表是辅助索引，不是权威真相**：TOS 实际对象状态才是权威，计数归零后、物理删除前仍需要一次真实的 TOS 存在性确认。

## 11. 上线与回滚

这次改动有三个独立的部分，耦合很松，回滚手段也完全不同，分开说明。三部分任何一个单独回滚，都不影响另外两个，也不影响 build/pause/模板删除这些生产主链路——本方案从头到尾都是"旁路读写"，不在任何主链路的返回路径上。

### 11.1 事件发布（orchestrator，`uploads.go` 的 `publishTOSLayerEvent`）

**风险收口**：这一段本身就是 best-effort——发布失败只记日志，不返回错误，不影响 build 本身成功与否（第 2 节已说明）。真正的风险不是"发布失败"，而是"发布本身造成未预期的副作用"（比如 Redis 压力、误触发某些下游逻辑），这种场景需要能立刻关掉，而不是等一次完整的发布流程。

**已实现的回滚开关**：新增 LaunchDarkly bool flag `TOSLayerRefCountEventsFlag`（`packages/shared/pkg/featureflags/flags.go`，key `tos-layer-ref-count-events`，默认值 `true`），`publishTOSLayerEvent` 在最前面读这个 flag，关闭时直接 return，连 JSON 序列化都不做。`Uploads` 结构体本来就持有 `u.ff *featureflags.Client`（`NewUploads` 构造时传入，`ancestorStorageFallback` 已经是同样的读法），所以这不是新接入一套机制，是复用现成的开关基础设施。

回滚步骤：LaunchDarkly 控制台把 `tos-layer-ref-count-events` 关到 `false`，所有 worker 节点下一次读 flag（没有本地缓存延迟问题，`BoolFlag` 每次发布都现读）就停止发布，不需要重新编译、不需要重启 orchestrator、不丢失任何已经落地的数据。

兜底：如果 LaunchDarkly 本身不可用（`u.ff == nil`，比如某些独立命令行工具不传 ff client），`tosLayerRefCountEventsEnabled` 回落到 flag 的 fallback 值（当前是 `true`，即保持发布）——这与其他现有 flag（如 `ancestorStorageFallback`）的降级行为一致，不是本方案特有的设计。如果需要"LD 挂了就默认关闭发布"，需要单独把 fallback 改成 `false`，目前保持和现有 flag 同样的"LD 不可用时退回默认行为"约定，没有特殊化。

真正要回滚代码本身（不只是关开关），走标准的 git revert + 重新部署 orchestrator 镜像，不涉及数据修复——这一段纯粹是"发不发一条消息"，没有状态需要清理。

### 11.2 消费逻辑（`pkg/tosrefcount`，现在跑在 orchestrator 进程内）

这部分的部署形态在第 2.1 节已经改过：不再是独立进程，而是 orchestrator 在 `template-manager` 角色下起的两个后台 goroutine，跟着 orchestrator 的生命周期一起启停。回滚手段因此也变成两档,从最轻到最重:

**第一档：关掉 `TOS_REF_COUNT_POSTGRES_CONNECTION_STRING`**。这个配置项为空就是功能完全关闭(第 2.1 节),不连 Postgres、不起任何 goroutine。改这个环境变量需要重启 orchestrator 才能生效(它是启动时读取一次的配置，不是运行时轮询的 flag),所以这一档的回滚速度取决于部署滚动重启的速度，不是即时的——如果需要比这更快的止血手段，用 11.1 的 `TOSLayerRefCountEventsFlag` 直接从源头断开事件发布，两者可以同时使用。

**第二档：代码回滚**。因为逻辑现在在 `pkg/tosrefcount` 和 `factories/run.go` 里，回滚是对 orchestrator 整个二进制走标准的 git revert + 重新部署，和撤销 orchestrator 其他功能的方式完全一致，不是单独一条特殊路径。旧版本如果是指"合并进 orchestrator 之前"的版本,需要同时决定 `tos-ref-count-consumer` 独立二进制要不要重新手动拉起来顶上——按第 2.1 节的设计，它走的是同一把 `RunElected` 锁，所以手动起这个独立二进制作为临时替代是安全的，不会和任何残留的 orchestrator 消费者冲突。

**两档回滚期间的影响和之前（独立进程时期）结论一致，没有变化**：
- 停用期间新产生的边不会被记录，`ref_count` 对这期间涉及的 build 会停留在停用前的值，**不会偏高导致误删，只会偏低/缺失导致该被判定可删的候选不出现**——这和第 10 节"事件丢失只导致少算、延迟清理，不会导致误删"的结论是同一个方向，恢复不是必须立刻做的紧急操作。
- 如果预计长期停用，应该同时考虑在 orchestrator 侧关闭 11.1 的发布开关，避免 Redis List（`tos.layer.ref.events`）无限积压。
- 恢复方式：重新开启后，用第 6 节的历史回填流程（现在是 `tos-ref-count-consumer backfill`，见第 2.1 节），把 T0（本次停用开始时间）到恢复时刻这段窗口当成一次新的回填区间跑一遍，补齐断档。

**分布式锁本身的失效模式**：如果 Redis 不可用导致没有任何实例能抢到 `RunElected` 的锁，效果等同于整条链路暂停（没有实例在写），不会出现"抢不到锁就退化成多实例同时写"的情况——`RunElected` 抢不到锁时只是持续重试，不会跳过锁直接执行 `fn`。这个失效模式和"第二档回滚"是同一个方向（偏保守、不写），不会引入误删风险。

### 11.3 两张新表（`tos_build_edges`、`tos_ancestor_reference_count`）

**当前状态**：手工 `schema.sql`，不受 `packages/db/migrations` 管理（第 3 节已标注），生产化前必须先决定这两张表的归属：

- **选项 A：转正式 migration**。按 `CLAUDE.md` 的约定用 `cd packages/db && make create-migration NAME=...` 生成，回滚就是标准的 goose down migration（`DROP TABLE`），和仓库里其他表一致的运维方式，优点是回滚路径标准化、不用额外记文档。
- **选项 B：继续手工管理**。如果选这条路，必须把删表脚本提前写好、提前评审，不能依赖"回滚时再手写 DROP TABLE"——这两张表没有外键被业务表引用（纯读写方是消费者自己），删表本身是安全操作，但"安全"不等于"不用提前准备脚本"。

**不管选哪个选项**，这两张表都不是任何生产读路径的依赖项——目前真正执行物理删除的逻辑还没接上 `ref_count == 0` 这个信号（第 8 节），所以就算把两张表整个清空或删除重建，最坏情况是候选信号丢失、需要重新跑一次回填，**不会导致任何已存在数据被误删**，因为"删 TOS 对象"这个动作本身还在 `tos-cleanup-plan.md` 的流程里，没有被这两张表自动触发。

**回滚执行**：
```sql
-- 如果选项 A（已转 migration）：走 down migration，不手写
-- 如果选项 B（手工管理）：
DROP TABLE IF EXISTS tos_build_edges;
DROP TABLE IF EXISTS tos_ancestor_reference_count;
```
执行前确认没有其他脚本/服务正在依赖这两张表做出"是否删除"的决策（目前没有，第 8 节已说明候选信号还没接自动化流程）。

### 11.4 上线顺序建议

1. 先上线 11.1 的 flag（代码本身不改变现有行为，`tos-layer-ref-count-events` 默认 `true` 等价于现状），确认 flag 读取路径正常后再推送 7f7c2e5fc。
2. 确认 11.3 的表归属决策（A 还是 B，目前表结构还在复核中，暂不动），如果选 A，先走完 migration 流程再开启消费逻辑。
3. 在目标环境的 orchestrator 配置里设置 `TOS_REF_COUNT_POSTGRES_CONNECTION_STRING`，滚动重启 template-manager 角色节点，让 `pkg/tosrefcount` 的两个 goroutine 开始跑（见第 2.1 节）。观察一段时间（建议参考双轨校验期的思路，第 11 节待办里仍需要补充具体对比脚本），确认分布式锁按预期只有一个实例在写（可以查 `tos_ancestor_reference_count.updated_at` 是否连续、没有断档，或者直接查 Redis 里 `lock:tos-ref-count-consumer-leader` 这个 key 的持有者）。
4. 任何一步出现非预期行为，优先用 11.1 的 flag 止血（最快、零数据风险）；如果问题在消费逻辑本身，清空 `TOS_REF_COUNT_POSTGRES_CONNECTION_STRING` 走第 11.2 节"第一档回滚"，不需要动发布开关也能停。

## 12. 待确认 / 下一步

- [x] **第 7 节的分叉决策**：已确定路线 B（自引用边 + 按 env_id 批量清除，只查表2，`ref_count == 0` 即可信）。代码已改（`handleEvent` 去掉自引用过滤）并用真实构建验证通过（见第9节）。
- [x] 移除 `layer_executor.go` 里为排查 `env_id` 为空问题加的临时调试日志（已确认问题修复，日志已删除）。
- [x] **中间缓存层去重**：已在 `uploads.go` 的 `publishTOSLayerEvent` 加判断（排除 `ObjectOriginTemplateBuildCache`），三个调用点（`build_upload_v3.go` 两处、`build_upload_v4.go` 一处）已改为传入 `buildOrigin`。已 commit（`7f7c2e5fc`），**待推送到远程并在 dev-server 重新编译部署、清表重测**，验证一次多层构建只产生一组边（不再是按层数展开的重复记录）。
- [x] **事件发布的回滚开关**：已加 `TOSLayerRefCountEventsFlag`（`tos-layer-ref-count-events`，默认 `true`），`publishTOSLayerEvent` 最前面读这个 flag，关闭即停止发布，不需要重新部署（见第 11.1 节）。
- [x] **消费逻辑不再需要手动启动**：已从独立二进制 `tos-ref-count-consumer` 抽成 `packages/orchestrator/pkg/tosrefcount` 包，由 `factories/run.go` 的 `setupTOSRefCount` 在 `template-manager` 角色下自动启动，开关是 `TOS_REF_COUNT_POSTGRES_CONNECTION_STRING` 是否配置；加了 `pkg/tosrefcount.RunElected` 分布式锁应对 `template-manager` 多实例池场景（见第 2.1 节）。独立二进制保留，改成薄封装，承担本地验证和历史回填（`backfill` 子命令）两个用途。
- [ ] 把第 7a 节的 live roots 查询写成代码（当前方案不强依赖，留作备用/抽样核对用，暂不需要接入自动化流程）。
- [ ] 确认 `env_last_spawned_at` 持续为空的原因（数据库字段本身未写入，还是查询时机问题，尚未最终排查完成）。
- [ ] **表1/表2要不要转正式 `packages/db/migrations` 管理**（见第 11.3 节两个选项）：表结构还在复核中，暂不动，确认完再决定。
- [ ] 生产化的事件队列选型（替换当前验证用的 Redis List）。
- [ ] 历史回填的实际执行时间点和 T0；现在有了 `tos-ref-count-consumer backfill -input result.jsonl` 子命令，执行方式已经明确，待确认的只是时间点和范围。
- [ ] 双轨校验期的具体对比脚本（复用 `tos-layer-scan` + diff 逻辑）。
- [ ] `RunElected` 分布式锁的监控/告警：目前只有日志（获取/丢失锁时各打一行 info/warn），生产化前需要确认要不要对"长时间没有任何实例持有锁"这种情况加监控，避免整条链路静默停摆却没人发现。
</content>
