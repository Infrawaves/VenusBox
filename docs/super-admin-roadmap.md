# 超级管理员 / 平台角色 路线图

把 E2B dashboard 产品化,引入平台级角色,让平台管理员能跨 team 纵览并运营全平台资源。

## 背景与目标

- 官方 dashboard 的功能/角色划分不够细,我们在其基础上做更细的平台管理能力,后续可能付费。
- 引入三级角色,与 team 成员关系正交:
  - `member`:自己所属 team 的资源使用者(现有能力,默认身份,无需标记)。
  - `platform_admin`:跨所有 team 的日常运营(全局只读查看、用户/团队信息、配额查看)。
  - `super_admin`:高危 + 平台根权限(批量 kill/cancel、删用户、封禁、高级网络策略、管理员管理)。
- 角色模型选定 **模型 A**(member / platform_admin / super_admin),team 内不分层(选择 1)。
- 认证架构选定 **纯 ORY**:真人登录,角色存 DB 表 `platform_admins`,不走 service JWT。

## 架构决策记录

- [ ] 角色模型 = member / platform_admin / super_admin(模型 A)
- [ ] team 内不分层(成员可自助管理本 team,不引入 team_admin)
- [ ] 平台角色存储 = DB 表 `platform_admins`(设计 A),不用 JWT claim / ORY metadata
- [ ] 认证 = 纯 ORY,去掉 service JWT 死路径(追求干净)
- [ ] 全局只读查看归 `platform_admin`(super_admin 因 IsAtLeast 自动可访问)
- [ ] 角色判定与来源解耦:中间件只从 context 读角色,来源(DB 查表)可插拔
- [ ] 共享地基抽成独立层(⑨),dashboard-api / infra-api 两服务共用一份
- [ ] 归因第一版只到 team 维度(user / api_key 归因暂缓)
- [ ] 代码规范:注释不超过一行;commit/PR 不带 Claude 署名

## 阶段 0:权限地基(后端)

- [ ] ① 平台角色类型 + context(`PlatformRole` / `IsAtLeast` / Set/Get,纯地基无 JWT)
- [ ] ② 鉴权中间件 `RequirePlatformRole` / `EnforcePlatformRole`
- [ ] ⑨ 共享地基:`platform_admins` 表 + migration
- [ ] ⑨ sqlc 查询 `GetPlatformRoleByUserID` / `UpsertPlatformAdmin`
- [ ] ⑨ `auth.SetPlatformRole` 公开 setter
- [ ] ⑨ `auth.ResolvePlatformRole` 泛型中间件(db-agnostic,接受 Lookup 契约)

## 阶段 1:全局查看接口(后端)

- [ ] ③ `GET /super-admin/templates`(dashboard-api / Postgres,team 归因,keyset 分页)
- [ ] ⑤ `GET /super-admin/builds`(dashboard-api / Postgres,team 归因,状态过滤 + 分页)
- [ ] ⑥ `GET /super-admin/teams`(dashboard-api / Postgres,去掉 user 过滤 + 配额)
- [ ] ④ `GET /super-admin/sandboxes`(infra-api / Redis,逐 team 聚合,运行中,不分页)
- [ ] ⑦ dashboard-api 接 ORY 鉴权(三端点 security 改 AuthProviderBearer + 挂 ResolvePlatformRole + env 种子)
- [ ] ⑧ infra-api 接 ORY 鉴权(sandbox 端点改 ORY + lookup/seed 方法 + 挂中间件)

## 阶段 2:后端收尾

- [ ] 推送剩余分支:⑨ 首推;③⑤⑥④⑦ 因 rebase 到⑨ 需 force-with-lease 强推(⑧ 已推)
- [ ] 更新各 PR 标题/描述(① 从 "JWT claim" 改为 "platform role foundation")
- [ ] 核对各堆叠 PR 的 base:⑨→② / ③→⑨ / ⑤→③ / ⑥→⑤ / ⑦→⑥ / ④→⑨ / ⑧→④
- [ ] 验证三条全局查询 SQL(EXPLAIN,在能连内网库的机器上跑)
- [ ] 修复 CI `Detect changed paths` 失败(`Repository not found`,仓库配置层,非代码)
- [ ] DB 集成测试通过(靠 CI,本地无 Docker/Postgres)

## 阶段 3:前端(e2b-infra-dashboard,独立仓库)

- [ ] 调研前端 ORY 认证现状(现只有 team key 表单,super-admin 接口要 ORY bearer)
- [ ] 决定:复用现有 ORY 还是新建超管登录流程
- [ ] 同步后端新 spec 到前端两份副本(openapi.dashboard-api.yaml / openapi.infra.yaml)
- [ ] 跑 generate 重新生成 TS 类型
- [ ] 超管登录态 + 路由守卫(前端菜单显隐仅体验,权限以后端为准)
- [ ] 全局视图页面:templates
- [ ] 全局视图页面:sandboxes
- [ ] 全局视图页面:builds
- [ ] 全局视图页面:teams
- [ ] 每条资源的 team 归因展示

## 阶段 4:平台运营能力(后端)

- [ ] 高危操作收归 super_admin:批量 kill 某 team sandbox
- [ ] 高危操作收归 super_admin:取消某 team 所有 build
- [ ] 高危操作收归 super_admin:删除用户
- [ ] 高危操作收归 super_admin:封禁 / 解封 team
- [ ] 配额查看 / 调整(platform_admin)
- [ ] 用量统计(团队并发指标 / ClickHouse 分析)
- [ ] 审计日志(actor / action / target / team / time)

## 阶段 5:产品化 / 商业化

- [ ] entitlement 模型 + 计费 / 套餐分层
- [ ] 是否引入 team_admin(当前模型 A 没有)
- [ ] 高级网络策略(BYOP / HTTP 变换)归属划分
- [ ] 超管管理超管(增删 platform_admins 的后台界面 + 接口,目前只有 env 种子)
- [ ] user / api_key 级归因(需从认证层改起,当前只到 team 维度)

## 未决项(需拍板)

- [ ] 归因粒度:user / api_key 归因是永久不做,还是列入阶段 4?
- [ ] 前端 ORY 登录:复用现有 ORY 还是新建?(决定阶段 3 工作量)
- [ ] 谁管 platform_admins:是否需要产品内增删管理员的界面,还是长期只靠 env 种子?

## 分支拓扑(堆叠)

```
main
 └─ ① platform-role-jwt-claim(纯地基)
     └─ ② require-platform-role-middleware
         └─ ⑨ platform-role-db-source(共享地基)
             ├─ ③ super-admin-templates
             │   └─ ⑤ super-admin-builds
             │       └─ ⑥ super-admin-teams
             │           └─ ⑦ platform-role-from-ory(dashboard-api 接线)
             └─ ④ super-admin-sandboxes
                 └─ ⑧ sandbox-platform-role-ory(infra-api 接线)
```

合并顺序从下往上:① → ② → ⑨ → (③ → ⑤ → ⑥ → ⑦) / (④ → ⑧)。

## 已知限制

- 全局 sandbox(④)是逐 team 聚合(枚举所有 team 再 TeamItems 合并),team 极多时有 N 次 Redis 调用;第一版可接受。
- 全局 sandbox 只含运行中、不分页、不含暂停快照。
- 全局 build / template / team 列表第一版不支持按 id / 名称搜索(仅分页 + 状态过滤)。
- DB migration 与真实 SQL 未在本地验证(无 Docker/Postgres),靠 CI / 内网库。
