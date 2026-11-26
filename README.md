# 陶驿窑务·预检记录分片

加权 Rendezvous Hash 重算候选归属，结合每轮搬迁字节预算生成可执行计划。面向演示数据和本地复现，场景、操作剧本和业务边界见 [BUSINESS.md](BUSINESS.md)。

## 业务模块

- 窑口节点
- 预检批次
- 重分片预算
- 搬迁清单

## 档案迁库接口

指定档案按盘点编号（`payload.snapshot_id`）从原库 `old` 迁到新库 `new`，主键与试算/报告编号保持不变。迁库期间试算、复核照常写入；迁库单持久化、可续跑，按单号幂等，行级 upsert 保证重试不产生重复记录。目标库逐行核对（行数与规范化内容）一致后才切换该档案的归属和全局接单库；切换前已 `claim` 的试算即使晚交报告，也会按档案归属写入新库（必要时先回补最新的 `attempt_token`）。按档案查询试算和报告只访问其当前归属的同一座库。

| 动作 | 接口 |
| --- | --- |
| 发起迁库（同步跑到完成） | `POST /migrations`（同义：`/migrate`、`/cases/migrate`、`/archives/{id}/migrate`、`/snapshots/{snapshot_id}/migrate`） |
| 迁库单列表 / 详情 | `GET /migrations`、`GET /migrations/{单号或id}` |
| 续跑 / 重试 / 核对 / 切换 | `POST /migrations/{id}/run`（`resume`/`retry`）、`/sync`、`/verify`、`/switch` |
| 查询当前由哪座库接单 | `GET /database`（同义：`/active-database`、`/databases/active`、`/routing`、`/cases/{id}/database`，可用 `?case_id=`/`?snapshot_id=` 查单档案归属） |
| 按档案查试算 | `GET /cases/{id}/runs`（也支持 `GET /runs?case_id=`、`/archives/{id}/runs`） |
| 按档案查报告/复核 | `GET /cases/{id}/evidence`（也支持 `GET /evidence?case_id=`、`/archives/{id}/reports`） |

发起 body 兼容 `{"snapshot_id":"..."}`、`{"snapshot_ids":[...]}`、`{"case_ids":[...]}`、裸数组、中文键 `盘点编号`，并可用 `order_no`/`idempotency_key` 字段或 `Idempotency-Key` 请求头指定同一单。响应同时给出 `active_database`、`current_database`、`writer`、`active_db` 等同义字段。

本地/测试默认使用两个独立的 SQLite 内存库，无需 MySQL/Redis；也可用 `NEW_DB_URL` 或 `NEW_DB_NAME` 配置目标库。compose 会自动创建 MySQL 目标库 `workflow_new`。

## 本地运行

```sh
cp -n .env.example .env
docker compose up -d --build --wait
```

- 工作台 `http://127.0.0.1:18301`
- FastAPI 文档 `http://127.0.0.1:19301/docs`
- MySQL 8.0 端口 `127.0.0.1:17801`，账号 `app`，演示库 `workflow`
- `docker compose exec -T backend python -m scripts.seed_demo` 手动重跑演示数据
- `docker compose logs -f backend worker` 查看运行情况
- `docker compose down` 停容器并保留演示卷

### 演示账号

统一密码 `Demo-Walkthrough-2026`。

| 账号 | 角色 | 能执行的操作 |
|---|---|---|
| `operator` | 经办员 | 读取档案、创建实验输入、提交执行、导出结果 |
| `reviewer` | 复核员 | 读取、导出和签署结果 |
| `viewer` | 观察员 | 读取、导出已有结果 |

`SEED_DEMO=true` 启动时生成账号与一个业务样例。工作台新建档案会预填样例。可编辑表单中的数值、文本和结构化 JSON 后提交。业务说明中的原始文件是上游示例，演示数据通过表单录入。角色枚举位于后端 `src/constants/workflow.py` 与前端 `src/constants/workflow.js`。执行状态枚举也位于这两个文件，使用于队列、列表和执行详情。证据校验码是报告 JSON 按 UTF-8、键排序且省略多余空格序列化后的 SHA-256。

## 工程结构

| 服务 | 实现 |
|---|---|
| frontend | Nginx 与原生 HTML、CSS、JavaScript |
| backend | Python 3.11、FastAPI、SQLAlchemy |
| worker | Python 持久队列执行与 Redis 唤醒信号 |
| db | MySQL 8.0，持久卷保存档案、执行、证据及审计 |
| redis | Redis 7，追加日志卷保存队列唤醒信号 |

编辑 `backend/src/services/domain.py` 可修改试算方法与业务表单说明。API 路由、数据定义和页面位于 `backend/src/routes/`、`backend/src/models/` 与 `frontend/src/pages/`。开发时更新源文件后重新构建对应服务。

`.env.example` 提供 `FRONTEND_PORT`、`BACKEND_PORT`、`DB_PORT`、`DB_PASSWORD`、`AUTH_KEY` 、`SEED_DEMO` 与 `NETWORK_SUBNET`。修改 `NETWORK_SUBNET` 可避开本机现有容器网段。所有发布的端口绑定本机回环地址。接入独立部署前需配置自己的凭据并按业务范围做独立验收。

[MIT License](LICENSE)
