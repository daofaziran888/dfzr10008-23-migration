# 数据产品与API工厂

面向数据产品经理与开发者的数据产品快速构建平台。覆盖数据产品定义、API接口生成、数据模型封装、产品版本管理、调用配额控制、文档自动生成、产品市场分发与使用效果追踪的完整数据产品链路。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   ├── app/persistence/      管线运行状态持久化与迁移（sqlite，重启不丢）
│   ├── app/store.py          内存数据仓库与示例数据
│   └── tests/                迁移验收测试（标准库 unittest）
├── .gitignore
└── docker-compose.yml
```

## 产品管线运行状态迁移

管线节点、依赖边、失败断点与输出格式快照全部落在 sqlite，重启后完整恢复，不再只存在内存里。

### 迁移样例

| 节点编号 | 运行结果 | 重试策略 | 失败码 | 输出格式 |
| --- | --- | --- | --- | --- |
| PL-0001 | 已完成 | 0 次 | — | parquet |
| PL-0002 | 失败 | 3 次 | ORA-LOAD-17 | parquet |
| PL-0003 | 等待上游 | 2 次 | — | parquet |

依赖关系：`PL-0001 → PL-0002 → PL-0003`。

迁移语义：

- 按节点编号顺序回填，每个节点落库后写断点；中断重启后从最后一个完整节点继续。
- 节点编号与依赖边（上游, 下游）均有唯一约束：新节点与回填并发时先完成落库者为准，重跑不产生第二条依赖边。
- PL-0003 在前置 PL-0002 完成前保持「等待上游」，迁移器与启动接口都不能把它提前改成运行中。
- 回填日志按节点编号记录开始、完成和中断位置（数据库 + `pipeline-migration.log` 双份留痕）。
- 既有 PIPE 示例节点幂等回填重试策略与输出格式快照，旧 CRUD 接口与页面保持不变。

### 命令行（在 backend 目录下）

```bash
python -m app.persistence.cli init       # 建库建表 + 回填既有示例节点
python -m app.persistence.cli migrate    # 执行/断点续跑迁移（幂等）
python -m app.persistence.cli status     # 查看节点、依赖边、断点与日志
python -m app.persistence.cli verify     # 按验收口径核对，不符则非零退出
python -m app.persistence.cli reset      # 清空持久层，重新演练
```

或用 Makefile：`make migrate`、`make migrate-status`、`make migrate-verify`、`make test`。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PIPELINE_DB_PATH` | `backend/data/pipeline_state.sqlite3` | sqlite 数据文件 |
| `PIPELINE_MIGRATION_LOG_DIR` | `backend/data/logs` | 回填日志目录 |
| `PIPELINE_AUTO_MIGRATE` | `0`（run.sh / 容器入口默认置 `1`） | 启动时自动断点续跑 |

四类环境的覆盖方式：本地开发走 `run.sh`；构建产物在镜像 `entrypoint.sh` 里 init + migrate；
上线部署由容器启动续跑；docker-compose 用命名卷 `pipeline-state` 持久化 `/srv/data`。

### 运行时接口

- `GET /api/pipeline/runtime/nodes`：迁移恢复后的节点运行结果
- `GET /api/pipeline/runtime/edges`：节点依赖边
- `GET /api/pipeline/runtime/snapshots?node_code=PL-0001`：输出格式快照
- `GET /api/pipeline/runtime/migration`：迁移断点与按节点编号的回填日志
- `POST /api/pipeline/runtime/resume`：手动触发断点续跑
- `POST /api/pipeline/runtime/nodes/{node_code}/running`：置运行中（等待态保护）
- `GET /api/datapack/snapshot/PL-0001`：数据模型封装按来源编号读取原输出格式

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据产品定义 | `product` | 数据产品 | 产品编号、产品名称、产品类型 |
| API接口生成 | `apigen` | 接口配置 | 接口编号、接口名称、请求方法 |
| 数据模型封装 | `datapack` | 模型封装 | 封装编号、封装名称、原始数据源 |
| 产品版本管理 | `version` | 产品版本 | 版本编号、产品名称、版本号 |
| 调用配额控制 | `quota` | 配额配置 | 配置编号、调用方名称、每秒上限 |
| 文档自动生成 | `docgen` | 文档模板 | 模板编号、文档标题、目标接口 |
| 产品市场分发 | `market` | 市场商品 | 商品编号、商品名称、所属产品 |
| 使用效果追踪 | `usage` | 使用记录 | 记录编号、产品名称、调用方名称 |
| 产品沙箱测试 | `sandbox` | 沙箱实例 | 实例编号、测试产品、测试参数 |
| 数据管线编排 | `pipeline` | 管线节点 | 节点编号、节点名称、上游节点 |
| 数据筛选引擎 | `filter` | 筛选模板 | 模板编号、模板名称、目标数据源 |
| 场景化配置模板 | `scene` | 场景模板 | 模板编号、模板名称、适用场景 |
| 实时数据推送 | `realtime` | 推送通道 | 通道编号、通道名称、数据源名称 |
| 跨源数据关联 | `crossref` | 关联规则 | 规则编号、主数据源、关联数据源 |
| 数据质量反馈 | `feedback` | 反馈记录 | 反馈编号、数据产品、反馈类型 |
| 数据产品认证 | `badge` | 认证记录 | 认证编号、产品名称、认证类型 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
