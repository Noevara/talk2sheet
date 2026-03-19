# Talk2Sheet

Talk2Sheet 是一个开源的前后端一体化框架，用于实现“用自然语言和 Excel / CSV 对话做数据分析”。

它的核心目标是：用户围绕表格数据提出自然语言问题，系统在 workbook 内定位合适的目标 sheet，将问题翻译成可执行的分析计划，用 pandas 执行分析，再把答案和执行链路一起返回给前端。

## v0.1.0 范围

当前版本聚焦：

- workbook 内一次针对一个 sheet 的分析
- 自然语言表格分析
- 带澄清和追问上下文的多轮对话
- 可见的执行口径、路由信息、结果表格和图表
- 中英日三语文档与界面

当前已支持：

- 文件上传、sheet 列表、表格预览
- workbook 内单个目标 sheet 的自动路由
- 总行数、总量、均值、去重计数
- Top N / 排名
- 明细行返回
- 趋势分析和基础图表
- 轻量时间序列预测
- `auto / text / chart` 模式切换
- 用户可见的分析链路、sheet 路由摘要和结构化回答

当前暂不支持：

- 跨 sheet join 或跨工作表联合分析
- 高级统计
- 因果推断
- 生产级对象存储与持久化会话后端

## 仓库结构

```text
apps/
  api/   FastAPI 后端
  web/   Vue 3 前端
docs/    英中日架构文档
packages/contracts/  生成的 OpenAPI 契约产物
```

## 文档入口

- 英文：[README.md](./README.md)
- 中文：当前文件
- 日文：[README.ja.md](./README.ja.md)
- 架构文档：[docs/architecture.zh-CN.md](./docs/architecture.zh-CN.md)
- 版本说明：[CHANGELOG.md](./CHANGELOG.md)

## 使用方式

1. 上传 Excel 或 CSV 文件
2. 预览 workbook 中的 sheet，并在需要时选择目标 sheet
3. 输入自然语言问题
4. 当问题不明确时，让系统先做 sheet 或意图澄清
5. 结合路由信息、执行范围、表格和图表查看分析结果

## 本地开发

### 后端

```bash
cd apps/api
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
python3.11 -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### 前端

```bash
cd apps/web
npm install
npm run dev -- --host 127.0.0.1 --port 5173
```

开发环境下，前端默认直接请求 `http://127.0.0.1:8000/api`。

## 安全说明

请基于 `.env.example` 在本地创建 `.env`，并在其中填写模型提供方相关配置。

不要把 `.env`、用户上传的表格、运行期元数据以及任何 API Key / 密码提交到公开仓库。

## Docker 启动

```bash
docker compose up --build
```

启动后访问：

- Web: `http://localhost:8080`
- API: `http://localhost:8000`

说明：当前 Docker 启动方式默认不会注入 LLM API Key。如果你希望容器内启用基于大模型的规划或回答能力，需要显式补充本地环境配置。

如果所在网络环境访问 Docker Hub 不稳定，可以在 `.env` 中覆盖基础镜像：

```bash
TALK2SHEET_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.11-slim
TALK2SHEET_NODE_IMAGE=docker.m.daocloud.io/node:20-alpine
TALK2SHEET_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:1.27-alpine
```

## 实现说明

当前后端主链路包括：

1. workbook context 构建
2. 单 sheet 路由
3. capability guard
4. planner 与意图理解
5. validation 与 repair
6. exact 或常规执行
7. 结构化回答生成
8. 通过 SSE 流式返回 `meta`、`pipeline`、`answer` 和结束标记

当前前端已覆盖：

- workbook 相关状态管理
- conversation 相关状态管理
- clarification 交互闭环
- 分类示例问题
- 执行链路可见性
- sheet routing 可见性

## 契约与校验

契约产物由 FastAPI 运行时 schema 生成。

- 导出 OpenAPI: `python apps/api/scripts/export_openapi.py`
- 校验契约产物: `python apps/api/scripts/check_contract_artifacts.py`
- 生成前端 API 类型: `cd apps/web && npm run generate:types`

校验命令：

- API: `pytest -q apps/api`
- Web: `cd apps/web && npm run ci`
- 全量: `make ci-check`
