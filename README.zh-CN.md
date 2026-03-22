# Talk2Sheet

Talk2Sheet 是一个开源的前后端一体化框架，用于实现“用自然语言和 Excel / CSV 对话做数据分析”。

它的核心目标是：用户围绕表格数据提出自然语言问题，系统在 workbook 内定位合适的目标 sheet，将问题翻译成可执行的分析计划，用 pandas 执行分析，再把答案和执行链路一起返回给前端。

当前稳定版本：`v0.3.3`。

## 当前范围

当前版本聚焦：

- workbook 内一次针对一个 sheet 的分析
- workbook 级多 sheet 问题识别与拆解引导
- 自然语言表格分析
- 带澄清和追问上下文的多轮对话
- 跨 sheet 顺序分析的任务步骤化流程
- 可见的执行口径、路由信息、结果表格和图表
- 回答一键复制、结果表 CSV 导出、图表 PNG 导出、刷新后的本地会话恢复
- 中英日三语文档与界面

当前已支持：

- 文件上传、sheet 列表、表格预览
- workbook 内单个目标 sheet 的自动路由
- 同一 workbook 内顺序多 sheet 分析（先 A 后 B，分轮执行）
- 受控 Join Beta 跨 sheet 能力（仅两表、单键、`inner`/`left`、聚合类问题）
- 任务步骤卡片（待执行 / 进行中 / 已完成 / 失败）
- “继续下一步”一键切到目标 sheet，并沿用当前分析口径
- analysis anchor 口径锚点复用（降低追问时指标/维度/时间口径漂移）
- 上一步 vs 当前步骤对照卡（A/B 对照，不做 join）
- 多 sheet 问题澄清与拆解提示
- 总行数、总量、均值、去重计数
- 周期对比：环比 / 同比、差值、倍数占比
- Top N / 排名
- 条件筛选 + 分组聚合 + Top N 组合问题
- 明细行返回
- 明细 + 总结的结构化回答卡（结论 / 依据 / 风险提示）
- 支持日 / 周 / 月粒度的趋势分析
- 图表类型推荐、图表元信息展示，以及无法出图时的文本降级提示
- 轻量时间序列预测
- `auto / text / chart` 模式切换
- 用户可见的分析链路、sheet 路由摘要和结构化回答
- task_step 可观测事件（`task_step_started` / `task_step_completed` / `task_step_failed`）与 request_id 关联
- 路由原因与解释文案可见（`reason` / `explanation` / `explanation_code`）
- 字段/工作表澄清卡片（区分类型），选择后自动用自然确认语继续原问题分析
- 结果卡“继续问”建议，点击后自动回填输入框并可编辑发送
- intent 回归语料与离线评测（已接入 CI 分层）

当前暂不支持：

- 单轮内任意跨 sheet join / union
- 多键 join、多跳 join、三表及以上 join
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
- 最新发布说明：[docs/releases/v0.3.3.md](./docs/releases/v0.3.3.md)

## 使用方式

1. 上传 Excel 或 CSV 文件
2. 预览 workbook 中的 sheet，并在需要时选择目标 sheet
3. 输入自然语言问题
4. 如果问题涉及多个 sheet，让系统先澄清并从一个 sheet 开始
5. 结合路由信息、执行范围、表格和图表查看分析结果
6. 需要时在后续轮次继续切换到另一个 sheet

## 本地开发

### 快速开始

1. 复制 `.env.example` 为 `.env`
2. 如果希望启用基于大模型的规划与回答，请填写 `TALK2SHEET_LLM_API_KEY`
3. 启动后端
4. 启动前端
5. 打开 `http://127.0.0.1:5173`

如果 `TALK2SHEET_LLM_API_KEY` 留空，系统仍可启动，但部分规划或回答链路会按当前 provider 配置退化到非 LLM 路径。

### 后端

```bash
cp .env.example .env
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

后端优先读取 `TALK2SHEET_LLM_API_KEY`，如果它为空，也兼容读取 `OPENAI_API_KEY` 作为兜底。

不要把 `.env`、用户上传的表格、运行期元数据以及任何 API Key / 密码提交到公开仓库。

## Docker 启动

```bash
docker compose up --build
```

启动后访问：

- Web: `http://localhost:8080`
- API: `http://localhost:8000`

如果希望容器内启用基于大模型的规划或回答能力：

```bash
cp .env.example .env
# 编辑 .env，填写 TALK2SHEET_LLM_API_KEY=...
docker compose up --build
```

现在 `docker-compose.yml` 已经会把相关 provider 与 LLM 配置透传到 `api` 容器中。Key 仍然只保存在你的本地 `.env` 文件里，不应该提交到仓库。

如果所在网络环境访问 Docker Hub 不稳定，可以在 `.env` 中覆盖基础镜像：

```bash
TALK2SHEET_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.11-slim
TALK2SHEET_NODE_IMAGE=docker.m.daocloud.io/node:20-alpine
TALK2SHEET_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:1.27-alpine
```

## 实现说明

当前后端主链路包括：

1. workbook context 构建
2. workbook 路由（单轮单 sheet 执行 + 多 sheet 澄清/拆解引导）
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
- 路由解释可见性（`reason`、`explanation`、`explanation_code`）

## 契约与校验

契约产物由 FastAPI 运行时 schema 生成。

- 导出 OpenAPI: `python apps/api/scripts/export_openapi.py`
- 校验契约产物: `python apps/api/scripts/check_contract_artifacts.py`
- 生成前端 API 类型: `cd apps/web && npm run generate:types`

校验命令：

- API: `pytest -q apps/api`
- Intent 回归: `python apps/api/scripts/eval_intent_cases.py`
- Web: `cd apps/web && npm run ci`
- 全量: `make ci-check`

## 常见问题

- 上传后立刻失败：
  请先确认文件格式是 `.xlsx`、`.xls` 或 `.csv`，如果文件特别大，先尝试一个更小的工作簿。
- 预览或对话报错里带有 `request_id`：
  可以保留这个 `request_id`，去后端日志中对应定位。
- Docker 已启动但没有大模型回答：
  请确认 `.env` 里已经填写 `TALK2SHEET_LLM_API_KEY`，并在修改后重新执行 `docker compose up --build`。
- 刷新后恢复的会话又消失了：
  说明上一次本地记录对应的上传文件已经不可用，需要重新上传。
