# Talk2Sheet 架构说明

## 1. 定位

Talk2Sheet 是一个面向 Excel / CSV 的开源表格对话分析框架。当前架构聚焦在：

- workbook 路由 + 单轮单 sheet 执行
- 多 sheet 问题识别、澄清与顺序拆解（先 A 后 B）
- 自然语言到结构化分析计划的转换
- 基于 pandas 的可执行分析链路
- 支持澄清、追问、模式切换和上下文继承的多轮对话
- 将执行链路、口径、结果和回答以结构化方式返回前端

当前不把自己定义为“任意 BI / 任意统计平台”。系统边界明确如下：

- 已支持：workbook 路由、单轮单 sheet 执行、顺序多 sheet 追问、明细 / 汇总 / 排名 / 趋势 /基础图表 / 轻量预测
- 暂不支持：单轮跨 sheet join、自由跨 sheet 联合执行、多表关系建模、高级统计、因果推断、复杂预测工作流

## 2. 当前已实现能力

### 2.1 产品能力

- 文件上传、sheet 列表和表格预览
- 文件存储已抽象为 `services/storage/`
- 面向 workbook 的单 sheet 路由
  - 问题显式提及 sheet
  - 根据列名 / sheet 名自动路由
  - 多 sheet 问题澄清与拆解提示
  - follow-up 顺序切换 sheet
  - 多候选时触发 sheet 澄清
  - 手动切换 sheet 后通过 `sheet_override` 强制生效
  - 当前已支持在同一 workbook 内按轮次定位目标 sheet 并完成分析
  - 当前仍不支持跨 sheet join 联合执行
- 自然语言分析
  - 总行数、总量、均值、去重计数
  - Top N / ranking
  - detail rows
  - 趋势分析和基础图表
  - 轻量时间序列预测
  - 具备语义意图理解，而不再只依赖松散的字符串意图
- 多轮对话
  - 记忆最近轮次的 pipeline 摘要
  - 继承上轮语义和命中 sheet
  - clarification 闭环
  - 顺序多 sheet 的 task step 记忆
  - 一键继续下一步（followup_action）路由
  - analysis anchor 口径锚点复用
- 前端执行可见性
  - 执行链路展示
  - sheet 路由摘要
  - 上一步 vs 当前步骤对照卡（非 join）
  - 结果表格 / 明细表 / 简单图表
  - 结构化答案分段
  - `mode = auto / text / chart` 切换
  - OpenAPI 与前端类型统一生成
  - 基础可观测性
- 前端结构已完成初步拆分，并按 feature 组织
- CI 已提供分层校验

## 3. 端到端链路

一次完整请求的主路径如下：

1. 前端上传文件到 `/api/files/upload`
2. 后端通过 `services/storage/` 落盘并返回 `file_id`
3. 前端请求 sheet 预览，建立 workbook / active sheet 上下文
4. 用户输入问题，前端提交：
   - `chat_text`
   - `mode`
   - `sheet_index`
   - `sheet_override`
   - `conversation_id`
   - `clarification_resolution`
5. 后端在 `stream_spreadsheet_chat()` 中：
   - 建立 / 续用 conversation session
   - 读取 workbook context
   - 执行 sheet routing
   - 加载当前 sheet 的 sampled dataframe
   - 进入分析调度层
6. 分析调度层内部：
   - capability guard
   - planner
   - semantic intent understanding
   - validation / repair
   - exact execution 或常规执行
   - answer generation
7. 后端通过 SSE 分段返回：
   - `meta`
   - `pipeline`
   - `answer`
   - `EOS`
8. 前端将流式 payload 组装成消息卡片，展示执行口径、路由信息、图表、明细和结构化答案

## 4. 后端架构

后端主代码位于 `apps/api/app/`，当前采用“按职责分包”的组织方式，便于在保持可运行的前提下持续演进。

### 4.1 API 与基础设施

- `apps/api/app/main.py`
  FastAPI 入口、CORS、中间件、统一异常处理
- `apps/api/app/schemas.py`
  HTTP 请求 / 响应契约
- `apps/api/app/observability.py`
  `X-Request-ID`、结构化日志辅助
  task step 事件日志（`task_step_started` / `task_step_completed` / `task_step_failed`）
- `apps/api/app/api/routes/files.py`
  上传、sheet 列表、预览接口
- `apps/api/app/api/routes/spreadsheet.py`
  SSE 对话接口

### 4.2 存储层

- `apps/api/app/services/storage/`
  抽象文件存储接口
- `local_file_store.py`
  当前默认实现，按块写入上传文件并记录元数据
- `object_storage_file_store.py`
  预留对象存储适配点

这一层的主要价值是把“文件怎么保存”与“表格怎么分析”解耦，方便后续接入对象存储或其他持久化方案。

### 4.3 表格管线层

- `apps/api/app/services/spreadsheet/pipeline/`
  文件加载、sheet 元数据、header detection、预览、workbook context 构建
- `workbook_context.py`
  为 sheet routing 提供 workbook 级上下文
- `sheet_metadata.py`
  读取 sheet 描述信息

这一层负责把“文件”转成“可分析的 dataframe 与 workbook 描述”。

### 4.4 路由层

- `apps/api/app/services/spreadsheet/routing/sheet_router.py`
  workbook 内单 sheet 路由决策
- `router_types.py`
  路由结果结构

当前路由优先级大致为：

1. 单 sheet 工作簿直通
2. 澄清结果命中
3. 手动切换 sheet 的 `sheet_override`
4. follow-up 切 sheet 命中（显式 / 上一个 / 另一个）
5. 多 sheet 澄清与拆解分流
6. 问题中显式指定 sheet
7. follow-up 继承上轮 sheet
8. 按 sheet 名 / 列名 / hints 自动评分
9. 候选冲突时发起澄清
10. 回退到请求 sheet

### 4.5 规划与语义层

- `apps/api/app/services/spreadsheet/planning/`
  planner、heuristic / llm provider、追问规划、guardrails
- `intent_models.py`
  `AnalysisIntent` 结构
- `intent_understanding.py`
  语义意图识别
- `intent_accessors.py`
  对 planner / answer / memory 统一读取语义意图

这一层的关键改进是：系统不再只靠一个松散的 `intent` 字符串，而是保留结构化语义，如：

- `target_metric`
- `target_dimension`
- `comparison_type`
- `time_scope`
- `answer_expectation`
- `clarification`

### 4.6 分析、质量与执行层

- `apps/api/app/services/spreadsheet/analysis/`
  总调度层，负责把路由后的 dataframe 推入 planner / validation / execution / answer generation
- `apps/api/app/services/spreadsheet/quality/`
  capability guard、validator、repair、治理规则
- `apps/api/app/services/spreadsheet/execution/`
  selection / transform / exact execution / pivot / formula metric 等执行能力
- `apps/api/app/services/spreadsheet/core/`
  schema、i18n、serialization 等跨层协议

当前分析链路已具备：

- intent 级 clarification 短路
- unsupported capability 阻断
- exact execution 披露
- structured pipeline metadata

### 4.7 会话与回答层

- `apps/api/app/services/spreadsheet/conversation/`
  会话记忆、追问上下文、rule-based / llm-based summarizer、结果格式化
- `conversation_memory.py`
  维护 session、turn summary、dataframe cache

当前会话层会保留：

- 最近轮次的 pipeline 摘要
- analysis intent 摘要
- 上轮命中的 `sheet_index / sheet_name`
- clarification resolution
- 顺序分析任务步骤与当前步骤 id
- analysis anchor 口径快照（用于 follow-up 复用）

## 5. 前端架构

前端主代码位于 `apps/web/src/`，当前已经从单页大组件演进为按功能分区的结构。

### 5.1 顶层装配

- `apps/web/src/app/AppShell.vue`
  负责把 workbook feature、conversation feature、locale 切换装配到同一工作区

### 5.2 Workbook Feature

- `apps/web/src/features/workbook/composables/useWorkbook.ts`
  文件上传、sheet 选择、预览加载、`pendingSheetOverride`
- `apps/web/src/features/workbook/components/WorkbookFeaturePanel.vue`
  workbook 区域容器
- `apps/web/src/components/WorkbookPreviewPanel.vue`
  预览面板与 sheet tabs

### 5.3 Conversation Feature

- `apps/web/src/features/conversation/composables/useConversation.ts`
  管理问题输入、消息列表、conversation id、澄清追问、请求组装
- `apps/web/src/features/conversation/composables/useSseChat.ts`
  SSE 请求与流式状态管理
- `apps/web/src/features/conversation/components/ConversationFeaturePanel.vue`
  对话区域装配

### 5.4 通用组件

- `apps/web/src/components/ConversationComposer.vue`
  输入框、mode 切换、示例 / 说明弹层
- `apps/web/src/components/ConversationMessage.vue`
  答案卡片、sheet 路由/原因解释、执行链路展示、明细表、图表、结构化回答
- `apps/web/src/components/ClarificationOptions.vue`
  clarification 选项交互
- `apps/web/src/components/DataTable.vue`
  预览 / 结果表格
- `apps/web/src/components/SimpleChart.vue`
  基础图表

### 5.5 契约与 i18n

- `apps/web/src/generated/api-types.ts`
  由 OpenAPI 生成
- `apps/web/src/types.ts`
  前端二次包装类型
- `apps/web/src/lib/api.ts`
  HTTP / SSE 调用
- `apps/web/src/lib/chatPayload.ts`
  SSE payload 解析与消息归一化
- `apps/web/src/i18n/messages.ts`
  中英日文案，已覆盖能力边界、sheet routing 和分类示例问题

## 6. 契约、测试与工程护栏

### 6.1 契约治理

- `apps/api/scripts/export_openapi.py`
  从 FastAPI 运行时导出 OpenAPI
- `apps/web/scripts/generate_api_types.py`
  生成前端类型
- `apps/api/scripts/check_contract_artifacts.py`
  校验契约产物未漂移

当前已实现：

- 后端 schema -> OpenAPI -> 前端类型 的单向生成链路
- `sheet_override`、clarification、sheet routing 等字段同步到前端

### 6.2 测试与 CI

- API：`pytest -q apps/api`
- Intent 回归（v0.3 语料）：`python apps/api/scripts/eval_intent_cases.py`
- Web：`npm run ci`
- 前端包含：
  - feature boundary 校验
  - lint
  - typecheck
  - vitest
  - build

### 6.3 基础可观测性

当前系统已补齐基础请求级观测：

- `X-Request-ID`
- request 级结构化日志
- pipeline 中的 `observability`
  - `request_id`
  - `request_total_ms`
  - stage timings
  - `multi_sheet_detected`
  - `clarification_sheet_count`
  - `sheet_switch_count`
  - `multi_sheet_failure_reason`
  - `multi_sheet_top_failure_reasons`（进程内轻量聚合）

### 6.4 如何用 request_id 排障

当上传、预览或对话失败时，前端错误文案里会尽量带上 `request_id`。排查建议如下：

1. 先记录前端显示的 `request_id`
2. 在后端日志中搜索同一个 `request_id`
3. 优先看这几类事件：
   - `http_request_started / http_request_completed`
   - `http_exception / request_validation_failed`
   - `file_upload_started / file_upload_completed`
   - `file_preview_loaded`
   - `spreadsheet_chat_stream_requested / spreadsheet_chat_stream_opened`
   - `spreadsheet_chat_started / spreadsheet_chat_completed / spreadsheet_chat_failed`
4. 如果 HTTP 接口已经成功但 SSE 中断，继续看 pipeline 的 `observability` 字段是否带有同一个 `request_id`

这样可以把“前端看到的报错”快速对应到“后端哪一次请求、哪个阶段失败”。

## 7. 当前边界

### 7.1 已支持

- workbook 内单 sheet 自动路由
- workbook 级多 sheet 澄清与拆解引导
- 在 follow-up 中顺序切换 sheet（先 A 后 B）
- sheet 澄清与手动覆盖
- follow-up 继承上下文
- 语义意图理解
- detail rows / summary table / chart / lightweight forecast
- 前端可见的执行链路与结果口径

### 7.2 暂不支持

- 单轮跨 sheet join 与自由跨 sheet 联合分析
- 多表关系推理与 schema join planning
- 高级统计分析
- 长耗时异步任务队列
- 持久化会话存储与分布式缓存
- 对象存储生产实现

## 8. 后续演进方向

下一阶段建议围绕以下方向继续推进：

1. 在现有顺序多 sheet 流程基础上，补齐可控的跨 sheet 关系建模与 join 规划能力（带显式约束与回退），而不是直接开放自由 join。
2. 把 session store / dataframe cache 进一步抽象为可替换实现，接入 Redis 或数据库。
3. 细化质量治理，把 planner policy、repair policy、capability policy 拆得更清楚。
4. 完善前端消息视图，把 pipeline 可视化从调试信息提升为正式产品组件。
5. 为生产部署补齐异步任务、审计日志和对象存储落地。
