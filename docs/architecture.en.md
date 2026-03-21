# Talk2Sheet Architecture

## 1. Positioning

Talk2Sheet is an open-source spreadsheet conversation framework for Excel and CSV analysis. The current architecture is intentionally focused on:

- workbook-aware routing with one-sheet execution per turn
- multi-sheet question detection, clarification, and sequential decomposition (A first, then B)
- natural language to structured analysis planning
- pandas-based executable analysis
- multi-turn conversation with clarification, follow-up, mode switching, and context carry-over
- structured UI feedback for pipeline metadata, execution scope, results, and final answer

This is not positioned as a general BI or advanced statistics platform yet. The current boundary is explicit:

- Supported: workbook-aware routing, per-turn single-sheet execution, sequential multi-sheet follow-up, detail / summary / ranking / trend / basic chart / lightweight forecast
- Not supported yet: cross-sheet joins in one step, unrestricted combined multi-sheet execution, relationship modeling across sheets, advanced statistics, causal inference, complex forecasting workflows

## 2. Implemented Capabilities

### 2.1 Product capabilities

- file upload, sheet listing, and preview
- file storage abstracted behind `services/storage/`
- workbook-aware single-sheet routing
  - explicit sheet reference in the question
  - auto routing from sheet name / column name signals
  - multi-sheet clarification and decomposition hints
  - sequential sheet switch in follow-up turns
  - sheet clarification when candidates are ambiguous
  - manual sheet override through `sheet_override`
  - support for resolving one target sheet per turn inside a workbook
  - no cross-sheet join execution yet
- natural-language analysis
  - row count, total, average, distinct count
  - Top N / ranking
  - detail rows
  - trend analysis and basic charts
  - lightweight time-series forecast
  - semantic intent understanding instead of relying only on a loose string intent
- multi-turn conversation
  - recent pipeline summaries stored in memory
  - reuse of semantic and sheet context
  - clarification loop
- frontend execution visibility
  - execution pipeline visibility
  - sheet routing summary
  - result tables / detail tables / simple charts
  - structured answer segments
  - `mode = auto / text / chart`
  - OpenAPI-driven contract sync
  - baseline observability
- the frontend has been split into feature-oriented modules
- CI already includes layered validation

## 3. End-to-End Request Flow

The main runtime path for one request is:

1. the frontend uploads a file to `/api/files/upload`
2. the backend stores the file through `services/storage/` and returns a `file_id`
3. the frontend loads sheet preview and builds workbook / active-sheet context
4. the user submits a question with:
   - `chat_text`
   - `mode`
   - `sheet_index`
   - `sheet_override`
   - `conversation_id`
   - `clarification_resolution`
5. `stream_spreadsheet_chat()` on the backend:
   - creates or resumes a conversation session
   - reads workbook context
   - resolves the target sheet
   - loads a sampled dataframe for that sheet
   - forwards execution into the analysis orchestrator
6. inside the analysis orchestrator:
   - capability guard
   - planner
   - semantic intent understanding
   - validation / repair
   - exact execution or standard execution
   - answer generation
7. the backend streams SSE chunks for:
   - `meta`
   - `pipeline`
   - `answer`
   - `EOS`
8. the frontend assembles these chunks into conversation cards with scope, routing, chart, detail rows, and structured answer content

## 4. Backend Architecture

The backend lives under `apps/api/app/` and uses a responsibility-oriented package layout that can keep evolving without a large rewrite.

### 4.1 API and infrastructure

- `apps/api/app/main.py`
  FastAPI app entry, CORS, middleware, and unified exception handling
- `apps/api/app/schemas.py`
  HTTP request / response contracts
- `apps/api/app/observability.py`
  `X-Request-ID` and structured logging helpers
- `apps/api/app/api/routes/files.py`
  upload, sheet listing, and preview endpoints
- `apps/api/app/api/routes/spreadsheet.py`
  SSE chat endpoint

### 4.2 Storage layer

- `apps/api/app/services/storage/`
  abstract file-store surface
- `local_file_store.py`
  current default implementation, chunked write to local disk plus metadata
- `object_storage_file_store.py`
  placeholder for object storage integration

This layer separates file persistence from spreadsheet analysis logic, which makes future object storage or alternative persistence backends easier to add.

### 4.3 Spreadsheet pipeline layer

- `apps/api/app/services/spreadsheet/pipeline/`
  file loading, sheet metadata, header detection, preview generation, workbook context building
- `workbook_context.py`
  workbook-level context used by sheet routing
- `sheet_metadata.py`
  sheet descriptor loading

This layer converts a raw file into workbook metadata and analyzable dataframes.

### 4.4 Routing layer

- `apps/api/app/services/spreadsheet/routing/sheet_router.py`
  single-sheet routing within a workbook
- `router_types.py`
  typed routing decision model

Current routing priority is roughly:

1. single-sheet shortcut
2. clarification resolution
3. manual `sheet_override`
4. follow-up sheet-switch resolution (explicit / previous / another)
5. multi-sheet clarification for decomposition
6. explicit sheet reference in the question
7. follow-up inheritance from the last turn
8. auto scoring from sheet names, columns, and hints
9. clarification when multiple candidates are too close
10. fallback to the requested sheet

### 4.5 Planning and semantic layer

- `apps/api/app/services/spreadsheet/planning/`
  planner providers, heuristic rules, LLM planner, follow-up planning, and guardrails
- `intent_models.py`
  structured `AnalysisIntent`
- `intent_understanding.py`
  semantic intent understanding
- `intent_accessors.py`
  shared semantic-intent access across planner, answer, and memory layers

The key improvement here is that the system no longer relies only on a loose `intent` string. It preserves richer structure such as:

- `target_metric`
- `target_dimension`
- `comparison_type`
- `time_scope`
- `answer_expectation`
- `clarification`

### 4.6 Analysis, quality, and execution layer

- `apps/api/app/services/spreadsheet/analysis/`
  top-level orchestration layer that wires planner, validation, execution, and answer generation
- `apps/api/app/services/spreadsheet/quality/`
  capability guard, validator, repair, and governance policies
- `apps/api/app/services/spreadsheet/execution/`
  selection / transform / exact execution / pivot / formula-metric execution logic
- `apps/api/app/services/spreadsheet/core/`
  shared schema, i18n, serialization, and utility contracts

The current analysis path already includes:

- intent-level clarification short-circuiting
- unsupported capability blocking
- exact-execution disclosure
- structured pipeline metadata

### 4.7 Conversation and answer layer

- `apps/api/app/services/spreadsheet/conversation/`
  session memory, follow-up context building, rule-based / llm-based summarizers, formatting helpers
- `conversation_memory.py`
  in-memory sessions, turn summaries, and dataframe cache

The conversation layer currently stores:

- recent pipeline summaries
- analysis intent summaries
- last resolved `sheet_index / sheet_name`
- clarification resolution payloads

## 5. Frontend Architecture

The frontend lives under `apps/web/src/` and has already moved away from a single giant page component into feature-oriented structure.

### 5.1 Top-level composition

- `apps/web/src/app/AppShell.vue`
  composes workbook feature, conversation feature, and locale switching into the main workspace

### 5.2 Workbook feature

- `apps/web/src/features/workbook/composables/useWorkbook.ts`
  file upload, sheet selection, preview loading, `pendingSheetOverride`
- `apps/web/src/features/workbook/components/WorkbookFeaturePanel.vue`
  workbook-side feature wrapper
- `apps/web/src/components/WorkbookPreviewPanel.vue`
  preview panel and sheet tabs

### 5.3 Conversation feature

- `apps/web/src/features/conversation/composables/useConversation.ts`
  question state, message list, conversation id, clarification follow-up, request assembly
- `apps/web/src/features/conversation/composables/useSseChat.ts`
  SSE request / streaming state
- `apps/web/src/features/conversation/components/ConversationFeaturePanel.vue`
  conversation-side feature wrapper

### 5.4 Shared UI components

- `apps/web/src/components/ConversationComposer.vue`
  input, mode switch, examples / guide popovers
- `apps/web/src/components/ConversationMessage.vue`
  answer card, sheet routing summary/reason explanation, execution disclosure, detail table, chart, structured answer
- `apps/web/src/components/ClarificationOptions.vue`
  clarification interaction
- `apps/web/src/components/DataTable.vue`
  preview and result tables
- `apps/web/src/components/SimpleChart.vue`
  lightweight chart rendering

### 5.5 Contracts and i18n

- `apps/web/src/generated/api-types.ts`
  generated from OpenAPI
- `apps/web/src/types.ts`
  frontend-level typed wrappers
- `apps/web/src/lib/api.ts`
  HTTP and SSE access
- `apps/web/src/lib/chatPayload.ts`
  SSE payload normalization
- `apps/web/src/i18n/messages.ts`
  English / Chinese / Japanese copy, including capability boundaries, sheet routing, and categorized example prompts

## 6. Contracts, Testing, and Engineering Guardrails

### 6.1 Contract governance

- `apps/api/scripts/export_openapi.py`
  exports OpenAPI from the FastAPI runtime schema
- `apps/web/scripts/generate_api_types.py`
  generates frontend types
- `apps/api/scripts/check_contract_artifacts.py`
  verifies that generated contract artifacts are not stale

Current contract flow:

- backend schema -> OpenAPI -> frontend types
- fields such as `sheet_override`, clarification payloads, and sheet-routing metadata are synchronized through this path

### 6.2 Testing and CI

- API: `pytest -q apps/api`
- intent regression (v0.3 corpus): `python apps/api/scripts/eval_intent_cases.py`
- Web: `npm run ci`
- frontend CI includes:
  - feature boundary checks
  - lint
  - typecheck
  - vitest
  - build

### 6.3 Baseline observability

The system already includes request-level baseline observability:

- `X-Request-ID`
- structured request logs
- `observability` block inside pipeline metadata
  - `request_id`
  - `request_total_ms`
  - stage timings
  - `multi_sheet_detected`
  - `clarification_sheet_count`
  - `sheet_switch_count`
  - `multi_sheet_failure_reason`
  - `multi_sheet_top_failure_reasons` (lightweight in-process aggregate)

### 6.4 How To Troubleshoot With request_id

When upload, preview, or conversation requests fail, the frontend error text tries to keep the `request_id` visible. A practical debugging flow is:

1. copy the `request_id` shown in the UI
2. search backend logs for the same `request_id`
3. check these events first:
   - `http_request_started / http_request_completed`
   - `http_exception / request_validation_failed`
   - `file_upload_started / file_upload_completed`
   - `file_preview_loaded`
   - `spreadsheet_chat_stream_requested / spreadsheet_chat_stream_opened`
   - `spreadsheet_chat_started / spreadsheet_chat_completed / spreadsheet_chat_failed`
4. if the HTTP request succeeded but SSE failed later, compare that id with the pipeline `observability.request_id`

This is the shortest path from a user-visible error to the failing backend stage.

## 7. Current Boundaries

### 7.1 Supported now

- workbook-aware single-sheet auto routing
- workbook-level multi-sheet clarification and decomposition guidance
- sequential workbook analysis across sheets in follow-up turns (A then B)
- sheet clarification and manual override
- follow-up context inheritance
- semantic intent understanding
- detail rows / summary table / chart / lightweight forecast
- user-visible execution pipeline and scope disclosure

### 7.2 Not supported yet

- cross-sheet joins and one-step combined multi-sheet analysis
- multi-sheet relationship reasoning and join planning
- advanced statistics
- long-running async job orchestration
- persistent session store and distributed dataframe cache
- production-ready object storage implementation

## 8. Next Architectural Directions

The next steps should extend the current design rather than replace it:

1. introduce explicit cross-sheet relationship modeling and guarded join planning on top of the current sequential multi-sheet flow
2. extract session store and dataframe cache into replaceable adapters backed by Redis or a database
3. split planner / repair / capability governance into clearer policy surfaces
4. evolve the frontend pipeline view from debug-adjacent metadata into a more productized explanation layer
5. add async jobs, audit logs, and object storage for production deployment
