# Changelog

## v0.3.3 (2026-03-22)

Controlled cross-sheet Join Beta release, focused on safe execution boundaries, preflight checks, quality transparency, and explicit fallback behavior.

### Highlights

- Added Join Beta intent gating and boundary governance for cross-sheet requests
- Added Join Preflight checks before beta execution:
  - join key existence
  - type compatibility
  - null/duplicate profile checks
  - estimated match-rate checks
  - repair suggestions
- Added Join Beta execution path for controlled scope:
  - two sheets only
  - single-key equality join
  - `inner` / `left` join types
  - aggregate-oriented questions (`sum` / `count` / `avg` / `top` / `trend`)
- Added join quality visualization and automatic fallback to sequential analysis when risk is high
- Added user-facing boundary and examples updates (EN / ZH / JA) so users can quickly distinguish:
  - supported Join Beta prompts
  - out-of-scope join prompts
- Added Join Beta feature-flag governance:
  - `TALK2SHEET_ENABLE_JOIN_BETA`
  - env-aware default (`dev` on, `prod` off unless explicitly enabled)
- Added independent Join Beta regression corpus + evaluator + CI job with failure snapshot upload:
  - `apps/api/tests/fixtures/join_beta_cases.v0.3.3.json`
  - `python apps/api/scripts/eval_join_beta_cases.py`
  - GitHub Actions job: `join-beta-regression`

### Current Scope

- Workbook-aware routing with one-sheet execution by default
- Sequential multi-sheet analysis (`A -> B`)
- Controlled Join Beta execution under strict constraints
- Join preflight checks, quality signals, and fallback explanation
- Request-level and step-level observability

### Out of Scope

- Arbitrary SQL-style joins and unions
- Multi-key joins, multi-hop joins, joins across 3+ sheets
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends

### Validation

- `pytest -q apps/api/tests/test_spreadsheet_sheet_router.py apps/api/tests/test_spreadsheet_conversation.py apps/api/tests/test_spreadsheet_join_preflight.py apps/api/tests/test_spreadsheet_join_executor.py apps/api/tests/test_spreadsheet_capability_guard.py apps/api/tests/test_config_join_beta.py`
- `python apps/api/scripts/eval_join_beta_cases.py --json`
- `cd apps/web && npm run ci`

## v0.3.2 (2026-03-22)

Batch workbook analysis release, focused on multi-sheet throughput, progress transparency, and batch usability.

### Highlights

- Added batch workbook APIs:
  - `POST /api/spreadsheet/batch`
  - `POST /api/spreadsheet/batch/stream`
- Added per-sheet failure isolation (`status/error/reason_code`) so partial results remain available
- Added real streaming progress and result events:
  - `batch_progress`
  - `batch_result`
  - `batch_error`
  - `batch_done`
- Added configurable constrained parallel execution for batch runs (`TALK2SHEET_BATCH_MAX_PARALLEL`, default `1`, capped at `3`)
- Added batch summary UI and CSV export in conversation cards
- Added batch selection UX improvements:
  - invert selection
  - reuse recent selection
  - recent selection hint
- Added batch regression coverage, performance baseline script, and CI layered check with artifact upload

### Current Scope

- Workbook-aware routing and batch orchestration across selected sheets
- Sheet-by-sheet execution within a single batch request
- Real-time progress visibility and per-sheet result summary
- Row count, totals, averages, ranking, detail rows, trend, compare, charts, lightweight forecast
- Request-level and step-level observability

### Out of Scope

- Cross-sheet join execution in one step
- Unrestricted combined multi-sheet analysis
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends

### Validation

- `pytest -q apps/api/tests/test_spreadsheet_batch_analysis.py apps/api/tests/test_spreadsheet_conversation.py apps/api/tests/test_spreadsheet_analysis.py`
- `cd apps/web && npm run ci`
- `python apps/api/scripts/check_contract_artifacts.py`

## v0.3.1 (2026-03-21)

Sequential multi-sheet task-workflow completion release, focused on step visibility, scope stability, and step-level observability.

### Highlights

- Added task-step workflow payloads and UI cards (`pending/current/completed/failed`) for sequential A->B analysis
- Added one-click `continue_next_step` follow-up action routing
- Added analysis-anchor generation, persistence, and follow-up reuse to reduce scope drift
- Added previous-vs-current step comparison card (non-join) for sequential sheet results
- Added step-level observability events:
  - `task_step_started`
  - `task_step_completed`
  - `task_step_failed`
- Added request-linked step event fields (`request_id`, `step_id`, `sheet`) for faster troubleshooting
- Synced README and architecture docs in English / Chinese / Japanese with v0.3.1 capability wording

### Current Scope

- Workbook-aware routing with one-sheet execution per turn
- Sequential multi-sheet workflow with task steps and continue-next-step action
- Follow-up scope carry-over via analysis anchor
- Row count, totals, averages, ranking, detail rows, trend, compare, charts, lightweight forecast
- Request-level and step-level observability

### Out of Scope

- Cross-sheet join execution in one step
- Unrestricted combined multi-sheet analysis
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends

### Validation

- `pytest -q $(rg --files apps/api/tests | rg 'spreadsheet.*\.py$')`
- `cd apps/web && npm run ci`

## v0.3.0 (2026-03-21)

Workbook-level routing and multi-sheet usability update, focused on safe decomposition and clearer user-visible reasoning.

### Highlights

- Added workbook-level multi-sheet detection and clarification flow for questions that reference multiple sheets
- Added sequential multi-sheet follow-up routing (analyze sheet A first, then continue on sheet B in later turns)
- Added routing explanation visibility (`reason`, `explanation`, `explanation_code`) in conversation cards
- Expanded intent regression corpus for v0.3 scenarios (single-sheet + multi-sheet clarification/sequence/follow-up)
- Added lightweight multi-sheet observability fields in pipeline and logs:
  - `multi_sheet_detected`
  - `clarification_sheet_count`
  - `sheet_switch_count`
  - `multi_sheet_failure_reason`
  - `multi_sheet_top_failure_reasons`
- Synchronized README and architecture docs in English / Chinese / Japanese with the same user-facing capability boundary language

### Current Scope

- Workbook-aware routing with one-sheet execution per turn
- Multi-sheet question clarification and sequential A→B analysis guidance
- Row count, totals, averages, distinct count
- Compare/trend/ranking/detail/basic chart/lightweight forecast
- Clarification and multi-turn follow-up conversation
- Request-level and routing-level observability for troubleshooting

### Out of Scope

- Cross-sheet join execution in one step
- Unrestricted combined multi-sheet analysis
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends

### Validation

- `pytest -q apps/api/tests/planning/test_intent_regression.py apps/api/tests/test_spreadsheet_conversation.py apps/api/tests/test_spreadsheet_sheet_router.py apps/api/tests/test_spreadsheet_planner_modules.py`
- `python apps/api/scripts/eval_intent_cases.py`

## v0.2.0

Single-sheet analysis depth release with stronger intent handling, richer answer structure, and cleaner quality gates.

### Highlights

- Added direct period comparison support for month-over-month / year-over-year questions, including delta and ratio outputs
- Added stronger filter + groupby + Top N planning coverage from natural-language value constraints
- Added day/week/month trend grain handling, recent-N period windows, and relative month trend filters
- Upgraded result cards to structured conclusion/evidence/risk output with better compare/trend/detail presentation
- Added chart recommendation and chart-context alignment across planner, runtime pipeline, and frontend cards
- Added chart downgrade visibility so chart-mode failures still return explainable text results
- Added frontend “continue asking” loop from result cards to composer prefill
- Added intent regression corpus, offline evaluator, CI layering, and failure snapshot artifact upload

### Current Scope

- Workbook-aware single-sheet routing and analysis
- Row count, totals, averages, distinct count
- Period compare, ranking, share, detail rows, trend, basic charts, lightweight forecast
- Clarification and follow-up conversation
- Structured pipeline visibility and request-level debugging identifiers

### Out of Scope

- Cross-sheet joins and combined multi-sheet analysis
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends

### Validation

- `make ci-check`
- `python apps/api/scripts/eval_intent_cases.py`

## v0.1.1

Usability, observability, and release-hardening update for the first public version line.

### Highlights

- Result answer cards now support one-click copy
- Result tables now support CSV export
- Result charts now support PNG export
- Frontend restores the latest local workbook, sheet, mode, and conversation state after refresh
- Clarification cards now distinguish column vs sheet confirmation more clearly, and selected options continue with natural confirmation text
- Upload, preview, and conversation errors now show clearer user-facing guidance, including `request_id` when available
- Frontend requests now inject `X-Request-ID`, and backend HTTP / file / SSE routes now emit more complete request-level structured logs
- Docker and `.env.example` now forward LLM-related settings more explicitly for local container startup
- Frontend state and API helpers were further cleaned up, including persisted-session extraction and simplified message i18n wiring
- Full backend regression is green again after removing stale compatibility drift in planning / exact-execution imports

### Current Scope

- Workbook-aware single-sheet analysis
- Row count, totals, averages, distinct count
- Top N and ranking
- Detail rows
- Trend analysis and basic charts
- Lightweight time-series forecasting
- Clarification and follow-up conversation

### Out of Scope

- Cross-sheet joins and combined multi-sheet analysis
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends

### Validation

- `pytest -q apps/api`
- `cd apps/web && npm run test`
- `cd apps/web && npm run typecheck`
- `cd apps/web && npm run lint`

## v0.1.0

Initial open-source release of Talk2Sheet.

### Highlights

- Full-stack spreadsheet conversation workspace with `Vue 3 + Vite + TypeScript` and `FastAPI + pandas`
- File upload, workbook preview, sheet tabs, and streaming spreadsheet chat
- Workbook-aware single-sheet routing with:
  - explicit sheet reference detection
  - auto routing from sheet and column signals
  - sheet clarification when routing is ambiguous
  - manual sheet override from the frontend
- Multi-turn conversation with:
  - `conversation_id`
  - follow-up context reuse
  - clarification loop
  - preserved routed-sheet context
- Structured semantic intent layer for planner and answer generation
- Deterministic analysis pipeline with planner, validator, repair, exact execution, and answer generation
- User-visible execution disclosure, structured pipeline metadata, simple charts, detail rows, and structured answer segments
- OpenAPI-driven contract artifacts and generated frontend API types
- Layered CI for contracts, API, and frontend checks
- Multilingual documentation in English, Chinese, and Japanese

### Current Scope

- Workbook-aware single-sheet analysis
- Row count, totals, averages, Top N, detail rows, trends, basic charts, and lightweight forecasting

### Out of Scope

- Cross-sheet joins and combined multi-sheet analysis
- Advanced statistics and causal inference
- Production-ready object storage and persistent session backends
