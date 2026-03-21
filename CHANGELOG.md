# Changelog

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
