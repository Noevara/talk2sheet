# Changelog

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
