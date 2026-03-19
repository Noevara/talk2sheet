# Talk2Sheet

Talk2Sheet is an open-source full-stack framework for conversational analytics on Excel and CSV files.

It lets users ask natural-language questions about spreadsheet data, routes the request to the right sheet inside a workbook, translates the question into an executable analysis plan, runs the analysis with pandas, and returns both the answer and the visible execution pipeline.

## v0.1.0 Scope

Current release focus:

- workbook-aware single-sheet routing
- natural-language spreadsheet analysis
- multi-turn conversation with clarification and follow-up context
- visible execution scope, routing summary, result tables, and charts
- multilingual UI and documentation

Supported now:

- file upload, sheet list, and preview
- workbook-aware routing to one sheet
- row count, totals, averages, distinct count
- Top N / ranking
- detail rows
- trend analysis and basic charts
- lightweight time-series forecasting
- `auto / text / chart` response mode
- structured planner, validator, repair, exact execution, and answer generation pipeline

Not supported yet:

- cross-sheet joins or combined multi-sheet analysis
- advanced statistics
- causal inference
- production object storage and persistent session backends

## Repository Layout

```text
apps/
  api/   FastAPI backend
  web/   Vue 3 frontend
docs/    architecture docs in English / Chinese / Japanese
packages/contracts/  generated OpenAPI artifacts
```

## Architecture

- English: this file
- Chinese: [README.zh-CN.md](./README.zh-CN.md)
- Japanese: [README.ja.md](./README.ja.md)
- Architecture overview: [docs/architecture.en.md](./docs/architecture.en.md)
- Changelog: [CHANGELOG.md](./CHANGELOG.md)

## Local Development

### Backend

```bash
cd apps/api
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
python3.11 -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### Frontend

```bash
cd apps/web
npm install
npm run dev -- --host 127.0.0.1 --port 5173
```

In development, the frontend talks to `http://127.0.0.1:8000/api` by default.

## Security

Copy `.env.example` to a local `.env` file and fill in your model provider settings there.

Do not commit `.env`, uploaded spreadsheets, runtime metadata, or any API keys/passwords to a public repository.

## Docker

```bash
docker compose up --build
```

After startup:

- Web: `http://localhost:8080`
- API: `http://localhost:8000`

If Docker Hub access is unreliable in your region, override base images in `.env`:

```bash
TALK2SHEET_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.11-slim
TALK2SHEET_NODE_IMAGE=docker.m.daocloud.io/node:20-alpine
TALK2SHEET_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:1.27-alpine
```

## Engineering Notes

The backend pipeline currently includes:

1. workbook context loading
2. single-sheet routing
3. capability guard
4. planner and semantic intent understanding
5. validation and repair
6. exact or standard execution
7. structured answer generation
8. SSE streaming of `meta`, `pipeline`, `answer`, and end-of-stream markers

The frontend currently includes:

- workbook feature state
- conversation feature state
- clarification interaction
- categorized example prompts
- execution pipeline visibility
- sheet routing visibility

## Contracts and CI

Contract artifacts are generated from FastAPI runtime schema.

- export OpenAPI: `python apps/api/scripts/export_openapi.py`
- check contract artifacts: `python apps/api/scripts/check_contract_artifacts.py`
- generate frontend API types: `cd apps/web && npm run generate:types`

Validation commands:

- API: `pytest -q apps/api`
- Web: `cd apps/web && npm run ci`
- Combined: `make ci-check`
