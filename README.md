# Talk2Sheet

Talk2Sheet is an open-source full-stack framework for conversational analytics on Excel and CSV files.

It lets users ask natural-language questions about spreadsheet data, resolves the right sheet inside a workbook, translates the question into an executable analysis plan, runs the analysis with pandas, and returns both the answer and the visible execution pipeline.

Latest stable release: `v0.2.0`.
Main branch status (toward `v0.3.0`): workbook-level multi-sheet clarification and sequential A→B analysis guidance are available.

## Current Scope

Current release focus:

- workbook-aware analysis on one sheet at a time
- workbook-level multi-sheet detection and decomposition guidance
- natural-language spreadsheet analysis
- multi-turn conversation with clarification and follow-up context
- visible execution scope, routing summary, result tables, and charts
- answer copy, CSV/PNG export, and local session restore after refresh
- multilingual UI and documentation

Supported now:

- file upload, sheet list, and preview
- workbook-aware routing to one target sheet
- sequential workbook analysis across sheets (analyze A first, then continue on B in follow-up turns)
- multi-sheet question clarification with decomposition hints
- row count, totals, averages, distinct count
- period compare: month-over-month / year-over-year, delta, ratio
- Top N / ranking
- filter + groupby + Top N combined questions
- detail rows
- detail + summary structured answer cards (conclusion / evidence / risk note)
- trend analysis with day/week/month grain
- chart recommendation, chart context metadata, and text fallback when chart rendering is not available
- lightweight time-series forecasting
- `auto / text / chart` response mode
- user-visible analysis pipeline, sheet-routing summary, and structured answer output
- user-visible routing explanations and reason codes
- clarification cards for both sheet and column resolution, with natural confirmation follow-up
- result-card follow-up suggestions that prefill the next question draft
- intent regression corpus and offline evaluation in CI

Not supported yet:

- cross-sheet joins or combined multi-sheet analysis in one step
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
- Latest release notes: [docs/releases/v0.2.0.md](./docs/releases/v0.2.0.md)

## How It Works

1. upload an Excel or CSV file
2. preview workbook sheets and select a sheet when needed
3. ask a natural-language question
4. for multi-sheet questions, let the system clarify and start from one sheet first
5. review the answer together with routing, scope, tables, and charts
6. continue with another sheet in follow-up turns when needed

## Local Development

### Quick Start

1. copy `.env.example` to `.env`
2. if you want live LLM planning / answer generation, fill in `TALK2SHEET_LLM_API_KEY`
3. start the backend
4. start the frontend
5. open `http://127.0.0.1:5173`

If `TALK2SHEET_LLM_API_KEY` is left empty, the app still runs, but some planning or answer paths may fall back to non-LLM behavior depending on provider settings.

### Backend

```bash
cp .env.example .env
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

The backend accepts `TALK2SHEET_LLM_API_KEY` directly and also accepts `OPENAI_API_KEY` as a fallback. Keep both local only.

Do not commit `.env`, uploaded spreadsheets, runtime metadata, or any API keys/passwords to a public repository.

## Docker

```bash
docker compose up --build
```

After startup:

- Web: `http://localhost:8080`
- API: `http://localhost:8000`

Note: the current Docker setup does not inject an LLM API key by default. If you want LLM-backed planning or answer generation in containers, add your local environment configuration explicitly.

To enable LLM-backed planning in Docker:

```bash
cp .env.example .env
# edit .env and set TALK2SHEET_LLM_API_KEY=...
docker compose up --build
```

`docker-compose.yml` now forwards the key and related provider settings into the API container. The key still stays in your local `.env` and should not be committed.

If Docker Hub access is unreliable in your region, override base images in `.env`:

```bash
TALK2SHEET_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.11-slim
TALK2SHEET_NODE_IMAGE=docker.m.daocloud.io/node:20-alpine
TALK2SHEET_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:1.27-alpine
```

## Implementation Notes

The backend request flow currently includes:

1. workbook context loading
2. workbook routing (single-sheet execution per turn, with multi-sheet clarification/decomposition guidance)
3. capability guard
4. planner and intent understanding
5. validation and repair
6. exact or standard execution
7. structured answer generation
8. SSE streaming of `meta`, `pipeline`, `answer`, and end-of-stream markers

The frontend currently provides:

- workbook-oriented state management
- conversation-oriented state management
- clarification interaction
- categorized example prompts
- execution pipeline visibility
- sheet routing visibility
- routing explanation visibility (`reason`, `explanation`, `explanation_code`)

## Contracts and CI

Contract artifacts are generated from FastAPI runtime schema.

- export OpenAPI: `python apps/api/scripts/export_openapi.py`
- check contract artifacts: `python apps/api/scripts/check_contract_artifacts.py`
- generate frontend API types: `cd apps/web && npm run generate:types`

Validation commands:

- API: `pytest -q apps/api`
- Intent regression: `python apps/api/scripts/eval_intent_cases.py`
- Web: `cd apps/web && npm run ci`
- Combined: `make ci-check`

## Troubleshooting

- Upload fails immediately:
  confirm the file is `.xlsx`, `.xls`, or `.csv`, and try a smaller workbook if the file is very large.
- Preview or conversation shows a request id:
  keep the `request_id` from the UI error text and match it with backend logs.
- Docker starts but LLM-backed answers are missing:
  check that `.env` contains `TALK2SHEET_LLM_API_KEY` and that `docker compose` was restarted after editing it.
- A restored session disappears after refresh:
  the previous uploaded file is no longer available under local storage metadata, so upload it again.
