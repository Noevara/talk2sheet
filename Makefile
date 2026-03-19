.PHONY: api-dev web-dev api-test api-lint web-build contracts-check api-check web-check ci-check

api-dev:
	cd apps/api && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

web-dev:
	cd apps/web && npm run dev -- --host 0.0.0.0 --port 5173

web-build:
	cd apps/web && npm run build

api-test:
	cd apps/api && pytest

api-lint:
	cd apps/api && python -m compileall app

contracts-check:
	cd apps/api && python scripts/check_contract_artifacts.py

api-check:
	cd apps/api && python -m compileall app
	cd apps/api && pytest -q

web-check:
	cd apps/web && npm run ci

ci-check:
	make contracts-check
	make api-check
	make web-check
