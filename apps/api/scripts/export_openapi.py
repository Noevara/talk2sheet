from __future__ import annotations

import json
import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from app.main import app


def build_openapi_schema() -> dict:
    return app.openapi()


def render_openapi_json(schema: dict) -> str:
    return json.dumps(schema, ensure_ascii=False, indent=2) + "\n"


def write_openapi_json(target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_openapi_json(build_openapi_schema()), encoding="utf-8")
    print(f"Wrote {target}")


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    write_openapi_json(root / "packages" / "contracts" / "openapi.json")


if __name__ == "__main__":
    main()
