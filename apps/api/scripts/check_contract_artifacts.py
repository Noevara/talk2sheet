from __future__ import annotations

import difflib
import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
API_ROOT = ROOT / "apps" / "api"
WEB_GENERATOR_PATH = ROOT / "apps" / "web" / "scripts" / "generate_api_types.py"
OPENAPI_JSON_PATH = ROOT / "packages" / "contracts" / "openapi.json"
GENERATED_TYPES_PATH = ROOT / "apps" / "web" / "src" / "generated" / "api-types.ts"

if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from scripts.export_openapi import build_openapi_schema, render_openapi_json


def _load_type_generator_module():
    spec = importlib.util.spec_from_file_location("generate_api_types", WEB_GENERATOR_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load generator module from {WEB_GENERATOR_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _diff(expected: str, actual: str, *, expected_name: str, actual_name: str) -> str:
    lines = difflib.unified_diff(
        actual.splitlines(),
        expected.splitlines(),
        fromfile=actual_name,
        tofile=expected_name,
        lineterm="",
    )
    return "\n".join(lines)


def main() -> None:
    failures: list[str] = []

    openapi_schema = build_openapi_schema()
    expected_openapi = render_openapi_json(openapi_schema)
    actual_openapi = OPENAPI_JSON_PATH.read_text(encoding="utf-8")
    if actual_openapi != expected_openapi:
        failures.append(
            "OpenAPI artifact is stale: "
            f"{OPENAPI_JSON_PATH}\n"
            + _diff(
                expected_openapi,
                actual_openapi,
                expected_name="expected_openapi.json",
                actual_name=str(OPENAPI_JSON_PATH),
            )
        )

    generator = _load_type_generator_module()
    expected_types = generator.render_selected_schemas(openapi_schema)
    actual_types = GENERATED_TYPES_PATH.read_text(encoding="utf-8")
    if actual_types != expected_types:
        failures.append(
            "Generated frontend API types are stale: "
            f"{GENERATED_TYPES_PATH}\n"
            + _diff(
                expected_types,
                actual_types,
                expected_name="expected_api-types.ts",
                actual_name=str(GENERATED_TYPES_PATH),
            )
        )

    if failures:
        print("\n\n".join(failures))
        raise SystemExit(1)

    print("Contract artifacts are up to date.")


if __name__ == "__main__":
    main()
