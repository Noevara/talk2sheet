from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
OPENAPI_PATH = ROOT / "packages" / "contracts" / "openapi.json"
OUTPUT_PATH = ROOT / "apps" / "web" / "src" / "generated" / "api-types.ts"
SELECTED_SCHEMAS = [
    "HealthResponse",
    "SheetDescriptor",
    "UploadedFileResponse",
    "PreviewResponse",
    "ClarificationResolution",
    "SpreadsheetChatRequest",
]


def render_type(schema: dict[str, Any]) -> str:
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]

    if "enum" in schema:
        return " | ".join(json.dumps(value, ensure_ascii=False) for value in schema["enum"])

    if "anyOf" in schema:
        return " | ".join(render_type(item) for item in schema["anyOf"])

    schema_type = schema.get("type")
    if schema_type == "string":
        return "string"
    if schema_type in {"integer", "number"}:
        return "number"
    if schema_type == "boolean":
        return "boolean"
    if schema_type == "null":
        return "null"
    if schema_type == "array":
        return f"{render_type(schema.get('items') or {})}[]"
    if schema_type == "object":
        properties = schema.get("properties")
        if not isinstance(properties, dict):
            return "Record<string, unknown>"
        required = set(schema.get("required") or [])
        lines = ["{"]
        for key, value in properties.items():
            optional = "?" if key not in required else ""
            lines.append(f"  {key}{optional}: {render_type(value)};")
        lines.append("}")
        return "\n".join(lines)
    return "unknown"


def render_schema(name: str, schema: dict[str, Any]) -> str:
    rendered = render_type(schema)
    if rendered.startswith("{"):
        return f"export interface {name} {rendered}"
    return f"export type {name} = {rendered};"


def render_selected_schemas(payload: dict[str, Any]) -> str:
    schemas = payload.get("components", {}).get("schemas", {})
    if not isinstance(schemas, dict):
        raise RuntimeError("Missing components.schemas in OpenAPI payload")

    lines = [
        "// This file is generated from packages/contracts/openapi.json.",
        "// Do not edit manually.",
        "",
    ]
    for name in SELECTED_SCHEMAS:
        schema = schemas.get(name)
        if not isinstance(schema, dict):
            continue
        lines.append(render_schema(name, schema))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    payload = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(render_selected_schemas(payload), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
