from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "app" / "services" / "spreadsheet"
PACKAGE_PREFIX = "app.services.spreadsheet"
ANALYSIS_ROOT = PACKAGE_ROOT / "analysis"
EXECUTION_ROOT = PACKAGE_ROOT / "execution"
QUALITY_ROOT = PACKAGE_ROOT / "quality"


def _internal_spreadsheet_modules() -> list[Path]:
    return sorted(path for path in PACKAGE_ROOT.rglob("*.py") if "__pycache__" not in path.parts)


def _package_export_names(path: Path) -> list[str]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    exported_names: list[str] = []

    for node in module.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "__all__" and isinstance(value, ast.List):
                exported_names = [elt.value for elt in value.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)]

    return exported_names


def test_spreadsheet_internal_imports_use_relative_paths() -> None:
    violations: list[str] = []

    for path in _internal_spreadsheet_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level:
                continue
            if not node.module or not node.module.startswith(PACKAGE_PREFIX):
                continue
            violations.append(f"{path.relative_to(PACKAGE_ROOT)}:{node.lineno} -> {node.module}")

    assert violations == []


def test_spreadsheet_root_package_exports_nothing() -> None:
    assert _package_export_names(PACKAGE_ROOT / "__init__.py") == []


def test_execution_internal_imports_use_relative_paths() -> None:
    violations: list[str] = []

    for path in sorted(EXECUTION_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level:
                continue
            if not node.module or not node.module.startswith(f"{PACKAGE_PREFIX}.execution"):
                continue
            violations.append(f"{path.relative_to(EXECUTION_ROOT)}:{node.lineno} -> {node.module}")

    assert violations == []


def test_analysis_internal_imports_use_relative_paths() -> None:
    violations: list[str] = []

    for path in sorted(ANALYSIS_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level:
                continue
            if not node.module or not node.module.startswith(f"{PACKAGE_PREFIX}.analysis"):
                continue
            violations.append(f"{path.relative_to(ANALYSIS_ROOT)}:{node.lineno} -> {node.module}")

    assert violations == []


def test_analysis_package_exports_expected_api() -> None:
    assert _package_export_names(ANALYSIS_ROOT / "__init__.py") == [
        "AnalysisPayload",
        "analyze",
        "get_default_planner",
    ]


def test_analysis_package_stays_thin_orchestration() -> None:
    module = ast.parse((ANALYSIS_ROOT / "__init__.py").read_text(encoding="utf-8"))
    top_level_functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]

    assert top_level_functions == [
        "analyze",
    ]


def test_analysis_helper_modules_do_not_depend_on_analysis_entrypoint() -> None:
    helper_modules = {"response.py", "stages.py", "types.py", "utils.py"}
    violations: list[str] = []

    for path in sorted(ANALYSIS_ROOT.glob("*.py")):
        if path.name not in helper_modules:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            is_absolute_analysis_import = node.level == 0 and node.module == "app.services.spreadsheet.analysis"
            if is_absolute_analysis_import:
                violations.append(f"{path.relative_to(ANALYSIS_ROOT)}:{node.lineno}")

    assert violations == []


def test_execution_package_exports_nothing() -> None:
    assert _package_export_names(EXECUTION_ROOT / "__init__.py") == []


def test_execution_executor_stays_thin_orchestration() -> None:
    module = ast.parse((EXECUTION_ROOT / "executor.py").read_text(encoding="utf-8"))
    top_level_functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]

    assert top_level_functions == [
        "apply_selection",
        "apply_transform",
    ]


def test_execution_helper_modules_do_not_depend_on_executor_entrypoint() -> None:
    violations: list[str] = []

    for path in sorted(EXECUTION_ROOT.glob("*.py")):
        if path.name in {"__init__.py", "executor.py", "exact_executor.py"}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            is_relative_executor_import = node.level > 0 and node.module == "executor"
            is_absolute_executor_import = node.level == 0 and node.module == "app.services.spreadsheet.execution.executor"
            if not is_relative_executor_import and not is_absolute_executor_import:
                continue
            violations.append(f"{path.relative_to(EXECUTION_ROOT)}:{node.lineno}")

    assert violations == []


def test_execution_exact_executor_stays_thin_orchestration() -> None:
    module = ast.parse((EXECUTION_ROOT / "exact_executor.py").read_text(encoding="utf-8"))
    top_level_functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]

    assert top_level_functions == [
        "execute_exact_plan",
        "execute_exact_plan_with_source_df",
        "execute_exact_plan_from_source",
    ]


def test_execution_transform_ops_stays_thin_orchestration() -> None:
    module = ast.parse((EXECUTION_ROOT / "transform_ops.py").read_text(encoding="utf-8"))
    top_level_functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]

    assert top_level_functions == [
        "apply_post_agg_operations",
    ]


def test_execution_split_transform_helpers_do_not_depend_on_transform_facade() -> None:
    helper_modules = {"aggregate_ops.py", "derived_ops.py", "formula_ops.py", "pivot_ops.py"}
    violations: list[str] = []

    for path in sorted(EXECUTION_ROOT.glob("*.py")):
        if path.name not in helper_modules:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            is_relative_transform_import = node.level > 0 and node.module == "transform_ops"
            is_absolute_transform_import = node.level == 0 and node.module == "app.services.spreadsheet.execution.transform_ops"
            if not is_relative_transform_import and not is_absolute_transform_import:
                continue
            violations.append(f"{path.relative_to(EXECUTION_ROOT)}:{node.lineno}")

    assert violations == []


def test_execution_exact_helpers_do_not_depend_on_exact_executor_facade() -> None:
    helper_modules = {"exact_metadata.py", "exact_source.py", "exact_support.py"}
    violations: list[str] = []

    for path in sorted(EXECUTION_ROOT.glob("*.py")):
        if path.name not in helper_modules:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            is_relative_exact_import = node.level > 0 and node.module == "exact_executor"
            is_absolute_exact_import = node.level == 0 and node.module == "app.services.spreadsheet.execution.exact_executor"
            if not is_relative_exact_import and not is_absolute_exact_import:
                continue
            violations.append(f"{path.relative_to(EXECUTION_ROOT)}:{node.lineno}")

    assert violations == []


def test_quality_internal_imports_use_relative_paths() -> None:
    violations: list[str] = []

    for path in sorted(QUALITY_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level:
                continue
            if not node.module or not node.module.startswith(f"{PACKAGE_PREFIX}.quality"):
                continue
            violations.append(f"{path.relative_to(QUALITY_ROOT)}:{node.lineno} -> {node.module}")

    assert violations == []


def test_quality_package_exports_nothing() -> None:
    assert _package_export_names(QUALITY_ROOT / "__init__.py") == []


def test_quality_validator_stays_reexport_facade() -> None:
    module = ast.parse((QUALITY_ROOT / "validator.py").read_text(encoding="utf-8"))
    top_level_functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]

    assert top_level_functions == []


def test_quality_repair_stays_thin_orchestration() -> None:
    module = ast.parse((QUALITY_ROOT / "repair.py").read_text(encoding="utf-8"))
    top_level_functions = [node.name for node in module.body if isinstance(node, ast.FunctionDef)]

    assert top_level_functions == [
        "repair_plan",
        "llm_repair_plan",
    ]


def test_quality_rule_helpers_do_not_depend_on_quality_facades() -> None:
    helper_modules = {
        "repair_chart_rules.py",
        "repair_common.py",
        "repair_selection_rules.py",
        "repair_transform_rules.py",
        "validator_chart_rules.py",
        "validator_common.py",
        "validator_selection_rules.py",
        "validator_transform_rules.py",
    }
    violations: list[str] = []

    for path in sorted(QUALITY_ROOT.glob("*.py")):
        if path.name not in helper_modules:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            is_relative_validator_import = node.level > 0 and node.module == "validator"
            is_absolute_validator_import = node.level == 0 and node.module == "app.services.spreadsheet.quality.validator"
            is_relative_repair_import = node.level > 0 and node.module == "repair"
            is_absolute_repair_import = node.level == 0 and node.module == "app.services.spreadsheet.quality.repair"
            if not any(
                [
                    is_relative_validator_import,
                    is_absolute_validator_import,
                    is_relative_repair_import,
                    is_absolute_repair_import,
                ]
            ):
                continue
            violations.append(f"{path.relative_to(QUALITY_ROOT)}:{node.lineno}")

    assert violations == []
