from __future__ import annotations

from typing import Any

from ..core.schema import DerivedColumn
from ..pipeline.column_profile import attach_column_profiles, get_column_profiles
from .column_resolution import pick_close_column
from .formula_ops import resolve_operand_series
from .value_coercion import coerce_datetime_series, safe_numeric_series


def apply_derived_columns(df: Any, derived_columns: list[DerivedColumn]) -> tuple[Any, list[dict[str, Any]]]:
    if not derived_columns:
        return df, []
    out = df.copy()
    meta: list[dict[str, Any]] = []
    for item in derived_columns:
        if item.kind == "date_bucket":
            if not item.source_col or not item.grain:
                raise ValueError("date_bucket requires source_col and grain")
            profiles = get_column_profiles(out)
            source_column = pick_close_column(item.source_col, list(out.columns), profiles=profiles)
            dt = coerce_datetime_series(out[source_column])
            valid_mask = dt.notna()
            if item.grain == "day":
                out[item.as_name] = dt.dt.strftime("%Y-%m-%d")
            elif item.grain == "week":
                iso = dt.dt.isocalendar()
                out[item.as_name] = (iso.year.astype("Int64").astype(str) + "-W" + iso.week.astype("Int64").astype(str).str.zfill(2)).where(
                    valid_mask
                )
            elif item.grain == "month":
                out[item.as_name] = dt.dt.strftime("%Y-%m")
            elif item.grain == "quarter":
                out[item.as_name] = (dt.dt.year.astype("Int64").astype(str) + "-Q" + dt.dt.quarter.astype("Int64").astype(str)).where(
                    valid_mask
                )
            elif item.grain == "weekday":
                labels = {0: "周一", 1: "周二", 2: "周三", 3: "周四", 4: "周五", 5: "周六", 6: "周日"}
                out[item.as_name] = dt.dt.weekday.map(labels)
            elif item.grain == "weekpart":
                out[item.as_name] = dt.dt.weekday.map(lambda value: "周末" if value >= 5 else "工作日").where(valid_mask)
            else:
                raise ValueError(f"Unsupported date bucket grain: {item.grain}")
            meta.append({"as_name": item.as_name, "kind": item.kind, "source_col": source_column, "grain": item.grain})
            continue

        if item.kind == "arithmetic":
            left, left_meta = resolve_operand_series(out, item.left)
            right, right_meta = resolve_operand_series(out, item.right)
            left_num = safe_numeric_series(left)
            right_num = safe_numeric_series(right)
            if item.op == "add":
                out[item.as_name] = left_num + right_num
            elif item.op == "sub":
                out[item.as_name] = left_num - right_num
            elif item.op == "mul":
                out[item.as_name] = left_num * right_num
            elif item.op == "div":
                if isinstance(right_num, (int, float)):
                    out[item.as_name] = left_num / right_num if right_num not in (0, 0.0) else float("nan")
                else:
                    out[item.as_name] = left_num / right_num.replace(0, float("nan"))
            else:
                raise ValueError(f"Unsupported arithmetic op: {item.op}")
            meta.append({"as_name": item.as_name, "kind": item.kind, "left": left_meta, "right": right_meta, "op": item.op})
            continue

        raise ValueError(f"Unsupported derived column kind: {item.kind}")
    return attach_column_profiles(out), meta
