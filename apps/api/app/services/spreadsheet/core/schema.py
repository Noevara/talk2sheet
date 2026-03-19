from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


FilterOp = Literal["=", "!=", ">", ">=", "<", "<=", "in", "contains"]
AggOp = Literal["count_rows", "count_distinct", "sum", "avg", "min", "max", "nunique"]
OrderDir = Literal["asc", "desc"]
ChartType = Literal["line", "bar", "pie", "scatter"]
DateGrain = Literal["day", "week", "month", "quarter", "weekday", "weekpart"]
BinaryOp = Literal["add", "sub", "mul", "div"]


class Filter(BaseModel):
    col: str
    op: FilterOp
    value: Any


class Sort(BaseModel):
    col: str
    dir: OrderDir = "asc"


class SelectionPlan(BaseModel):
    columns: list[str] = Field(default_factory=list)
    filters: list[Filter] = Field(default_factory=list)
    distinct_by: str | None = None
    sort: Sort | None = None
    limit: int | None = None


class Metric(BaseModel):
    agg: AggOp
    col: str | None = None
    as_name: str = "value"


class DerivedColumn(BaseModel):
    as_name: str
    kind: Literal["date_bucket", "arithmetic"]
    source_col: str | None = None
    grain: DateGrain | None = None
    left: str | None = None
    right: str | None = None
    op: BinaryOp | None = None


class FormulaMetric(BaseModel):
    as_name: str
    op: BinaryOp
    left: str
    right: str


class PivotSpec(BaseModel):
    index: list[str] = Field(default_factory=list)
    columns: str
    values: str
    fill_value: Any | None = 0


class TransformPlan(BaseModel):
    derived_columns: list[DerivedColumn] = Field(default_factory=list)
    groupby: list[str] = Field(default_factory=list)
    metrics: list[Metric] = Field(default_factory=list)
    formula_metrics: list[FormulaMetric] = Field(default_factory=list)
    having: list[Filter] = Field(default_factory=list)
    pivot: PivotSpec | None = None
    post_pivot_formula_metrics: list[FormulaMetric] = Field(default_factory=list)
    post_pivot_having: list[Filter] = Field(default_factory=list)
    return_rows: bool = False
    order_by: Sort | None = None
    top_k: int | None = None


class ChartSpec(BaseModel):
    type: ChartType
    title: str | None = None
    x: str
    y: str
    top_k: int | None = None
    agg: Literal["sum", "avg", "min", "max", "count_rows", "count_distinct"] | None = None


class HeaderPlan(BaseModel):
    has_header: bool = True
    header_row_1based: int | None = None
    header_depth: int = 1
    data_start_row_1based: int = 1
    confidence: float = 0.0
    reason: str | None = None


class Clarification(BaseModel):
    reason: str
    field: str | None = None
    options: list[dict[str, Any]] = Field(default_factory=list)
