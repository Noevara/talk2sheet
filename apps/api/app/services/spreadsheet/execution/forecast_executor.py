from __future__ import annotations

import math
import re
from datetime import date
from typing import Any

import numpy as np
import pandas as pd


_ISO_WEEK_PATTERN = re.compile(r"^(\d{4})-W(\d{2})$")


def _period_ordinal(value: Any, *, grain: str) -> int:
    text = str(value or "").strip()
    if grain == "day":
        return int(pd.Period(text, freq="D").ordinal)
    if grain == "month":
        return int(pd.Period(text, freq="M").ordinal)
    if grain == "quarter":
        return int(pd.Period(text, freq="Q").ordinal)
    if grain == "week":
        matched = _ISO_WEEK_PATTERN.match(text)
        if not matched:
            raise ValueError(f"Unsupported ISO week literal: {text}")
        year = int(matched.group(1))
        week = int(matched.group(2))
        return date.fromisocalendar(year, week, 1).toordinal()
    raise ValueError(f"Unsupported forecast grain: {grain}")


def _linear_forecast(x: np.ndarray, y: np.ndarray, *, target_x: float) -> dict[str, Any]:
    if len(x) < 2:
        raise ValueError("Linear forecast requires at least 2 points.")
    slope, intercept = np.polyfit(x, y, 1)
    fitted = intercept + slope * x
    prediction = float(intercept + slope * target_x)
    mae = float(np.mean(np.abs(y - fitted)))
    rmse = float(math.sqrt(np.mean((y - fitted) ** 2)))
    return {
        "model": "linear_regression",
        "prediction": prediction,
        "fitted": fitted,
        "mae": mae,
        "rmse": rmse,
        "slope": float(slope),
        "intercept": float(intercept),
    }


def _ses_forecast(y: np.ndarray) -> dict[str, Any]:
    if len(y) < 2:
        raise ValueError("Exponential smoothing requires at least 2 points.")
    candidates = [0.2, 0.35, 0.5, 0.65, 0.8]
    best: dict[str, Any] | None = None
    for alpha in candidates:
        level = float(y[0])
        preds: list[float] = [level]
        residuals: list[float] = []
        for actual in y[1:]:
            preds.append(level)
            residuals.append(float(actual - level))
            level = alpha * float(actual) + (1.0 - alpha) * level
        mae = float(np.mean(np.abs(residuals))) if residuals else 0.0
        rmse = float(math.sqrt(np.mean(np.square(residuals)))) if residuals else 0.0
        candidate = {
            "model": "simple_exponential_smoothing",
            "prediction": float(level),
            "fitted": np.array(preds, dtype=float),
            "mae": mae,
            "rmse": rmse,
            "alpha": float(alpha),
        }
        if best is None or candidate["mae"] < best["mae"]:
            best = candidate
    assert best is not None
    return best


def _predict_with_model(best: dict[str, Any], target_x: np.ndarray) -> np.ndarray:
    model = str(best.get("model") or "")
    if model == "linear_regression":
        slope = float(best.get("slope") or 0.0)
        intercept = float(best.get("intercept") or 0.0)
        return intercept + slope * target_x
    if model == "simple_exponential_smoothing":
        level = float(best.get("prediction") or 0.0)
        return np.full(shape=len(target_x), fill_value=level, dtype=float)
    raise ValueError(f"Unsupported forecast model: {model}")


def forecast_time_series(
    history_df: pd.DataFrame,
    *,
    period_column: str,
    value_column: str,
    target_period: str | None = None,
    target_periods: list[str] | None = None,
    grain: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if period_column not in history_df.columns or value_column not in history_df.columns:
        raise ValueError("Forecast input is missing the required period or value column.")

    working = history_df[[period_column, value_column]].copy()
    working[value_column] = pd.to_numeric(working[value_column], errors="coerce")
    working = working.dropna(subset=[period_column, value_column]).copy()
    if working.empty:
        raise ValueError("No historical data is available for forecasting.")

    working["__ordinal__"] = working[period_column].map(lambda value: _period_ordinal(value, grain=grain))
    working = working.sort_values("__ordinal__", kind="mergesort").reset_index(drop=True)
    if int(len(working.index)) < 3:
        raise ValueError("At least 3 historical periods are required for forecasting.")

    latest_ordinal = int(working["__ordinal__"].iloc[-1])
    target_period_list = [str(item) for item in (target_periods or []) if str(item or "").strip()]
    if not target_period_list and target_period:
        target_period_list = [str(target_period)]
    if not target_period_list:
        raise ValueError("The forecast target period is required.")

    target_ordinals = [_period_ordinal(item, grain=grain) for item in target_period_list]
    ordered_pairs = sorted(zip(target_ordinals, target_period_list), key=lambda item: item[0])
    target_ordinals = [int(item[0]) for item in ordered_pairs]
    target_period_list = [str(item[1]) for item in ordered_pairs]
    if target_ordinals[0] <= latest_ordinal:
        raise ValueError("The forecast target period must be later than the latest observed period.")

    base_ordinal = int(working["__ordinal__"].iloc[0])
    x = (working["__ordinal__"] - base_ordinal).astype(float).to_numpy()
    y = working[value_column].astype(float).to_numpy()
    target_x = np.array([float(ordinal - base_ordinal) for ordinal in target_ordinals], dtype=float)

    linear = _linear_forecast(x, y, target_x=float(target_x[-1]))
    ses = _ses_forecast(y)
    best = linear if float(linear["mae"]) <= float(ses["mae"]) else ses

    raw_predictions = _predict_with_model(best, target_x)
    predictions = np.maximum(raw_predictions.astype(float), 0.0)
    horizon_steps = np.array([max(1, int(ordinal - latest_ordinal)) for ordinal in target_ordinals], dtype=int)
    rmse_base = max(float(best["rmse"]), 1.0)
    intervals: list[float] = []
    lower_bounds: list[float] = []
    upper_bounds: list[float] = []
    for prediction, step in zip(predictions.tolist(), horizon_steps.tolist()):
        rmse = max(rmse_base * math.sqrt(float(step)), prediction * 0.05 if prediction else 1.0)
        interval = 1.96 * rmse
        intervals.append(interval)
        lower_bounds.append(max(0.0, prediction - interval))
        upper_bounds.append(prediction + interval)

    latest_value = float(working[value_column].iloc[-1])
    latest_period = str(working[period_column].iloc[-1])
    history_start = str(working[period_column].iloc[0])
    history_end = latest_period
    horizon = int(max(horizon_steps.tolist()))

    forecast_df = pd.DataFrame(
        {
            period_column: target_period_list,
            "forecast_value": predictions.tolist(),
            "lower_bound": lower_bounds,
            "upper_bound": upper_bounds,
            "model": [str(best["model"])] * len(target_period_list),
        }
    )
    total_forecast_value = float(np.sum(predictions))
    total_lower_bound = float(np.sum(lower_bounds))
    total_upper_bound = float(np.sum(upper_bounds))
    average_forecast_value = float(np.mean(predictions))
    meta = {
        "period_column": period_column,
        "value_column": value_column,
        "target_period": target_period_list[-1],
        "target_periods": target_period_list,
        "target_start_period": target_period_list[0],
        "target_end_period": target_period_list[-1],
        "target_count": int(len(target_period_list)),
        "grain": grain,
        "model": str(best["model"]),
        "history_points": int(len(working.index)),
        "history_start": history_start,
        "history_end": history_end,
        "latest_period": latest_period,
        "latest_value": latest_value,
        "horizon": horizon,
        "mae": float(best["mae"]),
        "rmse": float(best["rmse"]),
        "forecast_value": total_forecast_value if len(target_period_list) > 1 else float(predictions[0]),
        "lower_bound": total_lower_bound if len(target_period_list) > 1 else float(lower_bounds[0]),
        "upper_bound": total_upper_bound if len(target_period_list) > 1 else float(upper_bounds[0]),
        "average_forecast_value": average_forecast_value,
        "first_forecast_value": float(predictions[0]),
        "last_forecast_value": float(predictions[-1]),
        "multi_step": bool(len(target_period_list) > 1),
        **({"alpha": float(best["alpha"])} if "alpha" in best else {}),
        **({"slope": float(best["slope"])} if "slope" in best else {}),
    }
    return forecast_df, meta
