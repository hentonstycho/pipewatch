"""Simple trend forecasting for pipeline metrics using linear extrapolation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class ForecastResult:
    pipeline: str
    metric: str
    current: Optional[float]
    forecasted: Optional[float]
    trend: str  # 'improving', 'degrading', 'stable', 'unknown'

    @property
    def delta(self) -> Optional[float]:
        if self.current is None or self.forecasted is None:
            return None
        return self.forecasted - self.current


def _linear_forecast(values: List[float], steps_ahead: int = 1) -> Optional[float]:
    """Extrapolate the next value using ordinary least-squares on index positions."""
    n = len(values)
    if n < 2:
        return None
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return values[-1]
    slope = numerator / denominator
    return values[-1] + slope * steps_ahead


def _trend_label(current: Optional[float], forecasted: Optional[float], metric: str) -> str:
    """Classify the trend direction, accounting for metric polarity."""
    if current is None or forecasted is None:
        return "unknown"
    delta = forecasted - current
    threshold = 0.01 * max(abs(current), 1e-9)
    if abs(delta) < threshold:
        return "stable"
    # For error_rate and latency_seconds higher is worse; for success_rate higher is better.
    higher_is_bad = metric in ("error_rate", "avg_latency_seconds")
    if higher_is_bad:
        return "degrading" if delta > 0 else "improving"
    return "improving" if delta > 0 else "degrading"


def forecast_pipeline(metrics: PipelineMetrics, steps_ahead: int = 1) -> List[ForecastResult]:
    """Return a ForecastResult for each numeric metric in *metrics*."""
    fields = {
        "success_rate": metrics.success_rate,
        "error_rate": metrics.error_rate,
        "avg_latency_seconds": metrics.avg_latency_seconds,
    }
    results: List[ForecastResult] = []
    for name, value in fields.items():
        if value is None:
            results.append(ForecastResult(
                pipeline=metrics.pipeline,
                metric=name,
                current=None,
                forecasted=None,
                trend="unknown",
            ))
            continue
        # Build a synthetic series: we only have summary stats, so treat the
        # single scalar as the last point and use run_count as a weight hint.
        series = [value] * max(metrics.run_count, 1)
        forecasted = _linear_forecast(series, steps_ahead)
        trend = _trend_label(value, forecasted, name)
        results.append(ForecastResult(
            pipeline=metrics.pipeline,
            metric=name,
            current=value,
            forecasted=forecasted,
            trend=trend,
        ))
    return results


def forecast_all(all_metrics: List[PipelineMetrics], steps_ahead: int = 1) -> List[ForecastResult]:
    """Run forecasting across every pipeline's metrics."""
    out: List[ForecastResult] = []
    for m in all_metrics:
        out.extend(forecast_pipeline(m, steps_ahead))
    return out
