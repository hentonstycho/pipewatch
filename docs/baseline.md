# Baseline Comparison

Pipewatch can capture a snapshot of current pipeline metrics as a **baseline**
and later compare live metrics against it to surface regressions.

## Capturing a baseline

```bash
pipewatch baseline capture v1
# Baseline 'v1' saved to .pipewatch/baselines/v1.json
```

Baselines are stored as JSON files under `.pipewatch/baselines/` by default.
Override the location with the `PIPEWATCH_BASELINES_DIR` environment variable.

## Listing saved baselines

```bash
pipewatch baseline list
# v1
# pre-deploy
```

## Diffing against a baseline

```bash
pipewatch baseline diff v1
```

Sample output:

```
[orders_pipeline]
  avg_row_count: +0.0000
  avg_error_rate: +0.0200  ⚠ regression
  avg_latency_seconds: -1.2000
```

Fields where the metric **increased** (worse) are flagged with `⚠ regression`.
Note that `avg_row_count` increasing is **not** treated as a regression.

### Restrict to a single pipeline

```bash
pipewatch baseline diff v1 --pipeline orders_pipeline
```

### Exit with code 1 on any regression

Useful in CI pipelines:

```bash
pipewatch baseline diff pre-deploy --fail-on-regression
```

## How deltas are calculated

```
delta = current_value - baseline_value
```

A `null` / `n/a` delta means the metric was unavailable in either the current
run or the saved baseline (e.g. no history recorded yet).

## Storage format

Each baseline file is a JSON document:

```json
{
  "name": "v1",
  "captured_at": "2024-06-01T12:00:00+00:00",
  "metrics": {
    "orders_pipeline": {
      "avg_row_count": 1500.0,
      "avg_error_rate": 0.01,
      "avg_latency_seconds": 4.2,
      "total_runs": 20,
      "failure_count": 2
    }
  }
}
```
