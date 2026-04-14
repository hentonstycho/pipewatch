# Aggregator

The `aggregator` module computes rollup statistics across **all configured pipelines**, giving a
bird's-eye view of the overall health of your ETL estate.

## Data model

```
RollupStats
  total_pipelines   int    — number of pipelines in config
  healthy_pipelines int    — pipelines whose last check passed
  degraded_pipelines int   — pipelines whose last check failed
  total_checks      int    — total historical check entries
  total_failures    int    — total failed check entries
  failure_rate      float  — total_failures / total_checks (0–1)
  most_failing      str?   — pipeline with the most historical failures
  pipelines         [str]  — list of all pipeline names
```

## CLI usage

```bash
# Print a summary table
pipewatch aggregate summary

# Exit 1 if fewer than 80 % of pipelines are currently healthy
pipewatch aggregate summary --fail-below 0.8

# Use a custom config or history location
pipewatch aggregate summary \
  --config /etc/pipewatch.yaml \
  --history-dir /var/pipewatch/history
```

## Programmatic usage

```python
from pipewatch.config import load_config
from pipewatch.aggregator import aggregate

cfg = load_config("pipewatch.yaml")
stats = aggregate(cfg)
print(f"{stats.healthy_pipelines}/{stats.total_pipelines} healthy")
print(f"Overall failure rate: {stats.failure_rate:.1%}")
```

## Integration with alerting

Combine with `pipewatch.alerting` to fire a single Slack message when the estate-wide
healthy ratio drops below a configured threshold — rather than alerting per-pipeline.
