# Correlator

The **correlator** module surfaces pipelines that fail together within a
configurable time window, helping you identify shared root causes (e.g. a
common upstream dependency or infrastructure event).

## How it works

1. For every pipeline defined in `pipewatch.yaml`, the correlator loads its
   failure history from `.pipewatch/history/`.
2. All `(timestamp, pipeline)` failure events are sorted chronologically.
3. A sliding window groups events that occur within `--window` minutes of
   each other across **different** pipelines.
4. Groups with at least `--min-size` members are reported.

## CLI usage

```bash
# Default: 5-minute window, groups of ≥ 2
pipewatch correlate run

# Custom window and minimum group size
pipewatch correlate run --window 10 --min-size 3

# Point at a non-default config or history directory
pipewatch correlate run --config prod.yaml --history-dir /var/pipewatch/history
```

## Example output

```
Group 1 — 3 pipelines [2024-06-01T08:00:00+00:00 … 2024-06-01T08:05:00+00:00]
  • ingest_orders
  • enrich_customers
  • load_warehouse
```

## Python API

```python
from pipewatch.config import load_config
from pipewatch.correlator import correlate_failures

cfg = load_config("pipewatch.yaml")
groups = correlate_failures(cfg, window_minutes=10)
for g in groups:
    print(g.pipelines, g.window_start)
```

## Configuration

No extra keys are needed in `pipewatch.yaml`; the correlator works entirely
from the existing pipeline list and history files.
