# Alert Quota

Pipewatch can limit how many alerts fire for a given pipeline per calendar day, preventing alert fatigue during sustained outages.

## How It Works

Each time an alert would be dispatched, `record_alert()` increments a per-pipeline counter stored in `.pipewatch/quotas/<pipeline>.json`. If the counter exceeds the configured daily limit, the pipeline is considered **quota-exhausted** and further alerts are suppressed until the next UTC day.

The counter resets automatically at midnight UTC — no cron job required.

## CLI

### Show quota status

```bash
pipewatch quota status
pipewatch quota status --pipeline my_pipeline
pipewatch quota status --limit 20   # custom daily limit (default: 10)
```

Example output:

```
pipe_a: 3/10
pipe_b: 11/10 [EXHAUSTED]
```

### Reset a pipeline's quota

```bash
pipewatch quota reset my_pipeline
```

This deletes the quota state file so the counter starts fresh.

## Python API

```python
from pipewatch.quota import record_alert, get_quota, is_quota_exhausted

# Before dispatching a notification:
if not is_quota_exhausted("my_pipeline", limit=10):
    record_alert("my_pipeline", limit=10)
    dispatch_notifications(...)
```

## Storage

Quota state is written to `.pipewatch/quotas/` as JSON files, one per pipeline:

```json
{"date": "2024-06-01", "count": 4}
```
