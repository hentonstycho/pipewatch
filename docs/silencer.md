# Alert Silencing

Pipewatch supports **temporary alert silencing** for individual pipelines.
This is useful during planned maintenance windows or known degraded states
where you want to suppress notifications without disabling monitoring entirely.

## How It Works

Silences are stored as a JSON file at `.pipewatch/silences.json` (relative to
the working directory). Each entry maps a pipeline name to an ISO-8601 expiry
timestamp in UTC. When a silence expires, alerts resume automatically — no
cleanup step is required.

## CLI Usage

### Silence a pipeline

```bash
pipewatch silence add <pipeline> --minutes 60
```

Suppresses alerts for `<pipeline>` for 60 minutes (default: 60).

### Remove a silence early

```bash
pipewatch silence remove <pipeline>
```

Exits with code `1` if no active silence exists.

### Check silence status

```bash
pipewatch silence status <pipeline>
```

Exits with code `0` if silenced, `1` if not silenced or expired.

## Integration with Alerting

The `is_silenced()` function from `pipewatch.silencer` can be called inside
`dispatch_notifications` or `evaluate_alert` to skip notification dispatch
for silenced pipelines:

```python
from pipewatch.silencer import is_silenced

if not is_silenced(result.pipeline_name):
    dispatch_notifications(config, results)
```

## Silence File Format

```json
{
  "my_pipeline": "2024-06-01T15:30:00+00:00",
  "other_pipeline": "2024-06-01T16:00:00+00:00"
}
```

Entries with past timestamps are treated as inactive and ignored.
