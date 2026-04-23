# Replayer

The **replayer** module lets you replay historical check results through the
pipewatch pipeline — useful for auditing, debugging threshold changes, or
verifying that alerts would have fired correctly against real data.

## How it works

`replay_pipeline` reads the JSONL history file for a single pipeline and
returns a list of `ReplayEvent` objects sorted oldest-first.  Each event
wraps a `CheckResult` and the original timestamp from the history file.

`replay_all` merges events across all configured pipelines, also sorted by
timestamp, giving a chronological view of the entire fleet.

## CLI

```
pipewatch replay run [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--config` | `pipewatch.yaml` | Path to config file |
| `--pipeline` | *(all)* | Limit output to one pipeline |
| `--since` | *(none)* | ISO datetime lower bound (UTC) |
| `--limit` | *(none)* | Max events per pipeline |
| `--history-dir` | `.pipewatch/history` | History directory |

### Examples

Replay all events from the last week:

```bash
pipewatch replay run --since 2024-06-01T00:00:00
```

Replay only the last 20 events for a specific pipeline:

```bash
pipewatch replay run --pipeline orders_etl --limit 20
```

## Output

Each line shows the original timestamp, pipeline name, and status:

```
2024-06-01T08:00:00+00:00  orders_etl                     OK
2024-06-01T09:00:00+00:00  users_etl                      FAIL  [row_count]
```

Failing events include the violated threshold names in brackets.

## Programmatic use

```python
from pathlib import Path
from pipewatch.replayer import replay_pipeline
from pipewatch.config import PipewatchConfig

cfg = PipewatchConfig.load(Path("pipewatch.yaml"))
pipeline_cfg = cfg.pipelines[0]
events = replay_pipeline(pipeline_cfg, Path(".pipewatch/history"), limit=50)
for ev in events:
    print(ev)
```
