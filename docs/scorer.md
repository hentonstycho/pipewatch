# Pipeline Health Scorer

The **scorer** module computes a numeric health score (0–100) and letter grade
(A–F) for each pipeline based on its recent check history.

## How scoring works

| Factor | Max penalty |
|---|---|
| Overall failure rate over last N checks | 50 pts |
| Consecutive failures at the tail of history | 40 pts |

A pipeline that has never failed in the observation window starts at **100**.
Each factor deducts points; the final score is clamped to `[0, 100]`.

### Grade thresholds

| Score | Grade |
|---|---|
| ≥ 90 | A |
| ≥ 75 | B |
| ≥ 60 | C |
| ≥ 40 | D |
| < 40 | F |

## Python API

```python
from pipewatch.scorer import score_pipeline, score_all
from pipewatch.config import load_config

cfg = load_config("pipewatch.yaml")

# Score a single pipeline
ps = score_pipeline(cfg.pipelines[0], window=20)
print(ps.score, ps.grade, ps.reasons)

# Score all pipelines, sorted worst-first
for ps in score_all(cfg.pipelines, window=20):
    print(f"{ps.pipeline}: {ps.score} ({ps.grade})")
```

## CLI

```
pipewatch score show [OPTIONS]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--config` | `pipewatch.yaml` | Path to config file |
| `--pipeline NAME` | _(all)_ | Restrict to a single pipeline |
| `--window N` | `20` | Number of recent checks to consider |
| `--fail-below SCORE` | _(none)_ | Exit 1 if any score is below threshold |

### Example output

```
[F]  orders                          score= 15.0
     • failure rate 60% over last 20 checks
     • 5 consecutive failure(s)
[A]  users                           score= 98.0
     • all recent checks healthy
```

## Integration with CI

Use `--fail-below` to gate deployments on pipeline health:

```bash
pipewatch score show --fail-below 60 || echo "Pipeline health too low!"
```
