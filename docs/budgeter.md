# Error Budget Tracking

Pipewatch can track **error budgets** for each pipeline based on a configurable
SLO (Service Level Objective) target.

## Concept

An error budget is the allowable failure rate derived from your SLO:

```
budget_remaining = actual_success_rate - slo_target
```

If `budget_remaining < 0` the budget is **exhausted**.

## Configuration

Set a per-pipeline SLO target in `pipewatch.yaml`:

```yaml
pipelines:
  - name: orders_etl
    source: ...
    thresholds:
      slo_target: 0.99   # 99% success rate required
```

The default SLO target is **95%** when not specified.

## CLI Usage

### Show all budgets

```bash
pipewatch budget show
```

### Filter to one pipeline

```bash
pipewatch budget show --pipeline orders_etl
```

### Limit to last N runs

```bash
pipewatch budget show --window 50
```

### Exit 1 when any budget exhausted (useful in CI)

```bash
pipewatch budget show --fail-exhausted
```

## Output

```
✅ orders_etl: SLO=99% actual=99.5% budget_remaining=+0.5% [OK]
❌ payments_etl: SLO=99% actual=96.0% budget_remaining=-3.0% [EXHAUSTED]
```

## Python API

```python
from pipewatch.budgeter import compute_budget, compute_all_budgets

result = compute_budget("orders_etl", slo_target=0.99)
print(result.exhausted)        # False
print(result.budget_remaining) # +0.005
```
