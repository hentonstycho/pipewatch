# Prometheus Metrics Exporter

Pipewatch can export pipeline health metrics in the
[Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/),
making it easy to scrape with Prometheus or store as a text-file collector.

## Exported Metrics

| Metric | Type | Description |
|---|---|---|
| `pipewatch_success_rate` | gauge | Fraction of checks that passed (0–1) |
| `pipewatch_avg_latency_seconds` | gauge | Average pipeline latency across recorded runs |
| `pipewatch_avg_error_rate` | gauge | Average error rate across recorded runs |
| `pipewatch_total_checks` | gauge | Total number of recorded check results |
| `pipewatch_consecutive_failures` | gauge | Current run of consecutive failures |

All metrics carry a `pipeline` label set to the pipeline name from your config.
Metrics with no history are exported as `NaN`.

## CLI Usage

```bash
# Print metrics to stdout
pipewatch export

# Filter to a single pipeline
pipewatch export --pipeline orders_etl

# Write to a file (e.g. for node_exporter textfile collector)
pipewatch export --output /var/lib/node_exporter/textfile_collector/pipewatch.prom
```

## Automating with the Scheduler

Combine with `pipewatch schedule` to keep the file fresh:

```bash
pipewatch schedule --interval 60s & 
watch -n 60 'pipewatch export --output /tmp/pipewatch.prom'
```

## Prometheus Scrape Config Example

```yaml
scrape_configs:
  - job_name: pipewatch
    static_configs:
      - targets: ['localhost:9100']  # node_exporter
```
