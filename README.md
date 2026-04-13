# pipewatch

A lightweight CLI for monitoring and alerting on ETL pipeline health with configurable thresholds and Slack/email notifications.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Define your pipeline checks in a `pipewatch.yaml` config file:

```yaml
pipelines:
  - name: daily_sales_etl
    check: row_count
    threshold:
      min: 1000
    alert:
      slack: "#data-alerts"
      email: "team@example.com"
```

Then run the monitor:

```bash
pipewatch run --config pipewatch.yaml
```

You can also run a one-off check directly from the CLI:

```bash
pipewatch check --pipeline daily_sales_etl --metric row_count
```

Set up credentials for notifications via environment variables:

```bash
export PIPEWATCH_SLACK_TOKEN=xoxb-your-token
export PIPEWATCH_SMTP_HOST=smtp.example.com
```

---

## Features

- Configurable thresholds for row counts, null rates, freshness, and more
- Slack and email alerting out of the box
- Lightweight with minimal dependencies
- Easy integration into cron jobs or CI/CD pipelines

---

## License

This project is licensed under the [MIT License](LICENSE).