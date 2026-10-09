# Data Pipeline Quality System

A configurable Python system for **automated data-quality testing**, **root-cause
investigation of data anomalies**, **pipeline debugging** (stack traces + concurrency),
and **stakeholder reporting**.

Built to answer the three questions every data team eventually asks:

1. *Is my data correct?*
2. *If not, which pipeline stage broke it?*
3. *Is the pipeline itself hanging, deadlocking, or racing?*

---

## Why this exists

Data pipelines fail silently. A join drops rows, an upstream feed sends
`"not-a-date"` where a date was expected, a worker thread grabs two locks in the
wrong order — and nothing crashes. By the time a dashboard shows the wrong number,
the cause is three stages back and a week old.

This project compresses that loop from "days" to "seconds".

---

## Components

| Component | Purpose |
|---|---|
| **`DataQualityTester`** | Runs configurable checks (missing, type, range, unique, regex, referential integrity, outliers) in parallel across columns. Computes a weighted quality score. |
| **`AnomalyInvestigator`** | Traces each quality issue back to the pipeline stage that first introduced it — using per-stage metrics and an operation-risk heuristic. Reports a confidence score. |
| **`AdvancedDebugger`** | Parses Python tracebacks (including chained exceptions), classifies the root cause, and analyses multithreaded logs for **deadlocks**, **lock-order inversions**, **race conditions** and **lock contention**. |
| **`MonitoringReporter`** | Produces a stakeholder report (TXT + JSON), a monitoring-dashboard payload, and Prometheus metrics. |

---

## Quick start

```bash
git clone <your-repo-url>
cd Data_Quality_Pipeline
pip install -r requirements.txt

python sample_data_generator.py     # creates sample CSV + config + logs
python pipeline_quality_system.py   # runs everything end-to-end
```

Output appears in `output/`:

- `pipeline_quality_report.txt` — human-readable report
- `pipeline_quality_report.json` — structured report
- `dashboard_data.json` — monitoring payload (Grafana / Datadog ready)
- `metrics.prom` — Prometheus metrics

---

## Running on your own data

```bash
python pipeline_quality_system.py \
    --data        my_dataset.csv \
    --config      my_quality_config.json \
    --pipeline    my_pipeline_scenario.json \
    --stack-trace my_traceback.txt \
    --log         my_app.log \
    --output      ./my_output
```

Any argument you omit falls back to the generated sample.
`--data` and `--config` are the only strict requirements.

### Minimal config example

```json
{
  "dataset_name": "customers",
  "required_columns": ["customer_id", "email"],
  "columns": {
    "customer_id": {"type": "string",  "unique": true, "nullable": false},
    "email":       {"type": "string",  "nullable": false,
                    "regex": "[^@\\s]+@[^@\\s]+\\.[A-Za-z]{2,}"},
    "age":         {"type": "integer", "min": 0, "max": 120}
  }
}
```

Types: `string`, `integer`, `numeric`, `datetime`, `boolean`.

---

## Example output (excerpt)

```
================================================================================
DATA PIPELINE QUALITY & DEBUGGING REPORT
================================================================================
Dataset   : customers (20 rows, 8 columns)

1. EXECUTIVE SUMMARY
--------------------------------------------------------------------------------
Overall status : AT RISK - do not rely on this data until critical issues are resolved
Quality score  : 54.0 / 100
Issues found   : 10  (critical 0, high 3, medium 5, low 2)
Problem origin : data issues were introduced in pipeline stage(s): clean, ingest, parse
Concurrency    : a DEADLOCK was found in the pipeline's worker threads.

3. ROOT-CAUSE INVESTIGATION
--------------------------------------------------------------------------------
INV-0001  column 'email' / missing_values
   Likely origin : clean (confidence 99%, metric_comparison)
   Evidence      : null_pct for 'email' changed from 0.0 (upstream: parse) to 5.0
                   at stage 'clean'.

4. DEBUGGING FINDINGS
--------------------------------------------------------------------------------
Concurrency analysis: severity CRITICAL - Circular wait (deadlock):
   Worker-2 holds lock_b and waits for lock_a; Worker-1 holds lock_a and waits for lock_b
```

---

## Architecture

```
                     ┌─────────────────────────────┐
    CSV / DataFrame ─►  DataQualityTester          │  parallel checks, quality score
                     └──────────────┬──────────────┘
                                    │ issues
                                    ▼
                     ┌─────────────────────────────┐
                     │  AnomalyInvestigator        │  trace → origin stage
                     └──────────────┬──────────────┘
                                    │
   traceback / log ─►┌─────────────────────────────┐
                     │  AdvancedDebugger           │  stack traces, deadlocks
                     └──────────────┬──────────────┘
                                    │
                                    ▼
                     ┌─────────────────────────────┐
                     │  MonitoringReporter         │  TXT + JSON + Prometheus
                     └─────────────────────────────┘
```

---

## Tests

```bash
python -m pytest tests/ -v
```

13 tests covering:
- Missing values, duplicates, out-of-range values, regex mismatch
- Clean-data path (score == 100.0)
- Anomaly tracing to the correct upstream stage
- Stack-trace parsing (`ZeroDivisionError`, `KeyError`, chained exceptions)
- Deadlock detection with two different thread-name conventions
- Report and dashboard payload shape

---

## Tech stack

Python 3.11+ · pandas · numpy · pytest · threading (`ThreadPoolExecutor`)

## License

MIT