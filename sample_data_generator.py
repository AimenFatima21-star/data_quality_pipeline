#!/usr/bin/env python3
"""
sample_data_generator.py
========================

Creates sample data, config, pipeline scenario, a failing stack trace,
and a deadlock log for pipeline_quality_system.py.

Usage:
    python sample_data_generator.py
    # or let pipeline_quality_system.py auto-generate them:
    python pipeline_quality_system.py --regenerate-sample
"""

import json
import os


def _ensure_dirs(base):
    for sub in ("config", "sample_data", "output"):
        os.makedirs(os.path.join(base, sub), exist_ok=True)


# --------------------------------------------------------------------------- #
# 1. Sample customers.csv (deliberately dirty)
# --------------------------------------------------------------------------- #
def _write_customers_csv(path):
    rows = [
        "customer_id,full_name,email,signup_date,age,country,tier,lifetime_value",
        "C001,Alice Johnson,alice@example.com,2023-01-15,34,US,gold,1520.50",
        "C002,Bob Smith,bob.smith@example,2023-02-03,45,US,silver,300.00",       # bad email
        "C003,Carol Diaz,carol@example.com,2023-02-20,-7,CA,gold,880.10",       # bad age
        "C004,Dan Lee,,2023-03-01,29,US,bronze,150.00",                         # missing email
        "C005,Eve Park,eve@example.com,not-a-date,41,GB,gold,2200.00",          # bad date
        "C006,Frank Ray,frank@example.com,2023-04-11,52,US,silver,410.75",
        "C007,Gina Moss,gina@example.com,2023-04-11,52,US,silver,410.75",       # duplicate row of C006 pattern
        "C008,Hank Wu,hank@example.com,2023-05-02,,DE,bronze,95.00",            # missing age
        "C009,Ivy Chen,ivy@example.com,2023-05-19,38,US,platinum,5000.00",      # invalid tier
        "C010,Jack Ford,jack@example.com,2023-06-01,999,US,gold,1200.00",       # age out of range
        "C011,Kate Lin,kate@example.com,2023-06-05,27,US,gold,750.00",
        "C012,Leo Marx,leo@example.com,2023-06-18,33,US,silver,320.40",
        "C013,Mia Nova,mia@example.com,2023-07-01,44,FR,gold,910.90",
        "C014,Ned Polk,ned@example.com,2023-07-21,61,US,bronze,120.00",
        "C015,Ola Reed,ola@example.com,2023-07-30,25,US,gold,1010.10",
        "C001,Alice Johnson,alice@example.com,2023-01-15,34,US,gold,1520.50",   # duplicate id
        "C016,Pat Quinn,pat@example.com,2023-08-05,38,US,silver,260.00",
        "C017,Quinn Ross,quinn@example.com,2023-08-09,47,CA,gold,890.00",
        "C018,Rae Stone,rae@example.com,2023-08-15,29,US,unknown,300.00",       # bad tier
        "C019,Sam Turner,sam@example.com,2023-08-25,33,US,gold,1400.00",
    ]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(rows) + "\n")


# --------------------------------------------------------------------------- #
# 2. Quality config
# --------------------------------------------------------------------------- #
def _write_quality_config(path):
    config = {
        "dataset_name": "customers",
        "log_level": "INFO",
        "log_file": None,
        "max_workers": 4,
        "missing_tokens": ["", "na", "n/a", "nan", "null", "none", "-", "?"],
        "severity_thresholds": {"critical": 20.0, "high": 10.0, "medium": 2.0},
        "check_duplicate_rows": True,
        "check_outliers": True,
        "outlier_iqr_multiplier": 3.0,
        "sample_size": 5,
        "required_columns": ["customer_id", "email", "signup_date"],
        "columns": {
            "customer_id":   {"type": "string", "nullable": False, "unique": True, "regex": r"C\d{3}"},
            "full_name":     {"type": "string", "nullable": False, "min_length": 3},
            "email":         {"type": "string", "nullable": False,
                              "regex": r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}"},
            "signup_date":   {"type": "datetime", "nullable": False},
            "age":           {"type": "integer", "nullable": True, "min": 0, "max": 120},
            "country":       {"type": "string", "allowed_values": ["US", "CA", "GB", "DE", "FR"]},
            "tier":          {"type": "string", "allowed_values": ["bronze", "silver", "gold", "platinum"]},
            "lifetime_value":{"type": "numeric", "min": 0.0},
        },
        "referential_integrity": [],
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)


# --------------------------------------------------------------------------- #
# 3. Pipeline scenario (stages + anomalies + data_flow)
# --------------------------------------------------------------------------- #
def _write_pipeline_scenario(path):
    scenario = {
        "pipeline_config": {
            "stages": [
                {"name": "ingest",   "operation": "extract",  "depends_on": [],
                 "columns_modified": [], "description": "Load raw CSV from upstream SFTP"},
                {"name": "parse",    "operation": "parse",    "depends_on": ["ingest"],
                 "columns_modified": ["signup_date", "age", "lifetime_value"],
                 "description": "Parse dates and numeric fields"},
                {"name": "clean",    "operation": "impute",   "depends_on": ["parse"],
                 "columns_modified": ["email", "age"],
                 "description": "Fill missing emails and ages"},
                {"name": "enrich",   "operation": "join",     "depends_on": ["clean"],
                 "columns_modified": ["tier"],
                 "description": "Join with tier reference table"},
                {"name": "aggregate","operation": "aggregate","depends_on": ["enrich"],
                 "columns_modified": ["lifetime_value"],
                 "description": "Roll up lifetime value per tier"},
            ]
        },
        "anomalies": [
            {
                "column": "email",
                "issue_type": "missing_values",
                "metric": "null_pct",
                "threshold": 1.0,
                "expected_baseline": 0.0,
                "stage_metrics": {
                    "ingest": {"row_count": 20, "duplicate_rows": 2, "columns": {
                        "email": {"null_pct": 0.0, "dtype": "object"}}},
                    "parse":  {"row_count": 20, "duplicate_rows": 2, "columns": {
                        "email": {"null_pct": 0.0, "dtype": "object"}}},
                    "clean":  {"row_count": 20, "duplicate_rows": 2, "columns": {
                        "email": {"null_pct": 5.0, "dtype": "object"}}},
                    "enrich": {"row_count": 20, "duplicate_rows": 2, "columns": {
                        "email": {"null_pct": 5.0, "dtype": "object"}}},
                    "aggregate": {"row_count": 4, "duplicate_rows": 0, "columns": {
                        "email": {"null_pct": 0.0, "dtype": "object"}}},
                },
            },
            {
                "column": "age",
                "issue_type": "out_of_range",
                "metric": "out_of_range_pct",
                "threshold": 1.0,
                "expected_baseline": 0.0,
                "stage_metrics": {
                    "ingest": {"row_count": 20, "columns": {
                        "age": {"null_pct": 5.0, "out_of_range_pct": 0.0, "dtype": "object"}}},
                    "parse":  {"row_count": 20, "columns": {
                        "age": {"null_pct": 5.0, "out_of_range_pct": 10.0, "dtype": "int64"}}},
                    "clean":  {"row_count": 20, "columns": {
                        "age": {"null_pct": 0.0, "out_of_range_pct": 10.0, "dtype": "int64"}}},
                },
            },
        ],
        "data_flow": [
            {"step": "ingest", "operation": "extract",
             "input_columns": [], "output_columns": ["customer_id", "full_name", "email",
                                                     "signup_date", "age", "country",
                                                     "tier", "lifetime_value"],
             "rows_in": 20, "rows_out": 20, "depends_on": []},
            {"step": "parse", "operation": "parse",
             "input_columns": ["customer_id", "signup_date", "age", "lifetime_value"],
             "output_columns": ["customer_id", "signup_date", "age", "lifetime_value"],
             "modified_columns": ["signup_date", "age", "lifetime_value"],
             "rows_in": 20, "rows_out": 20, "depends_on": ["ingest"]},
            {"step": "clean", "operation": "impute",
             "input_columns": ["email", "age"],
             "output_columns": ["email", "age"],
             "modified_columns": ["email", "age"],
             "rows_in": 20, "rows_out": 20, "depends_on": ["parse"]},
            {"step": "enrich", "operation": "join",
             "input_columns": ["tier", "customer_id"],
             "output_columns": ["tier", "customer_id", "tier_name"],
             "modified_columns": ["tier"],
             "rows_in": 20, "rows_out": 18, "depends_on": ["clean"]},
            {"step": "aggregate", "operation": "aggregate",
             "input_columns": ["tier", "lifetime_value"],
             "output_columns": ["tier", "total_value"],
             "rows_in": 18, "rows_out": 4, "depends_on": ["enrich"]},
        ],
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(scenario, fh, indent=2)


# --------------------------------------------------------------------------- #
# 4. Sample failing stack trace
# --------------------------------------------------------------------------- #
def _write_stack_trace(path):
    trace = """Traceback (most recent call last):
  File "/app/pipeline/stages/parse.py", line 42, in parse_dates
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="raise")
  File "/usr/lib/python3.11/site-packages/pandas/core/tools/datetimes.py", line 1050, in to_datetime
    result = convert_listlike(arg._values, format)
  File "/usr/lib/python3.11/site-packages/pandas/core/tools/datetimes.py", line 401, in _convert_listlike_datetimes
    raise ValueError(msg)
ValueError: time data "not-a-date" doesn't match format "%Y-%m-%d"

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/app/pipeline/runner.py", line 118, in _run_stage
    stage_func(df, ctx)
  File "/app/pipeline/stages/parse.py", line 55, in parse_dates
    df.loc[idx, "signup_date"] = pd.to_datetime(row, format="%Y-%m-%d")
  File "/usr/lib/python3.11/site-packages/pandas/core/indexing.py", line 909, in __setitem__
    self.obj._setitem_single_column(loc, value, pi)
KeyError: 'signup_date'
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(trace)


# --------------------------------------------------------------------------- #
# 5. Sample deadlock log
# --------------------------------------------------------------------------- #
def _write_deadlock_log(path):
    log = """2024-06-01 10:00:01,000 - MainThread - INFO - Starting pipeline with 3 workers
2024-06-01 10:00:01,100 - Worker-1 - INFO - attempting to acquire lock_a
2024-06-01 10:00:01,101 - Worker-1 - INFO - acquired lock_a
2024-06-01 10:00:01,120 - Worker-2 - INFO - attempting to acquire lock_b
2024-06-01 10:00:01,121 - Worker-2 - INFO - acquired lock_b
2024-06-01 10:00:01,200 - Worker-1 - INFO - waiting for lock_b
2024-06-01 10:00:01,210 - Worker-2 - INFO - waiting for lock_a
2024-06-01 10:00:05,300 - Worker-1 - WARNING - still waiting for lock_b
2024-06-01 10:00:05,310 - Worker-2 - WARNING - still waiting for lock_a
2024-06-01 10:00:10,400 - Worker-1 - ERROR - timeout waiting for lock_b
2024-06-01 10:00:10,410 - Worker-2 - ERROR - timeout waiting for lock_a
2024-06-01 10:00:10,500 - Worker-3 - INFO - acquired lock_c
2024-06-01 10:00:10,510 - Worker-3 - INFO - released lock_c
2024-06-01 10:00:15,000 - MainThread - ERROR - Possible race condition detected in aggregation counter
2024-06-01 10:00:15,010 - MainThread - ERROR - expected 20 but got 17 rows in output
2024-06-01 10:00:15,100 - Worker-1 - ERROR - Retry attempt 1 for lock_b
2024-06-01 10:00:15,200 - Worker-1 - ERROR - Retry attempt 2 for lock_b
2024-06-01 10:00:15,300 - Worker-2 - ERROR - Retry attempt 1 for lock_a
2024-06-01 10:00:15,400 - Worker-1 - ERROR - Traceback (most recent call last):
"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(log)


# --------------------------------------------------------------------------- #
# Public entrypoint
# --------------------------------------------------------------------------- #
def generate_all(base_dir: str = None) -> str:
    """Generate every sample file inside base_dir (default: this file's directory)."""
    base = base_dir or os.path.dirname(os.path.abspath(__file__))
    _ensure_dirs(base)
    _write_customers_csv(os.path.join(base, "sample_data", "customers.csv"))
    _write_quality_config(os.path.join(base, "config", "quality_config.json"))
    _write_pipeline_scenario(os.path.join(base, "sample_data", "pipeline_scenario.json"))
    _write_stack_trace(os.path.join(base, "sample_data", "failed_pipeline_trace.txt"))
    _write_deadlock_log(os.path.join(base, "sample_data", "deadlock.log"))
    return base


if __name__ == "__main__":
    where = generate_all()
    print(f"Sample data + config generated under: {where}")