"""Unit tests for pipeline_quality_system."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pipeline_quality_system import (
    DataQualityTester, AnomalyInvestigator, AdvancedDebugger, MonitoringReporter,
)


def test_missing_values_detected():
    df = pd.DataFrame({"email": ["a@b.com", "", "c@d.com"]})
    tester = DataQualityTester({"columns": {"email": {"type": "string", "nullable": False}}})
    result = tester.run_integrity_tests(df)
    assert any(i["test"] == "missing_values" for i in result["issues"])


def test_duplicate_detection():
    df = pd.DataFrame({"id": ["A", "A", "B"]})
    tester = DataQualityTester({"columns": {"id": {"type": "string", "unique": True}}})
    result = tester.run_integrity_tests(df)
    assert any(i["test"] == "uniqueness" for i in result["issues"])


def test_clean_data_has_no_issues():
    df = pd.DataFrame({"id": ["A", "B"], "age": [10, 20]})
    tester = DataQualityTester({"columns": {
        "id":  {"type": "string",  "unique": True, "nullable": False},
        "age": {"type": "integer", "min": 0, "max": 120},
    }})
    result = tester.run_integrity_tests(df)
    assert result["summary"]["total_issues"] == 0
    assert result["summary"]["quality_score"] == 100.0


def test_out_of_range_detected():
    df = pd.DataFrame({"age": [25, -5, 200]})
    tester = DataQualityTester({"columns": {"age": {"type": "integer", "min": 0, "max": 120}}})
    result = tester.run_integrity_tests(df)
    assert any(i["test"] == "value_range" for i in result["issues"])


def test_regex_pattern_detected():
    df = pd.DataFrame({"email": ["ok@example.com", "bad-email", "also@ok.org"]})
    tester = DataQualityTester({"columns": {
        "email": {"type": "string", "regex": r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}"},
    }})
    result = tester.run_integrity_tests(df)
    assert any(i["test"] == "pattern_match" for i in result["issues"])


def test_anomaly_tracing_finds_origin():
    inv = AnomalyInvestigator()
    anomaly = {
        "column": "email", "issue_type": "missing_values", "metric": "null_pct",
        "threshold": 1.0, "expected_baseline": 0.0,
        "stage_metrics": {
            "ingest": {"row_count": 100, "columns": {"email": {"null_pct": 0.0}}},
            "clean":  {"row_count": 100, "columns": {"email": {"null_pct": 5.0}}},
        },
    }
    pipeline = {"stages": [
        {"name": "ingest", "operation": "extract", "depends_on": []},
        {"name": "clean",  "operation": "impute",  "depends_on": ["ingest"]},
    ]}
    result = inv.trace_anomaly_source(anomaly, pipeline)
    assert result["origin_stage"] == "clean"
    assert result["confidence"] > 0.5


def test_stack_trace_parsing():
    trace = (
        "Traceback (most recent call last):\n"
        '  File "app.py", line 10, in main\n'
        "    x = 1 / 0\n"
        "ZeroDivisionError: division by zero\n"
    )
    dbg = AdvancedDebugger().analyze_stack_trace(trace)
    assert dbg["exception_type"] == "ZeroDivisionError"
    assert dbg["category"] == "division_by_zero"


def test_keyerror_classification():
    trace = (
        "Traceback (most recent call last):\n"
        '  File "etl.py", line 42, in run\n'
        '    df["missing_col"]\n'
        "KeyError: 'missing_col'\n"
    )
    dbg = AdvancedDebugger().analyze_stack_trace(trace)
    assert dbg["category"] == "missing_key_or_column"
    assert dbg["root_cause"]["line"] == 42


def test_deadlock_detection():
    log = [
        "Worker-1 - acquired lock_a",
        "Worker-2 - acquired lock_b",
        "Worker-1 - waiting for lock_b",
        "Worker-2 - waiting for lock_a",
    ]
    result = AdvancedDebugger().debug_multithreading_issues(log)
    assert result["deadlock_detected"] is True
    assert result["severity"] == "critical"
def test_deadlock_detection_worker_names():
    log = [
        "Worker-1 - acquired lock_a",
        "Worker-2 - acquired lock_b",
        "Worker-1 - waiting for lock_b",
        "Worker-2 - waiting for lock_a",
    ]
    result = AdvancedDebugger().debug_multithreading_issues(log)
    assert result["deadlock_detected"] is True

def test_no_deadlock_when_clean():
    log = [
        "T1 - acquired lock_a",
        "T1 - released lock_a",
        "T2 - acquired lock_b",
        "T2 - released lock_b",
    ]
    result = AdvancedDebugger().debug_multithreading_issues(log)
    assert result["deadlock_detected"] is False


def test_report_generation_text():
    reporter = MonitoringReporter()
    tests = {"dataset": {"name": "t", "rows": 1, "columns": ["a"]},
             "summary": {"total_issues": 0, "by_severity": {}, "quality_score": 100.0,
                         "tests_run": 1, "tests_failed": 0, "duration_seconds": 0.01},
             "issues": [], "test_results": [], "column_profiles": {}, "errors": []}
    text = reporter.generate_quality_report(tests, {"traces": []}, {})
    assert "DATA PIPELINE QUALITY" in text
    assert "100.0 / 100" in text


def test_dashboard_payload_shape():
    reporter = MonitoringReporter()
    payload = reporter.create_monitoring_dashboard_data({
        "test_results": {"dataset": {"rows": 1}, "summary": {"quality_score": 100.0, "by_severity": {}},
                         "issues": [], "test_results": []},
        "investigation_results": {"traces": []},
        "debug_results": {},
    })
    assert payload["status"] == "healthy"
    assert "metrics" in payload
    assert "alerts" in payload