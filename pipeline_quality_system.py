#!/usr/bin/env python3
"""
pipeline_quality_system.py
==========================

Data pipeline quality testing, anomaly investigation, debugging and reporting.

Components
----------
DataQualityTester    -> configurable, parallel data-integrity tests (Step 1)
AnomalyInvestigator  -> lineage analysis + tracing anomalies to pipeline stages (Step 2)
AdvancedDebugger     -> stack-trace, multithreading and log-pattern analysis (Step 3)
MonitoringReporter   -> stakeholder report + monitoring-dashboard payloads (Step 4)
main()               -> runs everything end to end on the sample data (Step 5)

Usage
-----
    python pipeline_quality_system.py
    python pipeline_quality_system.py --data my.csv --config config/quality_config.json \
        --pipeline sample_data/pipeline_scenario.json --stack-trace trace.txt --log app.log
"""

import argparse
import json
import logging
import os
import re
import sys
import threading
import time
import warnings
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("pipeline_quality")

SEVERITY_RANK = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
SEVERITY_PENALTY = {"critical": 15.0, "high": 8.0, "medium": 4.0, "low": 1.0, "info": 0.0}


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _native(obj: Any) -> Any:
    """Recursively convert numpy/pandas/datetime objects into JSON-safe python types."""
    if isinstance(obj, dict):
        return {str(k): _native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_native(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return [_native(v) for v in obj.tolist()]
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        return None if (np.isnan(f) or np.isinf(f)) else f
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if obj is pd.NaT or obj is pd.NA:
        return None
    return obj


def to_json(obj: Any, indent: int = 2) -> str:
    return json.dumps(_native(obj), indent=indent, default=str)


def load_dataframe(path: str) -> pd.DataFrame:
    """Load a CSV as strings so that type problems are preserved (not silently coerced)."""
    kwargs = dict(dtype=str, keep_default_na=False, encoding_errors="replace", on_bad_lines="skip")
    try:
        return pd.read_csv(path, **kwargs)
    except (pd.errors.ParserError, ValueError) as exc:
        logger.warning("Fast CSV parser failed for %s (%s); retrying with python engine", path, exc)
        return pd.read_csv(path, engine="python", **kwargs)


def severity_max(a: str, b: str) -> str:
    return a if SEVERITY_RANK[a] >= SEVERITY_RANK[b] else b


def _is_textual(series: pd.Series) -> bool:
    return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)


def _build_stage_graph(stages: List[Dict]) -> Tuple[List[str], Dict, Dict, Dict]:
    """Return (topological order, deps, children, stages_by_name). Raises ValueError on bad graphs."""
    by_name: Dict[str, Dict] = {}
    deps: Dict[str, List[str]] = {}
    prev = None
    for st in stages:
        name = st.get("name") or st.get("step")
        if not name:
            raise ValueError("Every pipeline stage needs a 'name'")
        if name in by_name:
            raise ValueError("Duplicate stage name: %s" % name)
        by_name[name] = st
        d = st.get("depends_on")
        if d is None:
            d = [prev] if prev else []
        elif isinstance(d, str):
            d = [d]
        deps[name] = list(d)
        prev = name
    for name, ds in deps.items():
        for d in ds:
            if d not in by_name:
                raise ValueError("Stage '%s' depends on unknown stage '%s'" % (name, d))
    children: Dict[str, List[str]] = defaultdict(list)
    indeg = {n: len(ds) for n, ds in deps.items()}
    for n, ds in deps.items():
        for d in ds:
            children[d].append(n)
    queue = deque([n for n in by_name if indeg[n] == 0])
    order: List[str] = []
    while queue:
        n = queue.popleft()
        order.append(n)
        for c in children[n]:
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)
    if len(order) != len(by_name):
        raise ValueError("Pipeline graph contains a cycle")
    return order, deps, children, by_name


# =========================================================================== #
# STEP 1 - DATA QUALITY TESTING
# =========================================================================== #
class DataQualityTester:
    """Automated data quality testing framework.

    Config (JSON file path or dict) keys:
        dataset_name, log_level, log_file, max_workers, missing_tokens,
        severity_thresholds {critical, high, medium} (percent of rows),
        check_duplicate_rows, check_outliers, outlier_iqr_multiplier, sample_size,
        required_columns [..],
        columns {name: {type, nullable, min, max, allowed_values, unique, regex, format}},
        referential_integrity [{column, reference_file|reference_values, reference_column}]
    Types: string | integer | numeric | datetime | boolean
    """

    DEFAULT_CONFIG = {
        "dataset_name": "dataset",
        "log_level": "INFO",
        "log_file": None,
        "max_workers": 4,
        "missing_tokens": ["", "na", "n/a", "nan", "null", "none", "-", "?"],
        "severity_thresholds": {"critical": 20.0, "high": 10.0, "medium": 2.0},
        "check_duplicate_rows": True,
        "check_outliers": False,
        "outlier_iqr_multiplier": 3.0,
        "sample_size": 5,
        "required_columns": [],
        "columns": {},
        "referential_integrity": [],
    }
    VALID_TYPES = {"string", "integer", "numeric", "datetime", "boolean"}
    BOOL_TOKENS = {"true", "false", "0", "1", "yes", "no", "t", "f", "y", "n"}

    def __init__(self, config_file):
        self._config_dir = os.getcwd()
        self._ref_cache: Dict[str, set] = {}
        self._ref_lock = threading.Lock()
        self.custom_tests: List[Tuple[str, Any]] = []
        self.config = self._load_config(config_file)
        self._missing_tokens = {str(t).strip().lower() for t in self.config["missing_tokens"]}
        self.setup_logging()

    # ------------------------------------------------------------------ setup
    def setup_logging(self):
        """Configure logging for quality testing (console + optional file, idempotent)."""
        level = getattr(logging, str(self.config.get("log_level", "INFO")).upper(), logging.INFO)
        logger.setLevel(level)
        fmt = logging.Formatter("%(asctime)s - %(threadName)s - %(levelname)s - %(message)s")
        if not getattr(logger, "_pq_configured", False):
            console = logging.StreamHandler(sys.stderr)
            console.setFormatter(fmt)
            logger.addHandler(console)
            logger.propagate = False
            logger._pq_configured = True  # type: ignore[attr-defined]
        log_file = self.config.get("log_file")
        if log_file and not any(getattr(h, "_pq_file", None) == log_file for h in logger.handlers):
            try:
                fh = logging.FileHandler(log_file)
                fh.setFormatter(fmt)
                fh._pq_file = log_file  # type: ignore[attr-defined]
                logger.addHandler(fh)
            except OSError as exc:
                logger.warning("Cannot open log file %s: %s", log_file, exc)

    def _load_config(self, config_file) -> dict:
        """Load, merge with defaults and validate the configuration."""
        if config_file is None:
            raw: dict = {}
        elif isinstance(config_file, dict):
            raw = dict(config_file)
        else:
            path = str(config_file)
            self._config_dir = os.path.dirname(os.path.abspath(path))
            try:
                with open(path, "r", encoding="utf-8-sig") as fh:
                    raw = json.load(fh)
            except FileNotFoundError:
                raise FileNotFoundError("Quality config not found: %s" % path)
            except json.JSONDecodeError as exc:
                raise ValueError("Quality config %s is not valid JSON: %s" % (path, exc))
        if not isinstance(raw, dict):
            raise ValueError("Quality config must be a JSON object")

        config = json.loads(json.dumps(self.DEFAULT_CONFIG))
        for key, value in raw.items():
            if key == "severity_thresholds" and isinstance(value, dict):
                config[key].update(value)
            else:
                config[key] = value

        th = config["severity_thresholds"]
        if not (th["critical"] >= th["high"] >= th["medium"] >= 0):
            raise ValueError("severity_thresholds must satisfy critical >= high >= medium >= 0")
        if not isinstance(config["columns"], dict):
            raise ValueError("'columns' must be an object mapping column -> rules")
        for col, spec in config["columns"].items():
            t = spec.get("type", "string")
            if t not in self.VALID_TYPES:
                raise ValueError("Column '%s' has invalid type '%s'" % (col, t))
            if "min" in spec and "max" in spec and t in ("integer", "numeric"):
                if float(spec["min"]) > float(spec["max"]):
                    raise ValueError("Column '%s': min > max" % col)
            if "allowed_values" in spec and not isinstance(spec["allowed_values"], list):
                raise ValueError("Column '%s': allowed_values must be a list" % col)
            if "regex" in spec:
                try:
                    re.compile(spec["regex"])
                except re.error as exc:
                    raise ValueError("Column '%s': invalid regex (%s)" % (col, exc))
        for rule in config["referential_integrity"]:
            if "column" not in rule:
                raise ValueError("Referential rule needs 'column'")
            if "reference_values" not in rule and not (
                "reference_file" in rule and "reference_column" in rule
            ):
                raise ValueError("Referential rule for '%s' needs reference_values or "
                                 "reference_file + reference_column" % rule["column"])
        config["max_workers"] = max(1, int(config["max_workers"]))
        return config

    def register_test(self, name: str, func) -> None:
        """Register a custom test: func(DataFrame) -> list of issue dicts."""
        self.custom_tests.append((name, func))

    # --------------------------------------------------------------- helpers
    def _severity_for_pct(self, pct: float) -> str:
        th = self.config["severity_thresholds"]
        if pct >= th["critical"]:
            return "critical"
        if pct >= th["high"]:
            return "high"
        if pct >= th["medium"]:
            return "medium"
        return "low"

    def _issue(self, test, column, severity, count, total, message, samples=None, rows=None) -> Dict:
        k = self.config["sample_size"]
        return {
            "test": test,
            "column": column,
            "severity": severity,
            "count": int(count),
            "total": int(total),
            "percentage": round(count / total * 100, 2) if total else 0.0,
            "message": message,
            "sample_values": [_native(v) for v in list(samples if samples is not None else [])[:k]],
            "sample_rows": [_native(r) for r in list(rows if rows is not None else [])[:k]],
        }

    @staticmethod
    def _result(test: str, column: Optional[str], issues: List[Dict]) -> Dict:
        return {"test": test, "column": column, "passed": not issues, "issues": issues}

    def _missing_mask(self, series: pd.Series) -> pd.Series:
        mask = series.isna()
        if _is_textual(series):
            txt = series.astype(str).str.strip().str.lower()
            mask = mask | txt.isin(self._missing_tokens)
        return mask

    def _coerce(self, values: pd.Series, dtype: str, spec: Dict) -> Tuple[Optional[pd.Series], pd.Series]:
        """Return (parsed values, boolean 'valid' mask) for the configured type."""
        if dtype in ("integer", "numeric"):
            num = pd.to_numeric(values, errors="coerce")
            valid = num.notna()
            if dtype == "integer":
                valid = valid & ((num % 1) == 0)
            return num, valid
        if dtype == "datetime":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                dt = pd.to_datetime(values, errors="coerce", utc=True, format=spec.get("format"))
            return dt, dt.notna()
        if dtype == "boolean":
            valid = values.astype(str).str.strip().str.lower().isin(self.BOOL_TOKENS)
            return None, valid
        return None, pd.Series(True, index=values.index)

    def _norm_keys(self, s: pd.Series) -> pd.Series:
        """Normalise keys for comparison ('1', '1.0', ' 1 ' -> '1')."""
        s = s.astype(str).str.strip()
        num = pd.to_numeric(s, errors="coerce")
        isint = num.notna() & ((num % 1) == 0)
        if isint.any():
            repl = num.where(isint).astype("Int64").astype(str)
            s = s.where(~isint, repl)
        return s

    def _reference_set(self, rule: Dict) -> set:
        key = json.dumps(rule, sort_keys=True, default=str)
        with self._ref_lock:
            if key in self._ref_cache:
                return self._ref_cache[key]
        if "reference_values" in rule:
            ref = pd.Series(rule["reference_values"], dtype=str)
        else:
            path = rule["reference_file"]
            if not os.path.isabs(path):
                path = os.path.join(self._config_dir, path)
            ref_df = load_dataframe(path)
            ref = ref_df[rule["reference_column"]]
        values = set(self._norm_keys(ref).tolist())
        with self._ref_lock:
            self._ref_cache[key] = values
        return values

    # ----------------------------------------------------------------- tests
    def _test_schema(self, data: pd.DataFrame) -> Dict:
        cols = {str(c) for c in data.columns}
        required = list(self.config["required_columns"])
        configured = list(self.config["columns"].keys())
        issues = []
        for c in required:
            if c not in cols:
                issues.append(self._issue("schema_validation", c, "critical", 1, 1,
                                          "Required column '%s' is missing from the dataset" % c))
        for c in configured:
            if c not in cols and c not in required:
                issues.append(self._issue("schema_validation", c, "high", 1, 1,
                                          "Configured column '%s' is missing from the dataset" % c))
        if configured:
            for c in sorted(cols - set(configured)):
                issues.append(self._issue("schema_validation", c, "info", 1, 1,
                                          "Column '%s' is present but not described in the config" % c))
        return {"results": [self._result("schema_validation", None, issues)]}

    def _test_duplicate_rows(self, data: pd.DataFrame) -> Dict:
        total = len(data)
        n = int(data.duplicated().sum())
        issues = []
        if n:
            pct = n / total * 100
            issues.append(self._issue("duplicate_rows", None, self._severity_for_pct(pct), n, total,
                                      "%d fully duplicated rows (%.2f%%)" % (n, pct),
                                      rows=data.index[data.duplicated()][:self.config["sample_size"]]))
        return {"results": [self._result("duplicate_rows", None, issues)]}

    def _test_referential(self, data: pd.DataFrame, rule: Dict) -> Dict:
        col = rule["column"]
        if col not in data.columns:
            return {"results": [self._result("referential_integrity", col, [self._issue(
                "referential_integrity", col, "high", 1, 1,
                "Cannot check referential integrity: column '%s' missing" % col)])]}
        ref = self._reference_set(rule)
        series = data[col]
        present = series[~self._missing_mask(series)]
        keys = self._norm_keys(present)
        orphan = ~keys.isin(ref)
        n = int(orphan.sum())
        issues = []
        if n:
            pct = n / max(len(present), 1) * 100
            target = rule.get("reference_file", "inline list") + (
                "." + rule["reference_column"] if "reference_column" in rule else "")
            issues.append(self._issue(
                "referential_integrity", col, severity_max(self._severity_for_pct(pct), "high"), n,
                len(present), "%d values in '%s' have no match in %s" % (n, col, target),
                samples=present[orphan].unique(), rows=present.index[orphan]))
        return {"results": [self._result("referential_integrity", col, issues)]}

    def _test_column(self, data: pd.DataFrame, col) -> Dict:
        name = str(col)
        spec = self.config["columns"].get(name, {})
        series = data[col]
        total = len(series)
        missing = self._missing_mask(series)
        present = series[~missing]
        n_missing = int(missing.sum())
        profile = {
            "dtype": str(series.dtype),
            "missing_count": n_missing,
            "missing_pct": round(n_missing / total * 100, 2) if total else 0.0,
            "unique_count": int(present.nunique()),
        }
        results = []

        # --- missing values
        issues = []
        if n_missing:
            pct = n_missing / total * 100
            sev = self._severity_for_pct(pct)
            if spec.get("nullable", True) is False:
                sev = severity_max(sev, "high")
            issues.append(self._issue(
                "missing_values", name, sev, n_missing, total,
                "%d missing values (%.2f%%) in '%s'%s" % (
                    n_missing, pct, name, " - column is declared non-nullable" if spec.get("nullable", True) is False else ""),
                rows=series.index[missing]))
        results.append(self._result("missing_values", name, issues))

        dtype = spec.get("type", "string")
        parsed, valid = None, None

        # --- type consistency
        if dtype != "string":
            issues = []
            parsed, valid = self._coerce(present, dtype, spec)
            bad = ~valid
            n = int(bad.sum())
            if n:
                pct = n / max(len(present), 1) * 100
                issues.append(self._issue(
                    "type_consistency", name, severity_max(self._severity_for_pct(pct), "medium"), n, len(present),
                    "%d values in '%s' are not valid %s (%.2f%%)" % (n, name, dtype, pct),
                    samples=present[bad].unique(), rows=present.index[bad]))
            results.append(self._result("type_consistency", name, issues))

        # --- value range
        if dtype in ("integer", "numeric", "datetime") and ("min" in spec or "max" in spec) and parsed is not None:
            issues = []
            good = parsed[valid]
            if dtype == "datetime":
                lo = pd.to_datetime(spec["min"], utc=True) if "min" in spec else None
                hi = pd.to_datetime(spec["max"], utc=True) if "max" in spec else None
            else:
                lo = float(spec["min"]) if "min" in spec else None
                hi = float(spec["max"]) if "max" in spec else None
            out = pd.Series(False, index=good.index)
            if lo is not None:
                out = out | (good < lo)
            if hi is not None:
                out = out | (good > hi)
            n = int(out.sum())
            if n:
                pct = n / max(len(good), 1) * 100
                issues.append(self._issue(
                    "value_range", name, self._severity_for_pct(pct), n, len(good),
                    "%d values in '%s' fall outside [%s, %s] (%.2f%%)" % (
                        n, name, spec.get("min", "-inf"), spec.get("max", "inf"), pct),
                    samples=present.loc[good.index[out]].unique(), rows=good.index[out]))
            results.append(self._result("value_range", name, issues))

        # --- allowed values
        if "allowed_values" in spec:
            issues = []
            allowed = {str(v).strip() for v in spec["allowed_values"]}
            bad = ~present.astype(str).str.strip().isin(allowed)
            n = int(bad.sum())
            if n:
                pct = n / max(len(present), 1) * 100
                issues.append(self._issue(
                    "allowed_values", name, severity_max(self._severity_for_pct(pct), "medium"), n, len(present),
                    "%d values in '%s' are not in the allowed set (%.2f%%)" % (n, name, pct),
                    samples=present[bad].unique(), rows=present.index[bad]))
            results.append(self._result("allowed_values", name, issues))

        # --- uniqueness
        if spec.get("unique"):
            issues = []
            dup = present.duplicated(keep=False)
            n = int(dup.sum())
            if n:
                pct = n / max(len(present), 1) * 100
                issues.append(self._issue(
                    "uniqueness", name, severity_max(self._severity_for_pct(pct), "high"), n, len(present),
                    "%d rows share non-unique values in '%s' (%.2f%%)" % (n, name, pct),
                    samples=present[dup].unique(), rows=present.index[dup]))
            results.append(self._result("uniqueness", name, issues))

        # --- regex pattern
        if "regex" in spec:
            issues = []
            bad = ~present.astype(str).str.fullmatch(spec["regex"])
            n = int(bad.sum())
            if n:
                pct = n / max(len(present), 1) * 100
                issues.append(self._issue(
                    "pattern_match", name, severity_max(self._severity_for_pct(pct), "medium"), n, len(present),
                    "%d values in '%s' do not match the expected pattern (%.2f%%)" % (n, name, pct),
                    samples=present[bad].unique(), rows=present.index[bad]))
            results.append(self._result("pattern_match", name, issues))

        # --- statistical outliers (optional)
        if self.config["check_outliers"] and dtype in ("integer", "numeric") and parsed is not None:
            issues = []
            good = parsed[valid]
            if len(good) > 10:
                q1, q3 = good.quantile(0.25), good.quantile(0.75)
                iqr = q3 - q1
                m = float(self.config["outlier_iqr_multiplier"])
                out = (good < q1 - m * iqr) | (good > q3 + m * iqr)
                n = int(out.sum())
                if n and iqr > 0:
                    issues.append(self._issue(
                        "statistical_outliers", name, "low", n, len(good),
                        "%d extreme outliers (>%.1f IQR) in '%s'" % (n, m, name),
                        samples=good[out].unique(), rows=good.index[out]))
            results.append(self._result("statistical_outliers", name, issues))

        return {"results": results, "profile": (name, profile)}

    # ------------------------------------------------------------------- run
    def run_integrity_tests(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Execute all configured data integrity tests (columns are tested in parallel)."""
        if not isinstance(data, pd.DataFrame):
            raise TypeError("run_integrity_tests expects a pandas DataFrame")
        start = time.perf_counter()
        logger.info("Starting integrity tests on '%s' (%d rows x %d columns)",
                    self.config["dataset_name"], len(data), data.shape[1])

        all_results: List[Dict] = []
        profiles: Dict[str, Dict] = {}
        errors: List[Dict] = []

        if len(data) == 0:
            issue = self._issue("schema_validation", None, "critical", 1, 1, "Dataset contains no rows")
            all_results.append(self._result("schema_validation", None, [issue]))
            return self._assemble(data, all_results, profiles, errors, start)

        tasks: List[Tuple[str, Any]] = [("schema_validation", lambda: self._test_schema(data))]
        if self.config["check_duplicate_rows"]:
            tasks.append(("duplicate_rows", lambda: self._test_duplicate_rows(data)))
        for col in data.columns:
            tasks.append(("column:%s" % col, (lambda c=col: self._test_column(data, c))))
        for rule in self.config["referential_integrity"]:
            tasks.append(("referential:%s" % rule["column"], (lambda r=rule: self._test_referential(data, r))))
        for cname, func in self.custom_tests:
            tasks.append(("custom:%s" % cname, (lambda n=cname, f=func: {
                "results": [self._result(n, None, list(f(data) or []))]})))

        with ThreadPoolExecutor(max_workers=self.config["max_workers"]) as pool:
            futures = {pool.submit(fn): label for label, fn in tasks}
            for fut in as_completed(futures):
                label = futures[fut]
                try:
                    out = fut.result()
                    all_results.extend(out["results"])
                    if "profile" in out:
                        profiles[out["profile"][0]] = out["profile"][1]
                except Exception as exc:  # one broken test must not kill the run
                    logger.exception("Test task %s failed", label)
                    errors.append({"task": label, "error": "%s: %s" % (type(exc).__name__, exc)})
        return self._assemble(data, all_results, profiles, errors, start)

    def _assemble(self, data, results, profiles, errors, start) -> Dict[str, Any]:
        issues = [i for r in results for i in r["issues"]]
        issues.sort(key=lambda i: (-SEVERITY_RANK[i["severity"]], -i["percentage"], str(i["column"])))
        by_sev = {s: 0 for s in ("critical", "high", "medium", "low", "info")}
        for i in issues:
            by_sev[i["severity"]] += 1
        penalty = sum(SEVERITY_PENALTY[i["severity"]] for i in issues)
        duration = time.perf_counter() - start
        results = sorted(results, key=lambda r: (r["test"], str(r["column"])))
        out = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "dataset": {"name": self.config["dataset_name"], "rows": int(len(data)),
                        "columns": [str(c) for c in data.columns]},
            "summary": {
                "total_issues": len(issues),
                "by_severity": by_sev,
                "tests_run": len(results),
                "tests_passed": sum(1 for r in results if r["passed"]),
                "tests_failed": sum(1 for r in results if not r["passed"]),
                "quality_score": round(max(0.0, 100.0 - penalty), 1),
                "duration_seconds": round(duration, 3),
            },
            "issues": issues,
            "test_results": results,
            "column_profiles": profiles,
            "errors": errors,
        }
        logger.info("Integrity tests done: %d issues, score %.1f, %.2fs",
                    len(issues), out["summary"]["quality_score"], duration)
        return out


# =========================================================================== #
# STEP 2 - ANOMALY INVESTIGATION
# =========================================================================== #
class AnomalyInvestigator:
    """Tools for tracing data quality issues to their source."""

    ISSUE_METRICS = {
        "missing_values": "null_pct",
        "type_inconsistency": "invalid_pct",
        "out_of_range": "out_of_range_pct",
        "duplicates": "duplicate_rows",
        "row_loss": "row_count",
        "schema_drift": "dtype",
        "referential_integrity": "orphan_pct",
    }
    # quality-test name -> investigator issue type
    TEST_TO_ISSUE = {
        "missing_values": "missing_values",
        "type_consistency": "type_inconsistency",
        "value_range": "out_of_range",
        "uniqueness": "duplicates",
        "duplicate_rows": "duplicates",
        "referential_integrity": "referential_integrity",
        "allowed_values": "type_inconsistency",
        "pattern_match": "type_inconsistency",
    }
    OPERATION_RISK = {
        "missing_values": ["join", "merge", "parse", "cast", "convert", "derive", "calculate", "lookup",
                           "filter", "impute", "fill", "map"],
        "type_inconsistency": ["parse", "cast", "convert", "concat", "union", "extract", "ingest", "load"],
        "out_of_range": ["calculate", "derive", "aggregate", "convert", "fx", "scale", "normalize",
                         "transform", "impute", "fill"],
        "duplicates": ["join", "merge", "union", "append", "concat", "explode", "unnest", "extract", "ingest"],
        "row_loss": ["filter", "join", "dedup", "drop", "aggregate", "sample"],
        "referential_integrity": ["join", "lookup", "delete", "filter", "drop", "merge", "load"],
        "schema_drift": ["extract", "ingest", "cast", "rename", "select", "parse"],
    }
    OPERATION_ADVICE = {
        "parse": "Validate input formats before parsing, handle parse failures explicitly (errors='coerce' + "
                 "quarantine table) instead of silently producing nulls.",
        "join": "Check join keys for nulls, type mismatches and duplicates; log match rate and row counts "
                "before/after; assert key uniqueness on the lookup side.",
        "merge": "Check merge keys for nulls and duplicates; verify cardinality (validate='m:1').",
        "cast": "Inspect values that fail the cast; add a pre-cast validation step and a reject path.",
        "convert": "Inspect values that fail conversion; log counts of coerced/failed values.",
        "filter": "Review filter predicates for NULL handling (NULL comparisons drop rows) and log rows removed.",
        "dedup": "Verify dedup keys and ordering rules; log how many rows are removed and why.",
        "aggregate": "Verify grouping keys and null-handling; compare aggregate totals against the input.",
        "impute": "Review imputation rules; imputed values may violate range and distribution expectations.",
        "fill": "Review fill/default rules; defaults can mask upstream missing data.",
        "extract": "Check the source system/export for schema or format changes; add schema validation at ingestion.",
        "load": "Validate data just before load and enforce constraints in the target schema.",
        "calculate": "Unit-test the calculation with edge cases (nulls, zero, negative, very large values).",
        "derive": "Unit-test the derivation logic with edge cases (nulls, malformed inputs).",
    }

    def __init__(self):
        self.pipeline_flow: Dict[str, Any] = {}
        self.investigation_log: List[Dict] = []

    # ------------------------------------------------------------ profiling
    @staticmethod
    def profile_stage_output(df: pd.DataFrame, stage_name: str,
                             expected_types: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Build a stage-metrics snapshot from a DataFrame (feed into trace_anomaly_source)."""
        expected_types = expected_types or {}
        n = len(df)
        cols: Dict[str, Dict] = {}
        for c in df.columns:
            s = df[c]
            miss = s.isna()
            if _is_textual(s):
                miss = miss | s.astype(str).str.strip().str.lower().isin({"", "na", "n/a", "nan", "null", "none"})
            info: Dict[str, Any] = {"null_pct": round(float(miss.mean() * 100), 3) if n else 0.0,
                                    "dtype": str(s.dtype)}
            num = pd.to_numeric(s[~miss], errors="coerce")
            if len(num) and num.notna().any():
                info["min"], info["max"] = float(num.min()), float(num.max())
            if expected_types.get(str(c)) in ("integer", "numeric") and len(num):
                info["invalid_pct"] = round(float(num.isna().mean() * 100), 3)
            cols[str(c)] = info
        return {"stage": stage_name, "row_count": int(n), "duplicate_rows": int(df.duplicated().sum()),
                "columns": cols}

    # -------------------------------------------------------------- metrics
    @staticmethod
    def _to_float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _metric_value(self, stage_metrics: Optional[Dict], metric: str, column: Optional[str]):
        if not stage_metrics:
            return None
        if metric in ("row_count", "duplicate_rows"):
            return self._to_float(stage_metrics.get(metric))
        info = (stage_metrics.get("columns") or {}).get(column) or {}
        if metric == "dtype":
            return info.get("dtype")
        return self._to_float(info.get(metric))

    @staticmethod
    def _combine(values: List, metric: str):
        if metric == "dtype":
            return values[0]
        return max(values)

    @staticmethod
    def _evaluate(metric: str, value, baseline, threshold: float, expected: float, is_source: bool):
        """Return (flagged, delta). Flagged means this stage introduced/worsened the problem."""
        if metric == "dtype":
            return (baseline is not None and value != baseline), None
        if metric == "row_count":
            if baseline is None:
                return False, None
            pct = abs(value - baseline) / baseline * 100 if baseline else (100.0 if value else 0.0)
            return pct > threshold, value - baseline
        base = baseline if baseline is not None else expected
        delta = value - base
        return delta > threshold, delta

    # ------------------------------------------------------------- tracing
    def trace_anomaly_source(self, anomaly_data: Dict, pipeline_config: Dict) -> Dict[str, Any]:
        """Trace a data-quality issue back to the pipeline stage that introduced it.

        anomaly_data: {column, issue_type, [metric], [threshold], [expected_baseline],
                       [stage_metrics: {stage: {row_count, duplicate_rows, columns: {col: {...}}}}]}
        pipeline_config: {stages: [{name, operation, depends_on, columns_modified, description}]}
        """
        inv_id = "INV-%04d" % (len(self.investigation_log) + 1)
        column = anomaly_data.get("column")
        issue = anomaly_data.get("issue_type", "missing_values")
        metric = anomaly_data.get("metric") or self.ISSUE_METRICS.get(issue, "null_pct")
        result: Dict[str, Any] = {
            "analysis_type": "anomaly_trace", "investigation_id": inv_id, "column": column,
            "issue_type": issue, "metric": metric, "origin_stage": None, "confidence": 0.0,
            "method": None, "evidence": [], "propagation_path": [], "stage_analysis": [],
            "recommendations": [],
        }
        try:
            stages = (pipeline_config or {}).get("stages") or []
            if not stages:
                raise ValueError("pipeline_config contains no stages")
            order, deps, children, by_name = _build_stage_graph(stages)
        except ValueError as exc:
            result["error"] = str(exc)
            logger.error("%s: invalid pipeline config: %s", inv_id, exc)
            self.investigation_log.append({"id": inv_id, "error": result["error"]})
            return result

        self.pipeline_flow = {"order": order, "dependencies": deps}
        metrics = anomaly_data.get("stage_metrics") or {}
        default_threshold = 0.0 if metric == "duplicate_rows" else 1.0
        threshold = float(anomaly_data.get("threshold", default_threshold))
        expected = float(anomaly_data.get("expected_baseline", 0.0))

        effective: Dict[str, Any] = {}
        candidates: List[Dict] = []
        have_metrics = False
        for stage in order:
            value = self._metric_value(metrics.get(stage), metric, column)
            dep_vals = [effective[d] for d in deps[stage] if effective.get(d) is not None]
            baseline = self._combine(dep_vals, metric) if dep_vals else None
            effective[stage] = value if value is not None else baseline
            rec = {"stage": stage, "operation": by_name[stage].get("operation"), "value": value,
                   "baseline": baseline, "delta": None, "flagged": False}
            if value is not None:
                have_metrics = True
                flagged, delta = self._evaluate(metric, value, baseline, threshold, expected, not deps[stage])
                rec["flagged"], rec["delta"] = bool(flagged), delta
                if flagged:
                    candidates.append(rec)
            result["stage_analysis"].append(rec)

        if candidates:
            origin = candidates[0]
            st = by_name[origin["stage"]]
            modifies = column in (st.get("columns_modified") or st.get("affects_columns") or [])
            conf = 0.6 + (0.2 if modifies else 0.0)
            delta = origin["delta"]
            if delta is not None and (abs(delta) >= 5 or (origin["baseline"] and abs(delta) / origin["baseline"] >= 0.5)):
                conf += 0.1
            if origin["baseline"] is None or origin["baseline"] <= threshold:
                conf += 0.1
            result.update(method="metric_comparison", origin_stage=origin["stage"],
                          confidence=round(min(conf, 0.99), 2))
            if origin["baseline"] is None:
                result["evidence"].append("%s for '%s' is already %s at source stage '%s' (expected ~%s)." % (
                    metric, column, origin["value"], origin["stage"], expected))
            else:
                prev = [d for d in deps[origin["stage"]]]
                result["evidence"].append("%s for '%s' changed from %s (upstream: %s) to %s at stage '%s'." % (
                    metric, column, origin["baseline"], ", ".join(prev) or "n/a", origin["value"], origin["stage"]))
            if modifies:
                result["evidence"].append("Stage '%s' is declared to modify column '%s'." % (origin["stage"], column))
            for extra in candidates[1:]:
                result["evidence"].append("Further change at downstream stage '%s' (%s -> %s)." % (
                    extra["stage"], extra["baseline"], extra["value"]))
            result["amplifying_stages"] = [c["stage"] for c in candidates[1:]]
        else:
            self._heuristic_origin(result, order, by_name, column, issue, have_metrics)

        origin_stage = result["origin_stage"]
        if origin_stage:
            seen, queue, path = {origin_stage}, deque([origin_stage]), [origin_stage]
            while queue:
                for c in children[queue.popleft()]:
                    if c not in seen:
                        seen.add(c)
                        queue.append(c)
                        path.append(c)
            result["propagation_path"] = path
            result["affected_downstream"] = path[1:]
            result["recommendations"] = self._stage_recommendations(by_name[origin_stage], column, issue)
        else:
            result["recommendations"] = ["Could not isolate a stage; capture per-stage metrics "
                                         "(profile_stage_output) and re-run the investigation."]
        self.investigation_log.append({"id": inv_id, "column": column, "issue": issue,
                                       "origin": origin_stage, "confidence": result["confidence"],
                                       "method": result["method"]})
        logger.info("%s: %s/%s -> origin=%s (confidence %.2f, %s)", inv_id, column, issue,
                    origin_stage, result["confidence"], result["method"])
        return result

    def _heuristic_origin(self, result, order, by_name, column, issue, have_metrics):
        risky = self.OPERATION_RISK.get(issue, [])
        scored = []
        for idx, name in enumerate(order):
            st = by_name[name]
            score = 0
            if column in (st.get("columns_modified") or st.get("affects_columns") or []):
                score += 2
            text = ("%s %s %s" % (name, st.get("operation", ""), st.get("description", ""))).lower()
            if any(k in text for k in risky):
                score += 2
            if score:
                scored.append((score, idx, name))
        if not scored:
            result["method"] = "undetermined"
            result["evidence"].append("No metrics showed a change and no stage matches the issue profile.")
            return
        score, _, name = max(scored, key=lambda x: (x[0], -x[1]))
        result.update(method="heuristic_ranking", origin_stage=name,
                      confidence=round(min(0.5, 0.2 + 0.1 * score), 2))
        result["evidence"].append(
            ("No per-stage metrics showed a jump; " if have_metrics else "No per-stage metrics supplied; ") +
            "stage '%s' ranked most likely because its operation/columns match a '%s' issue." % (name, issue))
        result["candidate_ranking"] = [{"stage": n, "score": s} for s, _, n in sorted(scored, key=lambda x: (-x[0], x[1]))]

    def _stage_recommendations(self, stage: Dict, column: str, issue: str) -> List[str]:
        name = stage.get("name") or stage.get("step")
        op = str(stage.get("operation", "")).lower()
        recs = ["Review the logic of stage '%s'%s that first introduced the '%s' problem in column '%s'." % (
            name, " (operation: %s)" % op if op else "", issue, column)]
        for key, advice in self.OPERATION_ADVICE.items():
            if key in op or key in str(name).lower():
                recs.append(advice)
                break
        recs.append("Add a data-quality gate (DataQualityTester) at the output of '%s' so regressions "
                    "are caught before they propagate downstream." % name)
        return recs

    # -------------------------------------------------------------- lineage
    def analyze_data_lineage(self, data_flow: List[Dict]) -> Dict[str, Any]:
        """Analyse data lineage: column origins, row impact, downstream blast radius.

        data_flow entry: {step, operation, input_columns, output_columns, [modified_columns],
                          [rows_in], [rows_out], [depends_on]}
        """
        out: Dict[str, Any] = {"analysis_type": "lineage", "steps": 0}
        try:
            order, deps, children, by_name = _build_stage_graph(data_flow or [])
        except ValueError as exc:
            out["error"] = str(exc)
            logger.error("Lineage analysis failed: %s", exc)
            return out

        def downstream(n: str) -> List[str]:
            seen, stack = [], list(children[n])
            while stack:
                c = stack.pop(0)
                if c not in seen:
                    seen.append(c)
                    stack.extend(children[c])
            return seen

        col_lineage: Dict[str, List[Dict]] = defaultdict(list)
        created, dropped, row_impact, rows_out_by_step = {}, {}, [], {}
        for name in order:
            st = by_name[name]
            ins, outs = set(st.get("input_columns", [])), set(st.get("output_columns", []))
            modified = set(st.get("modified_columns", [])) & outs
            created[name] = sorted(outs - ins)
            dropped[name] = sorted(ins - outs)
            for c in created[name]:
                col_lineage[c].append({"step": name, "role": "created"})
            for c in sorted(modified):
                col_lineage[c].append({"step": name, "role": "modified"})
            for c in dropped[name]:
                col_lineage[c].append({"step": name, "role": "dropped"})
            rin = st.get("rows_in")
            if rin is None and deps[name]:
                rin = max((rows_out_by_step.get(d) or 0) for d in deps[name])
            rout = st.get("rows_out", rin)
            rows_out_by_step[name] = rout
            if rin is not None and rout is not None:
                pct = round((rout - rin) / rin * 100, 2) if rin else 0.0
                row_impact.append({"step": name, "rows_in": rin, "rows_out": rout,
                                   "delta": rout - rin, "pct_change": pct, "alert": abs(pct) >= 10})

        impact = []
        for name in order:
            st = by_name[name]
            ds = downstream(name)
            touched = len(created[name]) + len(st.get("modified_columns", [])) + len(dropped[name])
            impact.append({"step": name, "operation": st.get("operation"), "downstream_steps": ds,
                           "columns_touched": touched, "score": len(ds) + touched})
        impact.sort(key=lambda x: -x["score"])
        risky = []
        for r in row_impact:
            if r["alert"]:
                risky.append({"step": r["step"], "reason": "row count changed by %.1f%% (%s -> %s)" % (
                    r["pct_change"], r["rows_in"], r["rows_out"])})
        for name in order:
            if dropped[name]:
                risky.append({"step": name, "reason": "drops columns: %s" % ", ".join(dropped[name])})

        out.update({
            "steps": len(order), "execution_order": order, "dependency_graph": deps,
            "column_lineage": dict(col_lineage), "created_columns": created, "dropped_columns": dropped,
            "row_impact": row_impact, "high_impact_steps": impact[:3], "risky_steps": risky,
            "downstream_impact": {n: downstream(n) for n in order},
        })
        self.pipeline_flow = {"order": order, "dependencies": deps}
        logger.info("Lineage analysed: %d steps, %d risky", len(order), len(risky))
        return out


# =========================================================================== #
# STEP 3 - ADVANCED DEBUGGING
# =========================================================================== #
class AdvancedDebugger:
    """Advanced debugging utilities for pipeline code."""

    FRAME_RE = re.compile(r'^\s*File "(?P<file>[^"]+)", line (?P<line>\d+)(?:, in (?P<func>.+))?\s*$')
    EXC_RE = re.compile(
        r"^(?P<type>[A-Za-z_][\w\.]*(?:Error|Exception|Exit|Interrupt|Warning|Timeout|Fault))"
        r"(?::\s*(?P<msg>.*))?$")
    CHAIN_MARKERS = ("The above exception was the direct cause", "During handling of the above exception")
    LIB_MARKERS = ("site-packages", "dist-packages", "/lib/python", "\\lib\\python", "<frozen",
                   "threading.py", "concurrent/futures", "concurrent\\futures")
    TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[,.]\d+)?")
    THREAD_RE = re.compile(
    r"\b(MainThread|ThreadPoolExecutor-\d+_\d+"
    r"|(?:Thread|Worker|Consumer|Producer|Task|Pool|T|W)[-_]?\d+(?:_\d+)?)\b"
)
    THREAD_FALLBACK_RE = re.compile(r"(?i)\bthread[=:\s]+['\"]?(\w[\w\-]*)")
    LOCK_ACTION_RE = re.compile(
        r"(?i)\b(acquired|acquiring|attempting to acquire|trying to acquire|waiting for|waiting on|"
        r"requesting|released|releasing)\b\s+(?:the\s+)?(?:lock\s+)?['\"]?([\w\.\-]+)['\"]?")
    LEVEL_RE = re.compile(r"\b(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)\b")
    RACE_RE = re.compile(r"(?i)race condition|data race|lost update|inconsistent state|corrupt")
    MISMATCH_RE = re.compile(r"(?i)expected\W+(\d+)\W+(?:but\W+)?(?:got|actual|was|found)\W+(\d+)")

    def __init__(self):
        self.debug_sessions: List[Dict] = []
        self.resolution_patterns: Dict[str, Dict] = {
            r"changed size during iteration|modified during iteration": {
                "category": "concurrency_shared_state", "severity": "high",
                "explanation": "A shared collection was modified while another loop/thread was iterating over it.",
                "recommendations": [
                    "Protect shared collections with a threading.Lock, or iterate over a snapshot (list(d.items())).",
                    "Prefer queue.Queue or per-worker data merged after join() instead of shared mutable state."]},
            r"KeyError": {
                "category": "missing_key_or_column", "severity": "medium",
                "explanation": "A dictionary key or DataFrame column that the code expects does not exist.",
                "recommendations": [
                    "Validate the input schema at the start of the stage (required columns present) and fail fast with a clear message.",
                    "Use .get() / `if key in df.columns` where the field is optional."]},
            r"could not convert string to float|invalid literal for int|Unable to parse|cannot convert float NaN|"
            r"Cannot convert non-finite": {
                "category": "data_type_conversion", "severity": "medium",
                "explanation": "A value could not be converted to the required numeric type - usually dirty source data.",
                "recommendations": [
                    "Use pd.to_numeric(..., errors='coerce') and route rows that become NaN to a reject/quarantine table.",
                    "Add a type-consistency quality test (DataQualityTester) before this stage."]},
            r"Length of values|shape|broadcast|cannot reindex|Columns must be same length": {
                "category": "shape_mismatch", "severity": "medium",
                "explanation": "Array/Series lengths or shapes do not line up (often after a join/filter changed row counts).",
                "recommendations": [
                    "Assert row counts before and after joins/filters; align on index or key rather than position.",
                    "Log df.shape before the failing operation."]},
            r"AttributeError.*NoneType|'NoneType' object": {
                "category": "none_value", "severity": "medium",
                "explanation": "A function returned None (or a lookup failed) and the result was used without a check.",
                "recommendations": [
                    "Check the return value for None before use; make the upstream function raise explicitly instead of returning None.",
                    "Note: in-place pandas methods (sort_values(inplace=True), etc.) return None."]},
            r"TypeError": {
                "category": "type_mismatch", "severity": "medium",
                "explanation": "An operation received an incompatible type (e.g. None + int, str < int).",
                "recommendations": [
                    "Normalise column dtypes at stage input (astype / to_numeric) and handle nulls explicitly.",
                    "Add type annotations and a runtime type check at the stage boundary."]},
            r"ZeroDivisionError|division by zero": {
                "category": "division_by_zero", "severity": "medium",
                "explanation": "A denominator evaluated to zero.",
                "recommendations": ["Guard the division (if denom == 0) or use numpy with errstate and replace inf with NaN.",
                                    "Add a value-range test for the denominator column."]},
            r"FileNotFoundError|No such file": {
                "category": "missing_file", "severity": "high",
                "explanation": "An input/output path does not exist at run time.",
                "recommendations": ["Verify upstream job completion before starting this stage (dependency/sensor check).",
                                    "Use absolute or config-driven paths and log the resolved path."]},
            r"PermissionError": {
                "category": "permissions", "severity": "high",
                "explanation": "The process lacks permission for the file/resource.",
                "recommendations": ["Check file ownership/ACLs for the service account running the pipeline."]},
            r"MemoryError|Unable to allocate": {
                "category": "memory_exhaustion", "severity": "high",
                "explanation": "The process ran out of memory while loading or transforming data.",
                "recommendations": ["Process in chunks (pd.read_csv(chunksize=...)) or use categorical/smaller dtypes.",
                                    "Reduce worker count so concurrent tasks do not each hold a full copy of the data."]},
            r"IndexError": {
                "category": "index_out_of_range", "severity": "medium",
                "explanation": "A positional index was outside the bounds of a list/array/row set.",
                "recommendations": ["Check length before indexing; handle empty results explicitly (empty DataFrame/list)."]},
            r"ModuleNotFoundError|ImportError": {
                "category": "dependency_missing", "severity": "high",
                "explanation": "A required package or module cannot be imported.",
                "recommendations": ["Pin and install dependencies (requirements.txt) in the runtime environment."]},
            r"RecursionError|maximum recursion depth": {
                "category": "runaway_recursion", "severity": "high",
                "explanation": "Recursion exceeded the interpreter limit, usually a missing/incorrect base case.",
                "recommendations": ["Fix the base case or rewrite the algorithm iteratively."]},
            r"UnicodeDecodeError|ParserError|Error tokenizing": {
                "category": "encoding_or_parse", "severity": "medium",
                "explanation": "The input file has encoding problems or malformed rows.",
                "recommendations": ["Read with explicit encoding (encoding_errors='replace') and on_bad_lines='skip'/'warn'; "
                                    "log the rejected lines."]},
            r"TimeoutError|timed out": {
                "category": "timeout", "severity": "high",
                "explanation": "An operation exceeded its time limit - possible deadlock, slow dependency or lock contention.",
                "recommendations": ["Add timeouts + retries with backoff for external calls; check the log for lock waits."]},
            r"ConnectionError|OperationalError|Connection refused|ConnectionResetError": {
                "category": "connectivity", "severity": "high",
                "explanation": "The pipeline could not reach a database/service.",
                "recommendations": ["Add retry with exponential backoff and a circuit breaker; verify the service is up."]},
            r"RuntimeError.*(thread|lock)|can't start new thread|cannot join": {
                "category": "threading_error", "severity": "high",
                "explanation": "A threading primitive was misused or resources for threads were exhausted.",
                "recommendations": ["Use ThreadPoolExecutor with a bounded max_workers and `with lock:` blocks."]},
        }

    # ----------------------------------------------------------- stack trace
    def _is_user_code(self, path: str) -> bool:
        return not any(m in path for m in self.LIB_MARKERS)

    def _parse_segment(self, lines: List[str]) -> Dict[str, Any]:
        frames: List[Dict] = []
        exc_type, msg = None, ""
        i = 0
        while i < len(lines):
            line = lines[i]
            m = self.FRAME_RE.match(line)
            if m:
                code = ""
                if i + 1 < len(lines):
                    nxt = lines[i + 1].strip()
                    if nxt and not self.FRAME_RE.match(lines[i + 1]) and not set(nxt) <= set("^~ "):
                        code = nxt
                        i += 1
                frames.append({"file": m.group("file"), "line": int(m.group("line")),
                               "function": (m.group("func") or "<unknown>").strip(), "code": code,
                               "is_user_code": self._is_user_code(m.group("file"))})
            elif line and not line[0].isspace() and not line.startswith("Traceback"):
                em = self.EXC_RE.match(line.strip())
                if em:
                    exc_type, msg = em.group("type"), (em.group("msg") or "").strip()
            i += 1
        return {"frames": frames, "exception_type": exc_type, "message": msg}

    def analyze_stack_trace(self, stack_trace: str) -> Dict[str, Any]:
        """Parse a Python traceback and identify root cause, location and likely fix."""
        result: Dict[str, Any] = {"analysis_type": "stack_trace"}
        if not isinstance(stack_trace, str) or not stack_trace.strip():
            result["error"] = "Empty or non-text stack trace"
            return result

        segments: List[List[str]] = [[]]
        for line in stack_trace.splitlines():
            if any(mk in line for mk in self.CHAIN_MARKERS):
                segments.append([])
            else:
                segments[-1].append(line.rstrip("\r"))
        parsed = [p for p in (self._parse_segment(s) for s in segments) if p["exception_type"] or p["frames"]]
        if not parsed:
            result["error"] = "No Python traceback structure found"
            return result

        root, final = parsed[0], parsed[-1]
        exc_type = root["exception_type"] or final["exception_type"] or "UnknownException"
        message = root["message"] or final["message"]
        frames = root["frames"] or final["frames"]
        user_frames = [f for f in frames if f["is_user_code"]]
        root_frame = user_frames[-1] if user_frames else (frames[-1] if frames else None)

        probe = "%s: %s" % (exc_type, message)
        category, severity, explanation, hints = "unclassified", "medium", \
            "The failure does not match a known pattern; inspect the root-cause frame.", \
            ["Reproduce locally with the failing input and add logging around the root-cause line."]
        for pattern, info in self.resolution_patterns.items():
            if re.search(pattern, probe, re.IGNORECASE):
                category, severity, explanation = info["category"], info["severity"], info["explanation"]
                hints = list(info["recommendations"])
                break

        specifics = self._specific_hints(category, exc_type, message, root_frame)
        full_text = stack_trace
        multithreaded = bool(re.search(r"threading\.py|concurrent[/\\]futures|Exception in thread|Thread-\d+|"
                                       r"ThreadPoolExecutor", full_text))
        thread_name = None
        tm = re.search(r"Exception in thread (\S+)", full_text) or re.search(r"(Thread-\d+|ThreadPoolExecutor-\d+_\d+)", full_text)
        if tm:
            thread_name = tm.group(1)
        if multithreaded:
            if re.search(r"concurrent[/\\]futures", full_text):
                specifics.append("The error surfaced through concurrent.futures: the real failure happened inside the worker "
                                 "function. Always call future.result() (or iterate as_completed) so worker exceptions are "
                                 "not silently dropped, and log the item being processed.")
            if category in ("concurrency_shared_state", "threading_error", "none_value", "type_mismatch"):
                specifics.append("Because this ran in a worker thread, check for shared mutable state accessed without a lock.")

        result.update({
            "exception_type": exc_type, "message": message, "category": category, "severity": severity,
            "explanation": explanation,
            "root_cause": ({k: root_frame[k] for k in ("file", "line", "function", "code", "is_user_code")}
                           if root_frame else None),
            "call_chain": ["%s:%d (%s)" % (f["file"], f["line"], f["function"]) for f in frames],
            "chained_exceptions": [{"type": p["exception_type"], "message": p["message"]} for p in parsed],
            "is_multithreaded": multithreaded, "thread_name": thread_name,
            "hints": specifics + hints,
        })
        result["recommendations"] = self.generate_resolution_recommendations(result)
        self.debug_sessions.append({"type": "stack_trace", "exception": exc_type, "category": category,
                                    "time": datetime.now().isoformat(timespec="seconds")})
        logger.info("Stack trace analysed: %s (%s) at %s", exc_type, category,
                    "%s:%s" % (root_frame["file"], root_frame["line"]) if root_frame else "unknown location")
        return result

    @staticmethod
    def _specific_hints(category, exc_type, message, frame) -> List[str]:
        loc = ""
        if frame:
            loc = " at %s:%d in %s()" % (os.path.basename(frame["file"]), frame["line"], frame["function"])
        hints = []
        if category == "missing_key_or_column":
            key = message.strip("'\" ")
            hints.append("Key/column %r was not found%s. Log list(df.columns) / data.keys() just before this line to see "
                         "what upstream actually delivered; the column may have been renamed or dropped." % (key, loc))
        elif category == "data_type_conversion":
            m = re.search(r":\s*['\"]?([^'\"]+)['\"]?\s*$", message)
            bad = m.group(1) if m else message
            hints.append("Offending value %r%s. Find similar rows with a type-consistency test and clean or quarantine them." % (bad, loc))
        elif category == "none_value":
            hints.append("A None value was dereferenced%s. Trace which call returned None (look for missing return statements)." % loc)
        elif category == "missing_file":
            hints.append("Missing path: %s%s." % (message, loc))
        return hints

    # ---------------------------------------------------------- multithreading
    @staticmethod
    def _parse_ts(text: Optional[str]) -> Optional[datetime]:
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace(",", ".").replace("T", " "))
        except ValueError:
            return None

    @staticmethod
    def _has_lock_word(line: str) -> bool:
        for tok in re.findall(r"\w+", line.lower()):
            if tok in ("block", "blocked", "blocking", "deadlock", "deadlocked", "clock", "unblocked"):
                continue
            if "lock" in tok or "mutex" in tok or "semaphore" in tok:
                return True
        return False

    def _thread_of(self, line: str) -> Optional[str]:
        m = self.THREAD_RE.search(line) or self.THREAD_FALLBACK_RE.search(line)
        return m.group(1) if m else None

    def debug_multithreading_issues(self, log_data: List[str]) -> Dict[str, Any]:
        """Analyse concurrent-execution logs for deadlocks, lock-order inversions, races and contention."""
        if isinstance(log_data, str):
            log_data = log_data.splitlines()
        log_data = [l for l in (log_data or []) if isinstance(l, str)]
        held: Dict[str, List[str]] = defaultdict(list)
        holder: Dict[str, str] = {}
        waiting: Dict[str, Tuple[str, Optional[datetime], int]] = {}
        order_edges: Dict[Tuple[str, str], set] = defaultdict(set)
        threads: List[str] = []
        finished: set = set()
        deadlocks: List[Dict] = []
        seen_cycles: set = set()
        contention, races, thread_errors, timeouts, anomalies = [], [], [], [], []
        events = 0

        for idx, line in enumerate(log_data):
            thread = self._thread_of(line)
            ts = self._parse_ts((self.TS_RE.search(line) or [None])[0] if self.TS_RE.search(line) else None)
            if thread and thread not in threads:
                threads.append(thread)
            snippet = line.strip()[:200]

            if self.RACE_RE.search(line):
                races.append({"line": idx + 1, "thread": thread, "text": snippet})
            else:
                mm = self.MISMATCH_RE.search(line)
                if mm and mm.group(1) != mm.group(2):
                    races.append({"line": idx + 1, "thread": thread, "text": snippet,
                                  "expected": int(mm.group(1)), "actual": int(mm.group(2))})
            if re.search(r"(?i)time[d ]?-?out", line):
                timeouts.append({"line": idx + 1, "thread": thread, "text": snippet})
            lv = self.LEVEL_RE.search(line)
            if thread and ((lv and lv.group(1) in ("ERROR", "CRITICAL", "FATAL")) or "Traceback" in line
                           or "Exception in thread" in line):
                thread_errors.append({"line": idx + 1, "thread": thread, "text": snippet})
            if thread and re.search(r"(?i)\b(finished|exiting|exited|terminated|completed)\b", line):
                finished.add(thread)

            if not (thread and self._has_lock_word(line)):
                continue
            am = self.LOCK_ACTION_RE.search(line)
            if not am:
                continue
            verb, lock = am.group(1).lower(), am.group(2)
            events += 1
            if verb in ("acquired",):
                w = waiting.pop(thread, None)
                if w and w[0] == lock and w[1] and ts:
                    dur = (ts - w[1]).total_seconds()
                    if dur >= 2.0:
                        contention.append({"thread": thread, "lock": lock, "wait_seconds": round(dur, 3),
                                           "line": idx + 1})
                for h in held[thread]:
                    if h != lock:
                        order_edges[(h, lock)].add(thread)
                if lock in holder and holder[lock] != thread:
                    anomalies.append("Line %d: %s acquired '%s' while %s still held it (not a real lock?)" % (
                        idx + 1, thread, lock, holder[lock]))
                holder[lock] = thread
                if lock not in held[thread]:
                    held[thread].append(lock)
            elif verb in ("released", "releasing"):
                if lock in held[thread]:
                    held[thread].remove(lock)
                    if holder.get(lock) == thread:
                        del holder[lock]
                else:
                    anomalies.append("Line %d: %s released '%s' which it did not hold" % (idx + 1, thread, lock))
            else:  # waiting / requesting
                waiting[thread] = (lock, ts, idx)
                for h in held[thread]:
                    if h != lock:
                        order_edges[(h, lock)].add(thread)
                path, node = [thread], thread
                while True:
                    w = waiting.get(node)
                    nxt = holder.get(w[0]) if w else None
                    if not nxt or nxt == node:
                        break
                    if nxt in path:
                        cycle = path[path.index(nxt):]
                        key = frozenset(cycle)
                        if key not in seen_cycles:
                            seen_cycles.add(key)
                            deadlocks.append({
                                "threads": cycle,
                                "waiting_for": {t: waiting[t][0] for t in cycle},
                                "holding": {t: list(held[t]) for t in cycle},
                                "detected_at_line": idx + 1,
                                "description": "; ".join(
                                    "%s holds %s and waits for %s" % (t, ", ".join(held[t]) or "nothing", waiting[t][0])
                                    for t in cycle),
                            })
                        break
                    path.append(nxt)
                    node = nxt

        inversions = []
        for (a, b), ts_ab in order_edges.items():
            if a < b and (b, a) in order_edges:
                ts_ba = order_edges[(b, a)]
                if any(x != y for x in ts_ab for y in ts_ba):
                    inversions.append({"locks": [a, b], "order_a_then_b": sorted(ts_ab),
                                       "order_b_then_a": sorted(ts_ba)})
        in_cycle = {t for d in deadlocks for t in d["threads"]}
        blocked = [{"thread": t, "waiting_for": w[0], "holder": holder.get(w[0]), "line": w[2] + 1}
                   for t, w in waiting.items() if t not in in_cycle]
        leaked = [{"thread": t, "locks": list(l)} for t, l in held.items() if l and t in finished]

        if deadlocks:
            severity = "critical"
        elif inversions or races or leaked:
            severity = "high"
        elif blocked or contention or thread_errors or timeouts:
            severity = "medium"
        else:
            severity = "low"

        if deadlocks:
            root_cause = "Circular wait (deadlock): " + " | ".join(d["description"] for d in deadlocks)
        elif inversions:
            root_cause = "Inconsistent lock ordering between threads can deadlock under unlucky timing"
        elif races:
            root_cause = "Shared state appears to be updated without proper synchronisation"
        elif blocked:
            root_cause = "Threads are left waiting for locks that are never released"
        else:
            root_cause = "No definite concurrency fault found in the supplied logs"

        result = {
            "analysis_type": "multithreading", "lines_analyzed": len(log_data), "lock_events": events,
            "threads_observed": threads, "deadlock_detected": bool(deadlocks), "deadlocks": deadlocks,
            "lock_order_inversions": inversions, "race_conditions": races, "lock_contention": contention,
            "blocked_threads": blocked, "unreleased_locks": leaked, "thread_errors": thread_errors[:20],
            "timeouts": timeouts[:20], "anomalies": anomalies, "severity": severity, "root_cause": root_cause,
        }
        result["recommendations"] = self.generate_resolution_recommendations(result)
        self.debug_sessions.append({"type": "multithreading", "severity": severity,
                                    "time": datetime.now().isoformat(timespec="seconds")})
        logger.info("Multithreading analysis: severity=%s deadlocks=%d inversions=%d races=%d",
                    severity, len(deadlocks), len(inversions), len(races))
        return result

    # ------------------------------------------------------------- log patterns
    def analyze_log_patterns(self, log_lines: List[str]) -> Dict[str, Any]:
        """Extract level counts, recurring error signatures, time span and notable anomalies from logs."""
        if isinstance(log_lines, str):
            log_lines = log_lines.splitlines()
        lines = [l for l in (log_lines or []) if isinstance(l, str) and l.strip()]
        levels: Counter = Counter()
        signatures: Counter = Counter()
        stamps: List[datetime] = []
        per_minute: Counter = Counter()
        retries = 0
        for line in lines:
            lv = self.LEVEL_RE.search(line)
            level = {"WARN": "WARNING", "FATAL": "CRITICAL"}.get(lv.group(1), lv.group(1)) if lv else "UNKNOWN"
            levels[level] += 1
            ts = self._parse_ts((self.TS_RE.search(line) or [None])[0] if self.TS_RE.search(line) else None)
            if ts:
                stamps.append(ts)
            if re.search(r"(?i)\bretry|retrying\b", line):
                retries += 1
            if level in ("ERROR", "CRITICAL", "WARNING"):
                sig = self.TS_RE.sub("", line)
                sig = self.THREAD_RE.sub("<thread>", sig)
                sig = re.sub(r"0x[0-9a-fA-F]+|\b\d+(\.\d+)?\b", "<n>", sig)
                sig = re.sub(r"\s+", " ", sig).strip(" -")
                signatures[(level, sig[:160])] += 1
                if ts and level != "WARNING":
                    per_minute[ts.strftime("%Y-%m-%d %H:%M")] += 1
        total = len(lines)
        err = levels["ERROR"] + levels["CRITICAL"]
        found: List[str] = []
        if total and err / total > 0.2:
            found.append("High error ratio: %.0f%% of log lines are ERROR/CRITICAL." % (err / total * 100))
        for (lvl, sig), n in signatures.most_common(3):
            if n >= 5:
                found.append("Repeated %s signature (%dx): %s" % (lvl, n, sig))
        if retries >= 5:
            found.append("%d retry attempts logged - a dependency is probably flaky or down." % retries)
        if per_minute:
            minute, n = per_minute.most_common(1)[0]
            if n >= 5 and n > 3 * (sum(per_minute.values()) / len(per_minute)):
                found.append("Error burst: %d errors during %s." % (n, minute))
        result = {
            "analysis_type": "log_patterns", "total_lines": total, "level_counts": dict(levels),
            "first_timestamp": min(stamps).isoformat() if stamps else None,
            "last_timestamp": max(stamps).isoformat() if stamps else None,
            "top_error_signatures": [{"level": l, "signature": s, "count": n}
                                     for (l, s), n in signatures.most_common(5)],
            "retry_count": retries, "findings": found, "severity": "high" if found else "low",
        }
        result["recommendations"] = self.generate_resolution_recommendations(result)
        return result

    # ----------------------------------------------------------- recommendations
    def _collect(self, debug_results: Any) -> List[Dict]:
        if isinstance(debug_results, dict):
            if "analysis_type" in debug_results:
                return [debug_results]
            out: List[Dict] = []
            for v in debug_results.values():
                out.extend(self._collect(v))
            return out
        if isinstance(debug_results, list):
            out = []
            for v in debug_results:
                out.extend(self._collect(v))
            return out
        return []

    def generate_resolution_recommendations(self, debug_results: Dict) -> List[str]:
        """Turn analysis results (single or combined) into prioritised, actionable recommendations."""
        ranked: List[Tuple[int, str]] = []

        def add(sev: str, text: str):
            ranked.append((SEVERITY_RANK.get(sev, 1), "[%s] %s" % (sev.upper(), text)))

        for item in self._collect(debug_results):
            kind = item.get("analysis_type")
            if kind == "stack_trace" and not item.get("error"):
                sev = item.get("severity", "medium")
                rc = item.get("root_cause")
                if rc:
                    add(sev, "Fix the %s at %s:%d in %s() -> `%s`." % (
                        item["exception_type"], os.path.basename(rc["file"]), rc["line"], rc["function"],
                        rc["code"] or "see source"))
                for h in item.get("hints", []):
                    add(sev, h)
            elif kind == "multithreading":
                for d in item.get("deadlocks", []):
                    ts = d["threads"]
                    locks = sorted(set(d["waiting_for"].values()) | {l for v in d["holding"].values() for l in v})
                    add("critical", "Deadlock between %s over locks %s. Enforce one global lock-acquisition order "
                        "(e.g. always acquire %s)." % (", ".join(ts), ", ".join(locks), " before ".join(locks)))
                    add("critical", "Use lock.acquire(timeout=...) and back off/retry (or abort) instead of blocking forever; "
                        "or merge the locks into one, or avoid holding one lock while requesting another.")
                for inv in item.get("lock_order_inversions", []):
                    add("high", "Lock order inversion on %s: %s acquire them in opposite orders. Standardise the order." % (
                        " & ".join(inv["locks"]), " / ".join(inv["order_a_then_b"] + inv["order_b_then_a"])))
                if item.get("race_conditions"):
                    add("high", "Possible race condition (%d log hits). Guard shared counters/collections with a Lock, "
                        "use queue.Queue, or make workers return results that the main thread aggregates." %
                        len(item["race_conditions"]))
                for lk in item.get("unreleased_locks", []):
                    add("high", "%s finished while holding %s. Always use `with lock:` so locks release on errors." % (
                        lk["thread"], ", ".join(lk["locks"])))
                for b in item.get("blocked_threads", []):
                    add("medium", "%s is blocked on '%s' (held by %s). Verify the holder releases it in a finally/with block." % (
                        b["thread"], b["waiting_for"], b["holder"] or "unknown"))
                for c in item.get("lock_contention", [])[:3]:
                    add("medium", "%s waited %.1fs for '%s'. Shorten critical sections or shard the lock." % (
                        c["thread"], c["wait_seconds"], c["lock"]))
                if item.get("thread_errors"):
                    add("medium", "%d error lines were logged from worker threads; call future.result() to surface "
                        "worker exceptions." % len(item["thread_errors"]))
            elif kind == "log_patterns":
                for f in item.get("findings", []):
                    add("high", f)

        ranked.sort(key=lambda x: -x[0])
        seen, out = set(), []
        for _, text in ranked:
            if text not in seen:
                seen.add(text)
                out.append(text)
        return out


# =========================================================================== #
# STEP 4 - REPORTING AND MONITORING
# =========================================================================== #
class MonitoringReporter:
    """Generate reports and monitoring interfaces."""

    TEST_LABELS = {
        "missing_values": "Missing information",
        "type_consistency": "Values in the wrong format",
        "value_range": "Values outside the allowed range",
        "allowed_values": "Unexpected category values",
        "uniqueness": "Duplicate identifiers",
        "pattern_match": "Malformed values",
        "referential_integrity": "References to records that do not exist",
        "duplicate_rows": "Duplicate records",
        "schema_validation": "Missing or unexpected columns",
        "statistical_outliers": "Statistical outliers",
    }
    TEST_ACTIONS = {
        "missing_values": "Find out why values are missing (upstream feed vs. pipeline step) and backfill or reject those rows.",
        "type_consistency": "Clean or quarantine malformed values and enforce the data type at ingestion.",
        "value_range": "Review out-of-range records (data entry errors, unit mix-ups) and add validation at the source.",
        "allowed_values": "Map or reject unexpected category values; keep the allowed list in sync with the source system.",
        "uniqueness": "De-duplicate on the key and find what created the duplicates (usually a join or repeated load).",
        "pattern_match": "Fix or reject values that don't match the expected format.",
        "referential_integrity": "Load missing reference records or reject orphan rows before they reach reports.",
        "duplicate_rows": "Remove duplicate rows and make loads idempotent.",
        "schema_validation": "Restore missing columns or update the config if the schema intentionally changed.",
        "statistical_outliers": "Verify the extreme values are genuine.",
    }

    def __init__(self):
        self.report_templates = {
            "status_text": {"good": "GOOD - data is fit for use", "attention": "NEEDS ATTENTION - fix before relying on this data",
                            "at_risk": "AT RISK - do not rely on this data until critical issues are resolved"},
        }

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _normalise(investigation_results: Any) -> Dict:
        inv = investigation_results or {}
        if isinstance(inv, dict) and "origin_stage" in inv:
            inv = {"traces": [inv]}
        if isinstance(inv, list):
            inv = {"traces": inv}
        inv.setdefault("traces", [])
        return inv

    def _health(self, score: float, by_sev: Dict[str, int], extra_critical: bool = False) -> str:
        if by_sev.get("critical", 0) or extra_critical or score < 60:
            return "at_risk"
        if score < 85 or by_sev.get("high", 0):
            return "attention"
        return "good"

    def _recommendations(self, tests, inv, dbg) -> List[str]:
        ranked: List[Tuple[int, str]] = []
        seen_tests = set()
        for i in (tests or {}).get("issues", []):
            if i["test"] not in seen_tests and i["severity"] != "info":
                seen_tests.add(i["test"])
                action = self.TEST_ACTIONS.get(i["test"], "Investigate the failing check.")
                ranked.append((SEVERITY_RANK[i["severity"]], "[%s] %s (e.g. column '%s')" % (
                    i["severity"].upper(), action, i["column"] or "dataset")))
        for t in inv.get("traces", []):
            if t.get("origin_stage"):
                for r in t.get("recommendations", [])[:2]:
                    ranked.append((3, "[HIGH] %s" % r))
        for r in (dbg or {}).get("recommendations", []) or AdvancedDebugger()._collect and []:
            m = re.match(r"\[(\w+)\]", r)
            ranked.append((SEVERITY_RANK.get(m.group(1).lower(), 1) if m else 1, r))
        ranked.sort(key=lambda x: -x[0])
        out, seen = [], set()
        for _, text in ranked:
            if text not in seen:
                seen.add(text)
                out.append(text)
        return out

    # ----------------------------------------------------------------- report
    def generate_quality_report(self, test_results: Dict, investigation_results: Dict, debug_results: Dict,
                                output_format: str = "text") -> str:
        """Generate the comprehensive quality + debugging report ('text' or 'json')."""
        test_results = test_results or {}
        dbg = debug_results or {}
        inv = self._normalise(investigation_results)
        summary = test_results.get("summary", {})
        by_sev = summary.get("by_severity", {})
        score = summary.get("quality_score", 100.0)
        deadlock = bool((dbg.get("multithreading") or {}).get("deadlock_detected"))
        status = self._health(score, by_sev, deadlock)
        recs = self._recommendations(test_results, inv, dbg)

        if output_format == "json":
            return to_json({
                "report_metadata": {"generated_at": datetime.now().isoformat(timespec="seconds"),
                                    "status": status, "quality_score": score},
                "executive_summary": {"status": self.report_templates["status_text"][status],
                                      "rows_checked": (test_results.get("dataset") or {}).get("rows"),
                                      "total_issues": summary.get("total_issues", 0), "issues_by_severity": by_sev},
                "quality_issues": test_results.get("issues", []),
                "column_profiles": test_results.get("column_profiles", {}),
                "investigation": inv, "debugging": dbg, "recommendations": recs,
                "test_errors": test_results.get("errors", []),
            })

        L: List[str] = []
        bar = "=" * 78
        ds = test_results.get("dataset", {})
        L += [bar, "DATA PIPELINE QUALITY & DEBUGGING REPORT", bar,
              "Generated : %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
              "Dataset   : %s (%s rows, %s columns)" % (ds.get("name", "n/a"), ds.get("rows", "n/a"), len(ds.get("columns", []))),
              ""]
        L += ["1. EXECUTIVE SUMMARY", "-" * 78,
              "Overall status : %s" % self.report_templates["status_text"][status],
              "Quality score  : %.1f / 100" % score,
              "Issues found   : %d  (critical %d, high %d, medium %d, low %d)" % (
                  summary.get("total_issues", 0), by_sev.get("critical", 0), by_sev.get("high", 0),
                  by_sev.get("medium", 0), by_sev.get("low", 0))]
        if inv["traces"]:
            origins = sorted({t["origin_stage"] for t in inv["traces"] if t.get("origin_stage")})
            if origins:
                L.append("Problem origin : data issues were introduced in pipeline stage(s): %s" % ", ".join(origins))
        if deadlock:
            L.append("Concurrency    : a DEADLOCK was found in the pipeline's worker threads (jobs will hang).")
        L += ["", "What this means: " + {
            "good": "The data passed its checks and can be used with normal confidence.",
            "attention": "Most data is usable, but some records are faulty. Reports built on it may be slightly off.",
            "at_risk": "A significant share of the data is faulty or the pipeline can hang. Decisions based on this data may be wrong.",
        }[status], ""]

        L += ["2. KEY DATA QUALITY FINDINGS", "-" * 78]
        issues = [i for i in test_results.get("issues", []) if i["severity"] != "info"]
        if not issues:
            L.append("No data quality issues were detected.")
        for i in issues[:15]:
            label = self.TEST_LABELS.get(i["test"], i["test"])
            L.append("[%-8s] %s - %s" % (i["severity"].upper(), label, i["message"]))
            if i["sample_values"]:
                L.append("             examples: %s" % ", ".join(repr(v) for v in i["sample_values"]))
        if len(issues) > 15:
            L.append("... and %d more (see the JSON report)." % (len(issues) - 15))
        for e in test_results.get("errors", []):
            L.append("[WARNING ] A check could not run: %s (%s)" % (e["task"], e["error"]))
        L.append("")

        L += ["3. ROOT-CAUSE INVESTIGATION", "-" * 78]
        if not inv["traces"]:
            L.append("No investigations were run.")
        for t in inv["traces"]:
            if t.get("error"):
                L.append("%s: could not be completed - %s" % (t.get("investigation_id"), t["error"]))
                continue
            L.append("%s  column '%s' / %s" % (t["investigation_id"], t["column"], t["issue_type"]))
            L.append("   Likely origin : %s (confidence %.0f%%, %s)" % (
                t["origin_stage"] or "undetermined", t["confidence"] * 100, t["method"]))
            for ev in t["evidence"][:3]:
                L.append("   Evidence      : %s" % ev)
            if t.get("affected_downstream"):
                L.append("   Also affected : %s" % ", ".join(t["affected_downstream"]))
        lin = inv.get("lineage")
        if lin and not lin.get("error"):
            L.append("")
            L.append("Lineage: %d steps analysed; %d risky step(s)." % (lin["steps"], len(lin["risky_steps"])))
            for r in lin["risky_steps"][:5]:
                L.append("   - %s: %s" % (r["step"], r["reason"]))
        L.append("")

        L += ["4. DEBUGGING FINDINGS", "-" * 78]
        traces = dbg.get("stack_traces", [])
        if not traces and not dbg.get("multithreading") and not dbg.get("log_patterns"):
            L.append("No debugging inputs were provided.")
        for s in traces:
            if s.get("error"):
                L.append("Stack trace could not be analysed: %s" % s["error"])
                continue
            rc = s.get("root_cause") or {}
            L.append("Failure: %s: %s" % (s["exception_type"], s["message"]))
            L.append("   Category   : %s (%s severity)%s" % (s["category"], s["severity"],
                                                             ", ran in a worker thread" if s["is_multithreaded"] else ""))
            L.append("   Root cause : %s" % s["explanation"])
            if rc:
                L.append("   Location   : %s, line %s, in %s()  ->  %s" % (rc["file"], rc["line"], rc["function"], rc["code"]))
        mt = dbg.get("multithreading")
        if mt:
            L.append("Concurrency analysis: severity %s - %s" % (mt["severity"].upper(), mt["root_cause"]))
            L.append("   Threads observed: %s" % (", ".join(mt["threads_observed"]) or "none"))
            for key, label in (("lock_order_inversions", "lock-order inversion(s)"), ("race_conditions", "possible race(s)"),
                               ("lock_contention", "contention event(s)"), ("unreleased_locks", "unreleased lock(s)")):
                if mt.get(key):
                    L.append("   %d %s" % (len(mt[key]), label))
        lp = dbg.get("log_patterns")
        if lp:
            L.append("Log patterns: %d lines, levels %s" % (lp["total_lines"], lp["level_counts"]))
            for f in lp["findings"]:
                L.append("   - %s" % f)
        L.append("")

        L += ["5. RECOMMENDED ACTIONS (highest priority first)", "-" * 78]
        if not recs:
            L.append("No action required.")
        for n, r in enumerate(recs[:20], 1):
            L.append("%2d. %s" % (n, r))
        L += ["", bar, "End of report", bar]
        return "\n".join(L)

    # -------------------------------------------------------------- dashboard
    def create_monitoring_dashboard_data(self, analysis_results: Dict) -> Dict[str, Any]:
        """Structured payload for monitoring systems (Grafana/Datadog/custom). Keys of analysis_results:
        test_results, investigation_results, debug_results."""
        tests = analysis_results.get("test_results") or {}
        inv = self._normalise(analysis_results.get("investigation_results"))
        dbg = analysis_results.get("debug_results") or {}
        summary = tests.get("summary", {})
        by_sev = summary.get("by_severity", {})
        score = summary.get("quality_score", 100.0)
        mt = dbg.get("multithreading") or {}
        deadlock = bool(mt.get("deadlock_detected"))
        status = {"good": "healthy", "attention": "warning", "at_risk": "critical"}[self._health(score, by_sev, deadlock)]

        alerts = []
        for i in tests.get("issues", []):
            if i["severity"] in ("critical", "high"):
                alerts.append({"level": i["severity"], "source": "data_quality", "column": i["column"], "message": i["message"]})
        for t in inv["traces"]:
            if t.get("origin_stage"):
                alerts.append({"level": "high", "source": "investigation", "column": t["column"],
                               "message": "%s in '%s' introduced at stage '%s' (confidence %.0f%%)" % (
                                   t["issue_type"], t["column"], t["origin_stage"], t["confidence"] * 100)})
        for d in mt.get("deadlocks", []):
            alerts.append({"level": "critical", "source": "concurrency", "message": "Deadlock: " + d["description"]})
        for s in dbg.get("stack_traces", []):
            if not s.get("error"):
                alerts.append({"level": s["severity"], "source": "debugging",
                               "message": "%s: %s" % (s["exception_type"], s["message"])})
        alerts.sort(key=lambda a: -SEVERITY_RANK.get(a["level"], 0))

        checks = [{"name": "%s:%s" % (r["test"], r["column"] or "dataset"),
                   "status": "pass" if r["passed"] else "fail",
                   "issue_count": len(r["issues"]),
                   "worst_severity": max((i["severity"] for i in r["issues"]), key=lambda s: SEVERITY_RANK[s], default=None)}
                  for r in tests.get("test_results", [])]
        return {
            "schema_version": "1.0",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "health_score": score,
            "metrics": {
                "rows_processed": (tests.get("dataset") or {}).get("rows", 0),
                "total_issues": summary.get("total_issues", 0),
                "critical_issues": by_sev.get("critical", 0), "high_issues": by_sev.get("high", 0),
                "medium_issues": by_sev.get("medium", 0), "low_issues": by_sev.get("low", 0),
                "tests_run": summary.get("tests_run", 0), "tests_failed": summary.get("tests_failed", 0),
                "test_duration_seconds": summary.get("duration_seconds", 0),
                "investigations_with_origin": sum(1 for t in inv["traces"] if t.get("origin_stage")),
                "deadlocks_detected": len(mt.get("deadlocks", [])),
                "exceptions_analyzed": len(dbg.get("stack_traces", [])),
            },
            "components": {
                "data_quality": {"status": status, "score": score},
                "investigation": {"origins": sorted({t["origin_stage"] for t in inv["traces"] if t.get("origin_stage")})},
                "concurrency": {"severity": mt.get("severity", "none"), "deadlock": deadlock},
            },
            "checks": checks,
            "alerts": alerts[:50],
        }

    @staticmethod
    def to_prometheus(dashboard: Dict[str, Any]) -> str:
        """Render dashboard metrics in Prometheus exposition format."""
        lines = ["# TYPE pipeline_quality_score gauge", "pipeline_quality_score %s" % dashboard["health_score"]]
        for k, v in dashboard["metrics"].items():
            lines.append("# TYPE pipeline_%s gauge" % k)
            lines.append("pipeline_%s %s" % (k, v))
        lines.append("pipeline_status{status=\"%s\"} 1" % dashboard["status"])
        return "\n".join(lines) + "\n"


# =========================================================================== #
# STEP 5 - INTEGRATION
# =========================================================================== #
def _read_text(path: Optional[str]) -> Optional[str]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except OSError as exc:
        logger.error("Cannot read %s: %s", path, exc)
        return None


def run_full_analysis(data_path, config_path, pipeline_path, trace_path, log_path) -> Dict[str, Any]:
    """Run all four components and return their combined results."""
    tester = DataQualityTester(config_path)
    investigator = AnomalyInvestigator()
    debugger = AdvancedDebugger()

    # --- Step 1: data quality
    try:
        data = load_dataframe(data_path)
        test_results = tester.run_integrity_tests(data)
    except Exception as exc:
        logger.exception("Data quality testing failed")
        test_results = {"dataset": {"name": "unknown", "rows": 0, "columns": []},
                        "summary": {"total_issues": 1, "by_severity": {"critical": 1, "high": 0, "medium": 0, "low": 0, "info": 0},
                                    "quality_score": 0.0, "tests_run": 0, "tests_failed": 0, "duration_seconds": 0},
                        "issues": [{"test": "schema_validation", "column": None, "severity": "critical", "count": 1,
                                    "total": 1, "percentage": 100.0, "sample_values": [], "sample_rows": [],
                                    "message": "Dataset could not be loaded/tested: %s" % exc}],
                        "test_results": [], "column_profiles": {}, "errors": [{"task": "load", "error": str(exc)}]}

    # --- Step 2: investigation (uses the quality results)
    scenario: Dict[str, Any] = {}
    raw = _read_text(pipeline_path)
    if raw:
        try:
            scenario = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("Pipeline scenario is not valid JSON: %s", exc)
    pipeline_cfg = scenario.get("pipeline_config", {})
    traces: List[Dict] = []
    covered = set()
    for anomaly in scenario.get("anomalies", []):
        traces.append(investigator.trace_anomaly_source(anomaly, pipeline_cfg))
        covered.add((anomaly.get("column"), anomaly.get("issue_type")))
    extra = 0
    for issue in test_results.get("issues", []):
        itype = investigator.TEST_TO_ISSUE.get(issue["test"])
        key = (issue["column"], itype)
        if itype and issue["column"] and key not in covered and SEVERITY_RANK[issue["severity"]] >= 3 and extra < 3 and pipeline_cfg:
            covered.add(key)
            extra += 1
            traces.append(investigator.trace_anomaly_source({"column": issue["column"], "issue_type": itype}, pipeline_cfg))
    lineage = investigator.analyze_data_lineage(scenario["data_flow"]) if scenario.get("data_flow") else None
    investigation_results = {"traces": traces, "lineage": lineage}

    # --- Step 3: debugging
    debug_results: Dict[str, Any] = {"stack_traces": []}
    trace_text = _read_text(trace_path)
    if trace_text:
        chunks = [c for c in re.split(r"(?=Traceback \(most recent call last\):)", trace_text) if c.strip()]
        for chunk in chunks or [trace_text]:
            debug_results["stack_traces"].append(debugger.analyze_stack_trace(chunk))
    log_text = _read_text(log_path)
    if log_text:
        lines = log_text.splitlines()
        debug_results["multithreading"] = debugger.debug_multithreading_issues(lines)
        debug_results["log_patterns"] = debugger.analyze_log_patterns(lines)
    debug_results["recommendations"] = debugger.generate_resolution_recommendations(debug_results)

    return {"test_results": test_results, "investigation_results": investigation_results,
            "debug_results": debug_results}


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    """Main execution function integrating all components and running a complete analysis."""
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description="Pipeline data quality & debugging system")
    p.add_argument("--data", default=os.path.join(here, "sample_data", "customers.csv"))
    p.add_argument("--config", default=os.path.join(here, "config", "quality_config.json"))
    p.add_argument("--pipeline", default=os.path.join(here, "sample_data", "pipeline_scenario.json"))
    p.add_argument("--stack-trace", default=os.path.join(here, "sample_data", "failed_pipeline_trace.txt"))
    p.add_argument("--log", default=os.path.join(here, "sample_data", "deadlock.log"))
    p.add_argument("--output", default=os.path.join(here, "output"))
    p.add_argument("--regenerate-sample", action="store_true", help="(re)create the sample data and config")
    args, _ = p.parse_known_args(argv)

    if args.regenerate_sample or not (os.path.exists(args.data) and os.path.exists(args.config)):
        try:
            from sample_data_generator import generate_all
            if os.path.abspath(args.data).startswith(here):
                generate_all(here)
        except ImportError:
            logger.warning("sample_data_generator.py not found; continuing without sample data")

    start = time.perf_counter()
    results = run_full_analysis(args.data, args.config, args.pipeline, args.stack_trace, args.log)

    reporter = MonitoringReporter()
    text_report = reporter.generate_quality_report(results["test_results"], results["investigation_results"],
                                                   results["debug_results"])
    json_report = reporter.generate_quality_report(results["test_results"], results["investigation_results"],
                                                   results["debug_results"], output_format="json")
    dashboard = reporter.create_monitoring_dashboard_data(results)

    os.makedirs(args.output, exist_ok=True)
    outputs = {
        "pipeline_quality_report.txt": text_report,
        "pipeline_quality_report.json": json_report,
        "dashboard_data.json": to_json(dashboard),
        "metrics.prom": reporter.to_prometheus(dashboard),
    }
    for name, content in outputs.items():
        with open(os.path.join(args.output, name), "w", encoding="utf-8") as fh:
            fh.write(content)

    print(text_report)
    print("\nFiles written to: %s" % os.path.abspath(args.output))
    print("Total runtime: %.2fs" % (time.perf_counter() - start))
    results.update({"report_text": text_report, "report_json": json_report, "dashboard": dashboard})
    return results


if __name__ == "__main__":
    main()