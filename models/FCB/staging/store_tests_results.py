# insert_results_via_connector_custom.py
"""
Standalone script: read dbt artifacts (run_results.json, source.yml, manifest.json)
and insert test results into Snowflake with model-level metadata.

Usage:
  place this script at project root and run after dbt run/test that produces target/
  python insert_results_via_connector_custom.py
"""

import json
import yaml
from pathlib import Path
import snowflake.connector
from typing import Dict, Any, List, Tuple

# --- hardcoded connection details (you provided these) ---
SF_USER = 'FCB_AMIT'
SF_PASSWORD = 'FCBTrial@123456'
SF_ACCOUNT = 'dphbamj-snc95345'
SF_WAREHOUSE = 'RL_FCB_D'
SF_DATABASE = 'TEST_FCB'
SF_SCHEMA = 'DBT_ATIWARI'
TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.DBT_TEST_RESULTS"  # change if desired

# --- artifact paths (project-relative) ---
TARGET_DIR = Path("target")
RUN_RESULTS_PATH = TARGET_DIR / "run_results.json"
MANIFEST_PATH = TARGET_DIR / "manifest.json"
SOURCE_YML_PATH = Path("models") / "source" / "source.yml"

# test types to consider (same as your earlier list)
TEST_NAME_CANDIDATES = ['source_not_null', 'source_table_not_empty', 'not_null', 'relationships']


# -----------------------------
# Helpers: load artifacts
# -----------------------------
def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# -----------------------------
# Build manifest mapping
# -----------------------------
def build_manifest_map(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build map unique_id -> metadata dict (path, tags, description, package_name, resource_type)
    """
    mapping: Dict[str, Dict[str, Any]] = {}
    nodes = manifest.get("nodes", {}) or {}
    # seeds, sources and tests may also be in other keys, but nodes cover models/tests
    for unique_id, node in nodes.items():
        mapping[unique_id] = {
            "original_file_path": node.get("original_file_path") or node.get("file_path"),
            "node_path": node.get("path"),  # alternate
            "tags": node.get("tags") or [],
            "meta": node.get("meta") or {},
            "resource_type": node.get("resource_type"),
            "package_name": node.get("package_name"),
            "name": node.get("name"),
            "description": node.get("description") or node.get("meta", {}).get("description", "")
        }
    return mapping


# -----------------------------
# Build rows for insertion
# -----------------------------
def build_rows_from_run(run_json: Dict[str, Any], source_obj: Dict[str, Any], manifest_map: Dict[str, Dict[str, Any]]) -> List[Tuple]:
    """Return list of tuples matching insert columns in order used below."""
    known_tables = [t["name"] for src in source_obj.get("sources", []) for t in src.get("tables", []) if "name" in t]
    rows: List[Tuple] = []

    run_meta = run_json.get("metadata", {}) or {}
    run_id = str(run_meta.get("run_id") or run_json.get("run_id") or "")
    job_id = str(run_meta.get("job_id") or "")
    project_id = str(run_meta.get("project_id") or "")
    invocation_id = str(run_meta.get("invocation_id") or "")

    for res in run_json.get("results", []) or []:
        if not res.get("unique_id", "").startswith("test."):
            continue

        unique_id = res.get("unique_id")
        status = res.get("status")
        failures = 0 if res.get("failures") is None else res.get("failures")
        message = res.get("message") or ""
        if isinstance(message, str):
            message = message.replace("'", "`")
        # timing
        timing = res.get("timing") or []
        execution_time = None
        started_at = None
        completed_at = None
        if isinstance(timing, list) and timing:
            t0 = timing[0]
            execution_time = t0.get("execution_time") or res.get("execution_time")
            started_at = t0.get("started_at") or res.get("started_at")
            completed_at = t0.get("completed_at") or res.get("completed_at")
        else:
            execution_time = res.get("execution_time")
            started_at = res.get("started_at")
            completed_at = res.get("completed_at")
        effective_ts = completed_at or started_at or None

        # basic model/test parse (fallbacks)
        model_name = (res.get("node") or {}).get("name") or ""
        test_name_token = unique_id  # fallback to unique_id
        column_names = None
        severity = "error"
        ref_test_target = ""

        # attempt to extract simple tokens similar to prior logic
        parts = unique_id.split(".")
        if len(parts) >= 3 and parts[0] == "test":
            info = parts[2]
            # detect test candidate
            found_test = ""
            for t in TEST_NAME_CANDIDATES:
                if t in info:
                    found_test = t
                    test_name_token = t
                    if t == "relationships":
                        trimmed = info.replace("relationships", "")
                        candidate = trimmed.lstrip("_").replace("__", ",").split(",")[-1]
                        ref_test_target = candidate
                        info = info.replace(candidate, "")
                    info = info.replace(t, "")
                    break
            # severity markers
            if "__warn" in info:
                severity = "warn"
                info = info.replace("__warn", "")
            else:
                info = info.replace("__error", "")
            # detect model by known_tables
            for tbl in known_tables:
                if tbl and tbl in info:
                    model_name = tbl
                    break
            # column names
            if model_name:
                remaining = info.replace(model_name, "")
                remaining = remaining.replace("_amis_", "")
                if found_test != "relationships":
                    col = remaining.lstrip("_").replace("__", ",")
                    col = col.replace("sk", f"{model_name}_sk").replace("key", f"{model_name}_key")
                    column_names = col if col else None
                else:
                    column_names = remaining.lstrip("_").replace("__", ",")

        # model-level metadata from manifest if available
        manifest_meta = manifest_map.get(res.get("node", {}).get("unique_id") or unique_id, {})
        model_path = manifest_meta.get("original_file_path") or manifest_meta.get("node_path") or None
        model_tags = manifest_meta.get("tags") or []
        model_desc = manifest_meta.get("description") or None
        package_name = manifest_meta.get("package_name") or None
        resource_type = manifest_meta.get("resource_type") or (res.get("resource_type") or (res.get("node") or {}).get("resource_type"))

        # final test_name: human friendly
        final_test_name = "Relationship Integrity test" if ref_test_target else (test_name_token or (res.get("test_metadata") or {}).get("name") or "")

        raw_result = json.dumps(res)

        # Build tuple in the exact order the INSERT expects:
        # run_id, job_id, project_id, invocation_id,
        # test_name, model_name, model_path, model_tags, package_name, resource_type,
        # column_names, test_status, failures, message, test_execution_time, effective_timestamp, raw_result
        rows.append((
            run_id,
            job_id,
            project_id,
            invocation_id,
            final_test_name,
            model_name,
            model_path,
            json.dumps(model_tags),           # store tags as JSON string
            package_name,
            resource_type,
            column_names,
            status,
            int(failures) if failures is not None else None,
            message,
            float(execution_time) if execution_time is not None else None,
            effective_ts,
            raw_result
        ))

    return rows


# -----------------------------
# Insert rows into Snowflake
# -----------------------------
def insert_rows(rows: List[Tuple]):
    if not rows:
        print("No rows to insert.")
        return

    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    cs = conn.cursor()
    try:
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                run_id,
                job_id,
                project_id,
                invocation_id,
                test_name,
                model_name,
                model_path,
                model_tags,
                package_name,
                resource_type,
                column_names,
                test_status,
                failures,
                message,
                test_execution_time,
                effective_timestamp,
                raw_result
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
            )
        """
        cs.executemany(insert_sql, rows)
        conn.commit()
        print(f"Inserted {len(rows)} rows into {TARGET_TABLE}")
    finally:
        cs.close()
        conn.close()


# -----------------------------
# Main
# -----------------------------
def main():
    if not RUN_RESULTS_PATH.exists():
        raise FileNotFoundError(f"{RUN_RESULTS_PATH} not found. Run dbt tests/build to generate it first.")

    run_json = load_json(RUN_RESULTS_PATH)
    source_obj = load_yaml(SOURCE_YML_PATH) if SOURCE_YML_PATH.exists() else {}
    manifest = load_json(MANIFEST_PATH) if MANIFEST_PATH.exists() else {}
    manifest_map = build_manifest_map(manifest) if manifest else {}

    rows = build_rows_from_run(run_json, source_obj, manifest_map)
    insert_rows(rows)


if __name__ == "__main__":
    main()
