# insert_results_via_connector.py  (standalone script)
import json
import yaml
from pathlib import Path
import snowflake.connector

# --- hardcoded connection details (you provided these) ---
SF_USER = 'FCB_AMIT'
SF_PASSWORD = 'FCBTrial@123456'
SF_ACCOUNT = 'dphbamj-snc95345'
SF_WAREHOUSE = 'RL_FCB_D'
SF_DATABASE = 'TEST_FCB'
SF_SCHEMA = 'DBT_ATIWARI'
TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.DBT_TEST_RESULTS"  # change if desired

def load_inputs(target_path: Path, source_path: Path):
    run_json = json.loads(target_path.read_text(encoding="utf-8"))
    source_obj = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    return run_json, source_obj

def build_rows_from_run(run_json, source_obj):
    # implement similar parsing as the dbt model
    # returns list of tuples matching insert columns
    known_tables = [t["name"] for src in source_obj.get("sources", []) for t in src.get("tables", []) if "name" in t]
    rows = []
    test_name_candidates = ['source_not_null','source_table_not_empty', 'not_null', 'relationships']
    run_meta = run_json.get("metadata", {}) or {}
    for r in run_json.get("results", []) or []:
        if not r.get("unique_id", "").startswith("test."):
            continue
        # reuse same parsing logic as earlier (for brevity, simplified here)
        # (you can copy _parse_result_item logic from the model and adapt output format)
        # For demo: minimal columns:
        test_name = r.get("unique_id")
        model_name = (r.get("node") or {}).get("name") or ""
        column_names = None
        status = r.get("status")
        failures = 0 if r.get("failures") is None else r.get("failures")
        message = r.get("message") or ""
        exec_time = r.get("execution_time")
        effective_ts = r.get("completed_at") or r.get("started_at")
        # sanitize message
        message = message.replace("'", "`") if isinstance(message, str) else message
        rows.append((test_name, model_name, column_names, status, failures, message, exec_time, effective_ts))
    return rows

def insert_rows(rows):
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
        # authenticator=...  # if required
    )
    cs = conn.cursor()
    try:
        insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (
                test_name,
                model_name,
                column_names,
                test_status,
                failures,
                message,
                test_execution_time,
                effective_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cs.executemany(insert_sql, rows)
        conn.commit()
        print(f"Inserted {len(rows)} rows into {TARGET_TABLE}")
    finally:
        cs.close()
        conn.close()

if __name__ == "__main__":
    target_path = Path("target") / "run_results.json"
    source_path = Path("models") / "source" / "source.yml"
    run_json, source_obj = load_inputs(target_path, source_path)
    rows = build_rows_from_run(run_json, source_obj)
    insert_rows(rows)
