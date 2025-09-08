# models/store_test_results.py
from pathlib import Path
import json
import yaml
import pandas as pd
import snowflake.connector as sf

# ---------------------------
# HARD-CODED SNOWFLAKE CREDENTIALS (as requested)
# ---------------------------
SF_USER = 'FCB_AMIT'
SF_PASSWORD = 'FCBTrial@123456'
SF_ACCOUNT = 'dphbamj-snc95345'
SF_WAREHOUSE = 'RL_FCB_D'
SF_DATABASE = 'TEST_FCB'
SF_SCHEMA = 'DBT_ATIWARI'
TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.DBT_TEST_RESULTS"  # adjust if needed

# ---------------------------
# Helpers (top-level; NOT nested)
# ---------------------------
def load_inputs(target_path: Path, source_path: Path):
    run_json = json.loads(target_path.read_text(encoding="utf-8"))
    source_obj = yaml.safe_load(source_path.read_text(encoding="utf-8")) if source_path.exists() else {}
    return run_json, source_obj

def _get_known_tables(source_obj):
    return [t["name"] for src in source_obj.get("sources", []) for t in src.get("tables", []) if "name" in t]

def _parse_result_to_row(res, run_meta, known_tables):
    """
    Normalize a single dbt run_results 'result' entry into tuple matching:
    (test_name, model_name, column_names, test_status, failures, message, test_execution_time, effective_timestamp, raw_result)
    """
    # minimal safe parsing to avoid brittle tokens
    unique_id = res.get("unique_id", "")
    node = res.get("node") or {}
    # default fields
    test_name = (res.get("test_metadata") or {}).get("name") or res.get("name") or unique_id
    model_name = node.get("name") or node.get("alias") or ""
    column_names = None

    # try to detect model_name or column from unique_id and known_tables
    if not model_name and unique_id:
        # unique_id like "test.pkg.not_null.source__table__col__..."
        parts = unique_id.split(".")
        if len(parts) >= 3:
            info = parts[2]
            # find table name from known_tables inside token
            for tbl in known_tables:
                if tbl and tbl in info:
                    model_name = tbl
                    remaining = info.replace(tbl, "")
                    # infer column(s) for non-relationship tests
                    if "relationships" not in info:
                        col = remaining.lstrip("_").replace("__", ",")
                        col = col.replace("sk", f"{tbl}_sk").replace("key", f"{tbl}_key")
                        column_names = col if col else None
                    else:
                        column_names = remaining.lstrip("_").replace("__", ",")
                    break

    status = res.get("status")
    failures = 0 if res.get("failures") is None else res.get("failures")
    message = res.get("message") or ""
    if isinstance(message, str):
        message = message.replace("'", "`")

    # timing
    timing = res.get("timing") or []
    exec_time = None
    started_at = None
    completed_at = None
    if isinstance(timing, list) and timing:
        t0 = timing[0]
        exec_time = t0.get("execution_time") or res.get("execution_time")
        started_at = t0.get("started_at") or res.get("started_at")
        completed_at = t0.get("completed_at") or res.get("completed_at")
    else:
        exec_time = res.get("execution_time")
        started_at = res.get("started_at")
        completed_at = res.get("completed_at")

    effective_ts = completed_at or started_at

    raw_result = json.dumps(res)

    return (
        test_name,
        model_name,
        column_names,
        status,
        failures,
        message,
        exec_time,
        effective_ts,
        raw_result
    )

# ---------------------------
# Exactly one top-level model() function required by dbt
# ---------------------------
def model(dbt, session):
    """
    dbt Python model that:
      - reads target/run_results.json and models/source/source.yml
      - inserts rows into a Snowflake table (attempt)
      - returns a DataFrame to be materialized by dbt
    NOTE: run_results.json must exist before running this model.
    """
    # ensure dbt materializes the model as a table
    dbt.config(materialized="table")

    # project-relative paths
    target_path = Path("target") / "run_results.json"
    source_path = Path("models") / "source" / "source.yml"

    # if run_results.json missing -> return empty dataframe so dbt still materializes
    if not target_path.exists():
        dbt.log(f"target/run_results.json not found at {target_path}; returning empty table.")
        cols = [
            "test_name","model_name","column_names","test_status","failures","message",
            "test_execution_time","effective_timestamp","raw_result"
        ]
        empty_df = pd.DataFrame(columns=cols)
        try:
            return session.create_dataframe(empty_df)
        except Exception:
            return empty_df

    # load artifacts
    run_json, source_obj = load_inputs(target_path, source_path)
    known_tables = _get_known_tables(source_obj)
    results = run_json.get("results", []) or []

    parsed_rows = []
    parsed_rows_for_insert = []
    for r in results:
        # process only test results
        if not r.get("unique_id", "").startswith("test."):
            continue
        tup = _parse_result_to_row(r, run_json.get("metadata", {}), known_tables)
        parsed_rows.append({
            "test_name": tup[0],
            "model_name": tup[1],
            "column_names": tup[2],
            "test_status": tup[3],
            "failures": tup[4],
            "message": tup[5],
            "test_execution_time": tup[6],
            "effective_timestamp": tup[7],
            "raw_result": tup[8],
        })
        parsed_rows_for_insert.append(tup)

    # build dataframe to return
    if not parsed_rows:
        dbt.log("No test results found in run_results.json; returning empty dataframe.")
        cols = [
            "test_name","model_name","column_names","test_status","failures","message",
            "test_execution_time","effective_timestamp","raw_result"
        ]
        df_empty = pd.DataFrame(columns=cols)
        try:
            return session.create_dataframe(df_empty)
        except Exception:
            return df_empty

    df = pd.DataFrame(parsed_rows)

    # try inserting into Snowflake via connector (may fail in dbt Cloud due to network restrictions)
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            test_name,
            model_name,
            column_names,
            test_status,
            failures,
            message,
            test_execution_time,
            effective_timestamp,
            raw_result
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
    """
    if parsed_rows_for_insert:
        try:
            conn = sf.connect(
                user=SF_USER,
                password=SF_PASSWORD,
                account=SF_ACCOUNT,
                warehouse=SF_WAREHOUSE,
                database=SF_DATABASE,
                schema=SF_SCHEMA,
                client_session_keep_alive=False
            )
            cs = conn.cursor()
            try:
                cs.executemany(insert_sql, parsed_rows_for_insert)
                conn.commit()
                dbt.log(f"Inserted {len(parsed_rows_for_insert)} rows into {TARGET_TABLE}")
            finally:
                cs.close()
                conn.close()
        except Exception as e:
            # Do NOT raise — log and continue so dbt model materializes
            dbt.log(f"Warning: Snowflake connector insert failed: {e}")

    # return Snowpark DF if available, else pandas DF
    try:
        sp_df = session.create_dataframe(df)
        return sp_df
    except Exception:
        return df
