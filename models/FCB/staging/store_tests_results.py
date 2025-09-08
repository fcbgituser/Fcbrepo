# models/FCB/staging/store_tests_results.py
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
TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.DBT_TEST_RESULTS"

# ---------------------------
# Helpers (top-level only)
# ---------------------------
def load_inputs(target_path: Path, source_path: Path):
    run_json = json.loads(target_path.read_text(encoding="utf-8"))
    source_obj = yaml.safe_load(source_path.read_text(encoding="utf-8")) if source_path.exists() else {}
    return run_json, source_obj

def _get_known_tables(source_obj):
    return [t["name"] for src in source_obj.get("sources", []) for t in src.get("tables", []) if "name" in t]

def _parse_result_to_row(res, known_tables):
    """
    Returns a dict representing a single row.
    """
    unique_id = res.get("unique_id", "") or ""
    node = res.get("node") or {}
    test_name = (res.get("test_metadata") or {}).get("name") or res.get("name") or unique_id
    model_name = node.get("name") or node.get("alias") or ""
    column_names = None

    # Try to infer model and column from unique_id if possible
    if not model_name and unique_id:
        parts = unique_id.split(".")
        if len(parts) >= 3:
            info = parts[2]
            for tbl in known_tables:
                if tbl and tbl in info:
                    model_name = tbl
                    remaining = info.replace(tbl, "")
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

    return {
        "test_name": test_name,
        "model_name": model_name,
        "column_names": column_names,
        "test_status": status,
        "failures": failures,
        "message": message,
        "test_execution_time": exec_time,
        "effective_timestamp": effective_ts,
        "raw_result": raw_result
    }

# ---------------------------
# Exactly one top-level model() function (single return at the end)
# ---------------------------
def model(dbt, session):
    """
    dbt Python model that:
      - reads target/run_results.json and models/source/source.yml
      - tries to insert into Snowflake table (best-effort)
      - returns a single DataFrame (Snowpark DF if available)
    """
    # Ensure dbt materializes this model as a table
    dbt.config(materialized="table")

    # Paths (project relative)
    target_path = Path("target") / "run_results.json"
    source_path = Path("models") / "source" / "source.yml"

    # Prepare an empty dataframe schema to return in case of missing artifact
    columns = [
        "test_name","model_name","column_names","test_status","failures","message",
        "test_execution_time","effective_timestamp","raw_result"
    ]
    empty_df = pd.DataFrame(columns=columns)
    # If run_results.json is missing, return empty DF (single return)
    if not target_path.exists():
        dbt.log(f"target/run_results.json not found at {target_path}; returning empty table.")
        try:
            return session.create_dataframe(empty_df)
        except Exception:
            return empty_df

    # Load JSON/YAML
    try:
        run_json, source_obj = load_inputs(target_path, source_path)
    except Exception as e:
        dbt.log(f"Error loading artifacts: {e}")
        try:
            return session.create_dataframe(empty_df)
        except Exception:
            return empty_df

    known_tables = _get_known_tables(source_obj)
    results = run_json.get("results", []) or []

    parsed_rows = []
    insert_tuples = []
    for r in results:
        if not r.get("unique_id", "").startswith("test."):
            continue
        row = _parse_result_to_row(r, known_tables)
        parsed_rows.append(row)
        insert_tuples.append((
            row["test_name"],
            row["model_name"],
            row["column_names"],
            row["test_status"],
            int(row["failures"]) if row["failures"] is not None else None,
            row["message"],
            row["test_execution_time"],
            row["effective_timestamp"],
            row["raw_result"]
        ))

    # Build DataFrame to return (single dataframe)
    if not parsed_rows:
        df = empty_df
    else:
        df = pd.DataFrame(parsed_rows)

    # Attempt to insert into Snowflake, but do NOT return anything from here.
    # Any exception is logged and ignored so the model will still return the DataFrame.
    if insert_tuples:
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
                cs.executemany(insert_sql, insert_tuples)
                conn.commit()
                dbt.log(f"Inserted {len(insert_tuples)} rows into {TARGET_TABLE}")
            finally:
                cs.close()
                conn.close()
        except Exception as e:
            dbt.log(f"Warning: Snowflake insert via connector failed: {e}")

    # Single return here: prefer Snowpark DataFrame if available, otherwise pandas DataFrame
    try:
        sp_df = session.create_dataframe(df)
        return sp_df
    except Exception:
        return df
