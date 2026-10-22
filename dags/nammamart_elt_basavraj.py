"""Raw-first NammaMart ELT pipeline with SQL staging and marts."""

import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")


DATA_DIR = Path("/usr/local/airflow/include/data/nammamart")
SQL_DIR = Path("/usr/local/airflow/include/sql")
ELT_DB = Path("/usr/local/airflow/include/warehouse_basavraj_elt.duckdb")
SOURCES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
DEFAULT_ARGS = {
    "owner": "fde-cohort",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="nammamart_elt_basavraj",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "assignment"],
    doc_md=__doc__,
)
def nammamart_elt_basavraj():
    @task
    def extract_source(file_name: str) -> str:
        path = DATA_DIR / file_name
        if not path.is_file():
            raise FileNotFoundError(f"Required NammaMart source file is missing: {path}")
        return str(path)

    @task
    def load_raw(paths: list[str], **context) -> dict[str, int]:
        con = duckdb.connect(str(ELT_DB))
        run_id = context["run_id"]
        loaded: dict[str, int] = {}
        try:
            for (table, file_name), path in zip(SOURCES.items(), paths):
                con.execute(
                    f"""CREATE TABLE IF NOT EXISTS raw_{table} AS
                    SELECT *, CAST(NULL AS TIMESTAMPTZ) AS _loaded_at,
                           CAST(NULL AS VARCHAR) AS _source_file,
                           CAST(NULL AS VARCHAR) AS _run_id
                    FROM read_csv(?, all_varchar=true, nullstr='__NMM_NULL_SENTINEL__') WHERE FALSE""",
                    [path],
                )
                con.execute(
                    f"""INSERT INTO raw_{table}
                    SELECT *, CURRENT_TIMESTAMP, ?, ?
                    FROM read_csv(?, all_varchar=true, nullstr='__NMM_NULL_SENTINEL__')""",
                    [file_name, run_id, path],
                )
                loaded[f"{table}_extracted"] = con.execute(
                    "SELECT COUNT(*) FROM read_csv(?, all_varchar=true, nullstr='__NMM_NULL_SENTINEL__')", [path]
                ).fetchone()[0]
                loaded[f"{table}_loaded"] = loaded[f"{table}_extracted"]

            increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"
            if increment_path.is_file():
                con.execute(
                    """INSERT INTO raw_orders
                    SELECT *, CURRENT_TIMESTAMP, ?, ?
                    FROM read_csv(?, all_varchar=true, nullstr='__NMM_NULL_SENTINEL__')""",
                    [increment_path.name, run_id, str(increment_path)],
                )
                increment_count = con.execute(
                    "SELECT COUNT(*) FROM read_csv(?, all_varchar=true, nullstr='__NMM_NULL_SENTINEL__')",
                    [str(increment_path)]
                ).fetchone()[0]
                loaded["orders_increment_extracted"] = increment_count
                loaded["orders_increment_loaded"] = increment_count

            con.execute(
                """CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
                    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
                )"""
            )
            return loaded
        finally:
            con.close()

    @task
    def transform_staging(loaded: dict[str, int], **context) -> dict[str, int]:
        con = duckdb.connect(str(ELT_DB))
        run_id = context["run_id"].replace("'", "''")
        dag_id = "nammamart_elt_basavraj"
        try:
            if _table_exists(con, "dq_results"):
                con.execute("DELETE FROM dq_results WHERE run_id = ?", [context["run_id"]])
            sql = (SQL_DIR / "nammamart_staging.sql").read_text(encoding="utf-8")
            sql = sql.replace("{{RUN_ID}}", run_id).replace("{{DAG_ID}}", dag_id)
            con.execute(sql)
            stage_counts = con.execute(
                """SELECT COUNT(*) FILTER (WHERE dq_status = 'passed'),
                          COUNT(*) FILTER (WHERE dq_status = 'failed')
                   FROM (
                       SELECT dq_status FROM stg_orders
                       UNION ALL SELECT dq_status FROM stg_payments
                       UNION ALL SELECT dq_status FROM stg_customers
                       UNION ALL SELECT dq_status FROM stg_products
                       UNION ALL SELECT dq_status FROM stg_stores
                   )"""
            ).fetchone()
            loaded["rows_loaded"], loaded["rows_quarantined"] = stage_counts
            return loaded
        finally:
            con.close()

    @task
    def transform_marts(staged: dict[str, int]) -> dict[str, int]:
        con = duckdb.connect(str(ELT_DB))
        try:
            sql = (SQL_DIR / "nammamart_marts.sql").read_text(encoding="utf-8")
            con.execute(sql)
            print("[ELT] Mart daily store KPIs")
            print(con.execute("SELECT * FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id").fetchdf().to_string(index=False))
            print("[ELT] Mart category margin")
            print(con.execute("SELECT * FROM mart_category_margin ORDER BY category").fetchdf().to_string(index=False))
            return staged
        finally:
            con.close()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run(**context) -> None:
        ti = context["ti"]
        raw_counts = ti.xcom_pull(task_ids="load_raw") or {}
        counts = ti.xcom_pull(task_ids="transform_marts") or ti.xcom_pull(task_ids="transform_staging") or raw_counts
        status = "success" if raw_counts and ti.xcom_pull(task_ids="transform_marts") else "failed"
        con = duckdb.connect(str(ELT_DB))
        try:
            con.execute(
                """CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
                    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
                )"""
            )
            con.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
                [context["run_id"], "nammamart_elt_basavraj"],
            )
            con.execute(
                """INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    context["run_id"], "nammamart_elt_basavraj",
                    context["dag_run"].start_date, pendulum.now("UTC"),
                    sum(value for key, value in counts.items() if key.endswith("_extracted")),
                    counts.get("rows_loaded", sum(value for key, value in counts.items() if key.endswith("_loaded"))),
                    counts.get("rows_quarantined", 0), status,
                ],
            )
        finally:
            con.close()

    # The five source reads are independent; raw ingestion starts after all are available.
    source_paths = [
        extract_source.override(task_id=f"extract_{table}")(file_name)
        for table, file_name in SOURCES.items()
    ]
    raw = load_raw(source_paths)
    staging = transform_staging(raw)
    marts = transform_marts(staging)
    audit = audit_run()
    audit.set_upstream([marts])


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table_name]
    ).fetchone()[0] > 0


nammamart_elt_basavraj()
