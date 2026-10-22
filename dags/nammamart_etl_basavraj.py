"""Validated, idempotent NammaMart ETL pipeline."""

import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")
from nammamart_etl import (
    build_reports,
    extract_csv,
    load_star_schema,
    quarantine_failed as write_quarantine,
    reconcile_payments as reconcile_payment_data,
    transform_clean,
    validate_sources,
    write_run_audit,
)


SOURCE_FILES = {
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
    dag_id="nammamart_etl_basavraj",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "etl", "assignment"],
    doc_md=__doc__,
)
def nammamart_etl_basavraj():
    @task
    def extract_file(file_name: str) -> str:
        payload = extract_csv(file_name)
        print(f"[Extract] {file_name}")
        return payload

    @task
    def validate(extracted: list[str], **context) -> dict:
        source_payloads = dict(zip(SOURCE_FILES, extracted))
        result = validate_sources(source_payloads, context["run_id"])
        totals = result["counts"]
        print(
            "[Validate] "
            f"{sum(row['passed'] for row in totals.values())} passed rows; "
            f"{sum(row['quarantined'] for row in totals.values())} rows to quarantine"
        )
        return result

    @task
    def quarantine(validated: dict, **context) -> dict[str, int]:
        return write_quarantine(validated, context["run_id"])

    @task
    def transform(validated: dict) -> dict[str, str]:
        result = transform_clean(validated)
        print("[Transform] Standardized values, masked customer data, joined dimensions, and derived order metrics")
        return result

    @task
    def load_clean(transformed: dict[str, str]) -> int:
        return load_star_schema(transformed)

    @task
    def reconcile(transformed: dict[str, str], loaded_rows: int) -> int:
        print(f"[Reconcile] Fact rows available: {loaded_rows}")
        return reconcile_payment_data(transformed)

    @task
    def report(loaded_rows: int, mismatch_count: int) -> dict[str, int]:
        print(f"[Report] Payment mismatches available: {mismatch_count}")
        return build_reports() | {
            "rows_loaded": loaded_rows,
            "payment_mismatches": mismatch_count,
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run(**context) -> None:
        ti = context["ti"]
        validation = ti.xcom_pull(task_ids="validate") or {}
        quarantine_counts = ti.xcom_pull(task_ids="quarantine") or {}
        report_result = ti.xcom_pull(task_ids="report") or {}
        required_task_ids = [
            *(f"extract_{name}" for name in SOURCE_FILES),
            "validate",
            "quarantine",
            "transform",
            "load_clean",
            "reconcile",
            "report",
        ]
        successful_task_outputs = {
            task_id: ti.xcom_pull(task_ids=task_id)
            for task_id in required_task_ids
        }
        counts_by_file = validation.get("counts", {})
        counts = {
            "rows_extracted": sum(row["extracted"] for row in counts_by_file.values()),
            "rows_loaded": report_result.get("rows_loaded", 0),
            "rows_quarantined": sum(quarantine_counts.values()),
        }
        status = "success" if all(output is not None for output in successful_task_outputs.values()) else "failed"
        write_run_audit(
            context["run_id"],
            "nammamart_etl_basavraj",
            counts,
            status,
            start_time=context["dag_run"].start_date,
        )

    extracted = [
        extract_file.override(task_id=f"extract_{name}")(file_name)
        for name, file_name in SOURCE_FILES.items()
    ]
    validated = validate(extracted)
    quarantined = quarantine(validated)
    transformed = transform(validated)
    loaded = load_clean(transformed)
    reconciled = reconcile(transformed, loaded)
    reported = report(loaded, reconciled)
    audited = audit_run()
    audited.set_upstream([quarantined, reported])


nammamart_etl_basavraj()
