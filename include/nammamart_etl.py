"""Python transformations and data-quality checks for the NammaMart ETL DAG."""

from __future__ import annotations

import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DATA_DIR = Path("/usr/local/airflow/include/data/nammamart")
WAREHOUSE_DB = Path("/usr/local/airflow/include/warehouse_basavraj_etl.duckdb")
QUARANTINE_DIR = Path("/usr/local/airflow/include/quarantine/basavraj")
SOURCE_KEYS = {
    "orders": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
}
DQ_DIMENSIONS = ("completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness")


def extract_csv(file_name: str) -> str:
    """Return a source CSV as JSON records for TaskFlow XCom transfer."""
    path = DATA_DIR / file_name
    if not path.is_file():
        raise FileNotFoundError(f"Required NammaMart source file is missing: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False).to_json(orient="records")


def _mask_email(value: Any) -> str:
    text = str(value or "").strip()
    if "@" not in text:
        return "***"
    local, domain = text.split("@", 1)
    return f"{local[:2]}***@{domain}"


def _mask_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 4:
        return "*" * len(digits)
    return f"{digits[:2]}{'*' * max(0, len(digits) - 4)}{digits[-2:]}"


def _read_json_frame(payload: str) -> pd.DataFrame:
    return pd.read_json(io.StringIO(payload), dtype=False)


def _row_failures(
    file_name: str,
    frame: pd.DataFrame,
    references: dict[str, set[str]],
) -> tuple[pd.Series, list[str], dict[str, pd.Series]]:
    """Return critical row failures and warning masks without modifying input values."""
    reasons = pd.Series("", index=frame.index, dtype="object")
    warnings: dict[str, pd.Series] = {}

    def add(mask: pd.Series, reason: str) -> None:
        nonlocal reasons
        mask = mask.fillna(True)
        reasons.loc[mask] = reasons.loc[mask].map(
            lambda old: f"{old}; {reason}" if old else reason
        )

    required = {
        "orders": [
            "order_id", "order_ts", "customer_id", "store_id", "product_id",
            "quantity", "unit_price", "discount_pct", "order_amount",
            "payment_mode", "order_status",
        ],
        "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"],
        "customers": ["customer_id", "full_name", "signup_date"],
        "products": ["product_id", "product_name", "category"],
        "stores": ["store_id", "store_name", "zone"],
    }[file_name]
    for column in required:
        if column not in frame:
            raise ValueError(f"{file_name}.csv is missing required column {column!r}")
    missing = pd.Series(False, index=frame.index)
    for column in required:
        missing |= frame[column].astype(str).str.strip().eq("")
    add(missing, "completeness: required value is missing")

    key = SOURCE_KEYS[file_name]
    add(frame[key].astype(str).duplicated(keep=False), f"uniqueness: duplicate {key}")

    if file_name == "orders":
        status = frame["order_status"].astype(str).str.strip().str.upper()
        add(~status.isin(["DELIVERED", "CANCELLED", "RETURNED"]), "validity: invalid order_status")
        order_ts = pd.to_datetime(frame["order_ts"], errors="coerce")
        add(order_ts.isna(), "validity: invalid order_ts")
        add(order_ts > pd.Timestamp.now(tz=None), "timeliness: order_ts is in the future")
        for column, label in (
            ("customer_id", "customer"),
            ("product_id", "product"),
            ("store_id", "store"),
        ):
            add(~frame[column].astype(str).isin(references[label]), f"consistency: unknown {label}_id")
        quantity = pd.to_numeric(frame["quantity"], errors="coerce")
        unit_price = pd.to_numeric(frame["unit_price"], errors="coerce")
        discount = pd.to_numeric(frame["discount_pct"], errors="coerce")
        amount = pd.to_numeric(frame["order_amount"], errors="coerce")
        add(quantity.isna() | (quantity <= 0), "accuracy: quantity must be positive")
        add(
            discount.isna() | (discount < 0) | (discount > 100),
            "validity: discount_pct outside 0-100",
        )
        expected = (quantity * unit_price * (1 - discount / 100)).round(2)
        add(
            unit_price.isna() | amount.isna() | ((amount - expected).abs() > 0.01),
            "accuracy: order_amount does not match formula",
        )
        delivery = pd.to_numeric(frame["delivery_minutes"], errors="coerce")
        add(status.eq("DELIVERED") & delivery.isna(), "completeness: DELIVERED order has no delivery_minutes")
        add(status.eq("DELIVERED") & (delivery >= 120), "accuracy: delivery_minutes must be below 120")
        mode = frame["payment_mode"].astype(str).str.strip().str.lower()
        mode = mode.replace({"cod": "cash on delivery"})
        add(~mode.isin(["upi", "card", "cash on delivery", "wallet"]), "validity: invalid payment_mode")
        if unit_price.notna().any():
            distribution_warning = pd.Series(False, index=frame.index)
            median = unit_price.median()
            if median > 0:
                distribution_warning = unit_price > median * 10
            warnings["unit_price_distribution"] = distribution_warning
    elif file_name == "payments":
        payment_ts = pd.to_datetime(frame["payment_ts"], errors="coerce")
        add(payment_ts.isna(), "validity: invalid payment_ts")
        amount = pd.to_numeric(frame["amount_paid"], errors="coerce")
        add(amount.isna() | (amount < 0), "accuracy: amount_paid must be non-negative")
        add(~frame["order_id"].astype(str).isin(references["orders"]), "consistency: payment has unknown order_id")
        mode = frame["payment_mode"].astype(str).str.strip().str.lower()
        add(~mode.isin(["upi", "card", "cash on delivery", "wallet"]), "validity: invalid payment_mode")
        add(
            ~frame["payment_status"].astype(str).str.strip().str.upper().isin(["SUCCESS", "FAILED", "REFUNDED"]),
            "validity: invalid payment_status",
        )
    elif file_name == "customers":
        signup_date = pd.to_datetime(frame["signup_date"], errors="coerce")
        add(signup_date.isna(), "validity: invalid signup_date")
        add(signup_date > pd.Timestamp.now().normalize(), "timeliness: signup_date is in the future")
        email = frame["email"].astype(str)
        phone = frame["phone"].astype(str)
        warnings["email_format"] = ~email.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        warnings["phone_format"] = ~phone.str.replace(r"\D", "", regex=True).str.match(r"^\d{10}$")
    elif file_name == "products":
        mrp = pd.to_numeric(frame["mrp"], errors="coerce")
        cost = pd.to_numeric(frame["cost_price"], errors="coerce")
        add(mrp.isna() | (mrp <= 0), "accuracy: mrp must be positive")
        add(cost.isna() | (cost < 0) | (cost > mrp), "accuracy: cost_price must be between 0 and mrp")
    elif file_name == "stores":
        add(
            ~frame["zone"].astype(str).str.strip().str.title().isin(["North", "South", "East", "West"]),
            "validity: invalid zone",
        )

    return reasons.ne(""), reasons.tolist(), warnings


def _record_dq(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    dag_id: str,
    check_name: str,
    dimension: str,
    severity: str,
    rows_checked: int,
    rows_failed: int,
) -> None:
    status = "warning" if rows_failed and severity == "warning" else "failed" if rows_failed else "passed"
    con.execute(
        """INSERT INTO dq_results
           (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, datetime.now(timezone.utc)],
    )


def _ensure_dq_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
            status VARCHAR, checked_at TIMESTAMPTZ
        )"""
    )


def validate_sources(source_json: dict[str, str], run_id: str) -> dict[str, Any]:
    """Validate each source and return separate accepted and failed row sets."""
    frames = {name: _read_json_frame(payload) for name, payload in source_json.items()}
    increment_path = DATA_DIR / "orders_2026-10-01_increment.csv"
    if increment_path.is_file():
        frames["orders_increment"] = pd.read_csv(increment_path, dtype=str, keep_default_na=False)

    references: dict[str, set[str]] = {
        "customer": set(),
        "product": set(),
        "store": set(),
        "orders": set(frames["orders"]["order_id"].astype(str)),
    }
    if "orders_increment" in frames:
        references["orders"].update(frames["orders_increment"]["order_id"].astype(str))

    con = duckdb.connect(str(WAREHOUSE_DB))
    _ensure_dq_table(con)
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, "nammamart_etl_basavraj"])
    validated: dict[str, Any] = {"passed": {}, "failed": {}, "counts": {}, "payments_source": source_json["payments"]}
    try:
        file_order = ["customers", "products", "stores", "orders"]
        if "orders_increment" in frames:
            file_order.append("orders_increment")
        file_order.append("payments")
        for name in file_order:
            original = frames[name].copy()
            base_name = "orders" if name == "orders_increment" else name
            key = SOURCE_KEYS[base_name]
            duplicate_mask = original[key].astype(str).duplicated(keep="last")
            duplicate_rows = original.loc[duplicate_mask].copy()
            if not duplicate_rows.empty:
                duplicate_rows["failure_reason"] = f"uniqueness: duplicate {key}"
            candidate = original.loc[~duplicate_mask].copy()

            if name == "customers":
                reference_name = "customer"
            elif name == "products":
                reference_name = "product"
            elif name == "stores":
                reference_name = "store"
            else:
                reference_name = ""
            if reference_name:
                references[reference_name] = set(candidate[key].astype(str))

            failed_mask, reasons, warning_masks = _row_failures(base_name, candidate, references)
            failed_rows = candidate.loc[failed_mask].copy()
            failed_rows["failure_reason"] = [reason for reason, failed in zip(reasons, failed_mask) if failed]
            failed_rows = pd.concat([duplicate_rows, failed_rows], ignore_index=True)
            passed_rows = candidate.loc[~failed_mask].copy()
            if reference_name:
                references[reference_name] = set(passed_rows[key].astype(str))
            validated["passed"][name] = passed_rows.to_json(orient="records")
            validated["failed"][name] = failed_rows.to_json(orient="records")
            validated["counts"][name] = {
                "extracted": len(original),
                "passed": len(passed_rows),
                "quarantined": len(failed_rows),
            }

            checks: dict[str, tuple[str, int]] = {
                dimension: (
                    "critical",
                    int(duplicate_mask.sum()) if dimension == "uniqueness"
                    else sum(dimension in reason for reason in reasons),
                )
                for dimension in DQ_DIMENSIONS
            }
            if name == "customers":
                checks["validity"] = ("critical", sum("validity:" in reason for reason in reasons))
            for dimension, (severity, failures) in checks.items():
                _record_dq(con, run_id, "nammamart_etl_basavraj", f"{name}_{dimension}", dimension,
                           severity, len(original), failures)
            for check_name, warning_mask in warning_masks.items():
                _record_dq(con, run_id, "nammamart_etl_basavraj", f"{name}_{check_name}",
                           "validity" if name == "customers" else "accuracy", "warning",
                           len(candidate), int(warning_mask.sum()))

            if base_name == "orders":
                order_ts = pd.to_datetime(candidate["order_ts"], errors="coerce")
                latest = order_ts.max()
                stale = bool(pd.notna(latest) and latest < pd.Timestamp.now() - pd.Timedelta(days=1))
                _record_dq(con, run_id, "nammamart_etl_basavraj", f"{name}_freshness",
                           "timeliness", "warning", len(candidate), int(stale))
                if name == "orders":
                    volume_low = len(original) < 242
                    _record_dq(con, run_id, "nammamart_etl_basavraj", "orders_volume_anomaly",
                               "accuracy", "warning", len(original), int(volume_low))

        return validated
    finally:
        con.close()


def quarantine_failed(validated: dict[str, Any], run_id: str) -> dict[str, int]:
    """Write failed source rows to run-stamped quarantine CSVs with reasons."""
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    counts: dict[str, int] = {}
    for file_name, payload in validated["failed"].items():
        failed = _read_json_frame(payload)
        if file_name in ("customers",):
            if "full_name" in failed:
                failed["full_name"] = "***"
            for column in ("email", "phone"):
                if column in failed:
                    failed[column] = failed[column].map(_mask_email if column == "email" else _mask_phone)
        counts[file_name] = len(failed)
        if failed.empty:
            print(f"[Quarantine] {file_name}: no failed rows")
            continue
        path = QUARANTINE_DIR / f"{file_name}_failed_{safe_run_id}.csv"
        failed.to_csv(path, index=False)
        print(f"[Quarantine] {len(failed)} {file_name} rows written to {path}")
    return counts


def transform_clean(validated: dict[str, Any]) -> dict[str, str]:
    """Standardize accepted rows, mask PII, join dimensions, and derive metrics."""
    clean = {
        name: _read_json_frame(payload)
        for name, payload in validated["passed"].items()
    }
    orders = clean["orders"].copy()
    if "orders_increment" in clean:
        base = orders.assign(_priority=0)
        increment = clean["orders_increment"].assign(_priority=1)
        orders = pd.concat([base, increment], ignore_index=True)
        orders = orders.sort_values("_priority").drop_duplicates("order_id", keep="last").drop(columns="_priority")

    customers = clean["customers"].drop_duplicates("customer_id", keep="last").copy()
    products = clean["products"].drop_duplicates("product_id", keep="last").copy()
    stores = clean["stores"].drop_duplicates("store_id", keep="last").copy()
    customers["full_name"] = "***"
    customers["email"] = customers["email"].map(_mask_email)
    customers["phone"] = customers["phone"].map(_mask_phone)
    customers["loyalty_tier"] = customers["loyalty_tier"].astype(str).str.strip().str.title()
    stores["manager_name"] = "***"
    stores["zone"] = stores["zone"].astype(str).str.strip().str.title()
    products["category"] = products["category"].astype(str).str.strip()

    orders["order_status"] = orders["order_status"].astype(str).str.strip().str.upper()
    mode = orders["payment_mode"].astype(str).str.strip().str.lower()
    orders["payment_mode"] = mode.map({
        "upi": "UPI",
        "card": "Card",
        "cash on delivery": "Cash on Delivery",
        "cod": "Cash on Delivery",
        "wallet": "Wallet",
    }).fillna(orders["payment_mode"].astype(str).str.strip())
    orders["channel"] = orders["channel"].astype(str).str.strip()
    for column in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
        orders[column] = pd.to_numeric(orders[column], errors="coerce")
    orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce")
    orders["order_date"] = orders["order_ts"].dt.date
    orders["net_revenue"] = orders["order_amount"].where(orders["order_status"].eq("DELIVERED"), 0.0)
    orders["within_15_min"] = orders["delivery_minutes"].le(15)
    products["cost_price"] = pd.to_numeric(products["cost_price"], errors="coerce")

    facts = orders.merge(customers[["customer_id", "loyalty_tier"]], on="customer_id", how="left")
    facts = facts.merge(products[["product_id", "category", "cost_price"]], on="product_id", how="left")
    facts = facts.merge(stores[["store_id", "store_name", "zone"]], on="store_id", how="left")
    facts["gross_margin"] = facts["net_revenue"] - facts["quantity"] * facts["cost_price"]
    return {
        "customers": customers.to_json(orient="records", date_format="iso"),
        "products": products.to_json(orient="records", date_format="iso"),
        "stores": stores.to_json(orient="records", date_format="iso"),
        "facts": facts.to_json(orient="records", date_format="iso"),
        "source_order_ids": json.dumps(
            pd.concat(
                [
                    _read_json_frame(validated["passed"]["orders"])["order_id"],
                    _read_json_frame(validated["failed"]["orders"])["order_id"],
                    _read_json_frame(validated["passed"].get("orders_increment", "[]"))["order_id"],
                    _read_json_frame(validated["failed"].get("orders_increment", "[]"))["order_id"],
                ],
                ignore_index=True,
            ).drop_duplicates().tolist()
        ),
        "payments_source": validated["payments_source"],
    }


def load_star_schema(transformed: dict[str, str]) -> int:
    """Idempotently load transformed facts and dimensions into DuckDB."""
    customers = _read_json_frame(transformed["customers"])
    products = _read_json_frame(transformed["products"])
    stores = _read_json_frame(transformed["stores"])
    facts = _read_json_frame(transformed["facts"])
    source_order_ids = json.loads(transformed["source_order_ids"])

    con = duckdb.connect(str(WAREHOUSE_DB))
    try:
        con.execute("BEGIN TRANSACTION")
        con.register("customer_frame", customers)
        con.register("product_frame", products)
        con.register("store_frame", stores)
        con.register("fact_frame", facts)
        con.register("source_order_ids", pd.DataFrame({"order_id": source_order_ids}))
        for table, view in (
            ("etl_dim_customer", "customer_frame"),
            ("etl_dim_product", "product_frame"),
            ("etl_dim_store", "store_frame"),
        ):
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {view}")
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM fact_frame WHERE FALSE")
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM source_order_ids)")
        con.execute("INSERT INTO etl_fact_orders SELECT * FROM fact_frame")
        con.execute("COMMIT")
        row_count = con.execute("SELECT COUNT(*) FROM fact_frame").fetchone()[0]
        print(f"[Load] Loaded {row_count} accepted order rows to etl_fact_orders")
        return row_count
    except Exception:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        con.close()


def reconcile_payments(transformed: dict[str, str]) -> int:
    """Write every delivered-order/payment mismatch to the reconciliation table."""
    payments = _read_json_frame(transformed["payments_source"])
    con = duckdb.connect(str(WAREHOUSE_DB))
    try:
        con.register("payment_source", payments)
        con.execute(
            """CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            WITH comparisons AS (
                SELECT o.order_id, p.payment_id, o.order_amount,
                       TRY_CAST(p.amount_paid AS DOUBLE) AS amount_paid,
                       CASE
                           WHEN p.payment_id IS NULL THEN 'missing payment'
                           WHEN UPPER(TRIM(p.payment_status)) <> 'SUCCESS' THEN 'payment not successful'
                           WHEN ABS(o.order_amount - TRY_CAST(p.amount_paid AS DOUBLE)) > 0.01 THEN 'amount mismatch'
                           WHEN LOWER(TRIM(o.payment_mode)) <> LOWER(TRIM(p.payment_mode)) THEN 'payment mode mismatch'
                       END AS reason
                FROM etl_fact_orders o
                LEFT JOIN payment_source p ON p.order_id = o.order_id
                WHERE o.order_status = 'DELIVERED'
            ), orphan_payments AS (
                SELECT p.order_id, p.payment_id, NULL::DOUBLE AS order_amount,
                       TRY_CAST(p.amount_paid AS DOUBLE) AS amount_paid,
                       'payment has no source order' AS reason
                FROM payment_source p
                LEFT JOIN etl_fact_orders o ON o.order_id = p.order_id
                WHERE o.order_id IS NULL
            )
            SELECT * FROM comparisons WHERE reason IS NOT NULL
            UNION ALL SELECT * FROM orphan_payments"""
        )
        count = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
        print(f"[Reconcile] {count} payment mismatches written")
        return count
    finally:
        con.close()


def build_reports() -> dict[str, int]:
    """Build and print the ETL report tables from the accepted star schema."""
    con = duckdb.connect(str(WAREHOUSE_DB))
    try:
        con.execute(
            """CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, zone, store_name, SUM(net_revenue) AS net_revenue
            FROM etl_fact_orders WHERE order_status = 'DELIVERED'
            GROUP BY order_date, zone, store_name"""
        )
        con.execute(
            """CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT category, SUM(net_revenue) AS revenue, SUM(gross_margin) AS gross_margin
            FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY category"""
        )
        con.execute(
            """CREATE OR REPLACE TABLE etl_delivery_sla AS
            SELECT store_id, store_name, zone, AVG(delivery_minutes) AS avg_delivery_minutes,
                   100.0 * COUNT(*) FILTER (WHERE within_15_min) / NULLIF(COUNT(*), 0) AS within_15_min_pct,
                   COUNT(*) AS delivered_orders
            FROM etl_fact_orders WHERE order_status = 'DELIVERED'
            GROUP BY store_id, store_name, zone"""
        )
        con.execute(
            """CREATE OR REPLACE TABLE etl_top_customers AS
            SELECT customer_id, loyalty_tier, SUM(net_revenue) AS total_spend, COUNT(*) AS delivered_orders
            FROM etl_fact_orders WHERE order_status = 'DELIVERED'
            GROUP BY customer_id, loyalty_tier ORDER BY total_spend DESC LIMIT 10"""
        )
        con.execute(
            """CREATE OR REPLACE TABLE etl_revenue_by_payment_mode AS
            SELECT payment_mode, SUM(net_revenue) AS revenue
            FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY payment_mode"""
        )
        daily = con.execute(
            """SELECT zone, store_name, SUM(net_revenue) AS net_revenue
            FROM etl_daily_zone_revenue GROUP BY zone, store_name ORDER BY zone, store_name"""
        ).fetchdf()
        margin = con.execute("SELECT * FROM etl_category_margin ORDER BY category").fetchdf()
        print("[Report] Daily revenue by zone and store")
        print(daily.to_string(index=False))
        print("[Report] Category margin")
        print(margin.to_string(index=False))
        return {
            "daily_zone_rows": len(daily),
            "category_rows": len(margin),
        }
    finally:
        con.close()


def write_run_audit(
    run_id: str,
    dag_id: str,
    counts: dict[str, int],
    status: str,
    start_time: datetime | None = None,
) -> None:
    con = duckdb.connect(str(WAREHOUSE_DB))
    try:
        con.execute(
            """CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
                rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
            )"""
        )
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        now = datetime.now(timezone.utc)
        con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id,
                dag_id,
                start_time or now,
                now,
                sum(value for key, value in counts.items() if key.endswith("_extracted")),
                counts.get("rows_loaded", 0),
                counts.get("rows_quarantined", 0),
                status,
            ],
        )
    finally:
        con.close()
