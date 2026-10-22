# NammaMart Airflow assignment

This Astro project contains parallel ETL and ELT implementations for the NammaMart quick-commerce assignment. It keeps the existing Meridian demo pipeline and its folder layout, and adds the assignment pipelines alongside it.

## Project map

- `dags/nammamart_etl_basavraj.py` — five parallel extracts → Validate → Transform → Load clean (`load_clean`) → Reconcile → Report; Validate also branches to a dedicated Quarantine task. A final audit task records the run.
- `dags/nammamart_elt_basavraj.py` — five parallel extracts → Load raw → SQL Staging → SQL Marts; a final audit task records the run.
- `include/nammamart_etl.py` — ETL checks, masking, warehouse loads, and audit helpers.
- `include/sql/` — SQL-only ELT staging and mart transformations.
- `include/data/nammamart/` — the six provided CSVs, including the 1 October increment.
- `include/warehouse_basavraj_etl.duckdb` and `include/warehouse_basavraj_elt.duckdb` — separate runtime databases to prevent concurrent DAG writes from locking one another.
- `include/quarantine/basavraj/` — failed ETL rows, with customer PII masked.
- `assignment/ASSIGNMENT.md` — full task requirements.
- `SUBMISSION.md` — profiling, implementation notes, query examples, governance answers, and validation status.

## Run locally

Start Airflow with `astro dev start`, then trigger either DAG from the Airflow UI at `http://localhost:8080`. Both are scheduled for 09:00 Asia/Kolkata, Monday through Saturday. The runtime source files are read from `include/data/nammamart/`; do not edit the supplied CSVs by hand.

Before starting Airflow, check DAG imports with:

```powershell
astro dev run dags list-import-errors
```

The local transformation smoke tests use Python and DuckDB. The current Astro containers are stopped; run `astro dev start` before checking DAG imports, viewing the graph/schedule, or performing the assignment's failure/recovery experiments.
