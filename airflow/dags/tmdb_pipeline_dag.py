"""
tmdb_pipeline_dag.py
---------------------
Airflow DAG — orchestrates the full TMDb → Snowflake → dbt pipeline.

Schedule: Weekly (every Sunday at 2 AM UTC)

Tasks:
  1. fetch_movies_task     — calls TMDb API, writes CSV to /tmp
  2. load_snowflake_task   — uploads CSV to Snowflake RAW schema
  3. dbt_run_task          — runs dbt models (staging → intermediate → marts)
  4. dbt_test_task         — runs dbt tests
  5. notify_success_task   — logs completion summary

Prerequisites:
  pip install apache-airflow apache-airflow-providers-snowflake
  Set Airflow Variables: TMDB_API_KEY, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
"""

from datetime import datetime, timedelta
import json, os, logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

# ── DAG defaults ──────────────────────────────────────────────
default_args = {
    "owner":            "ishita",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id          = "tmdb_streaming_pipeline",
    description     = "Weekly TMDb movie data pipeline: API → Snowflake → dbt → Tableau",
    schedule_interval = "0 2 * * 0",   # every Sunday 02:00 UTC
    start_date      = datetime(2024, 7, 1),
    catchup         = False,
    default_args    = default_args,
    tags            = ["streaming", "tmdb", "snowflake", "dbt"],
)


# ── Task 1: Fetch from TMDb API ───────────────────────────────
def fetch_movies(**context):
    import requests, pandas as pd, time
    from pathlib import Path

    api_key = Variable.get("TMDB_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}", "accept": "application/json"}
    base    = "https://api.themoviedb.org/3"

    # Fetch genres
    genres_raw = requests.get(f"{base}/genre/movie/list", headers=headers).json()
    genre_map  = {g["id"]: g["name"] for g in genres_raw["genres"]}

    # Fetch movies (100 pages = 2,000 per weekly refresh)
    movies = []
    for page in range(1, 101):
        r = requests.get(
            f"{base}/discover/movie",
            headers=headers,
            params={"sort_by": "popularity.desc", "page": page, "vote_count.gte": 5},
        )
        if r.status_code == 429:
            time.sleep(15)
            r = requests.get(f"{base}/discover/movie", headers=headers,
                             params={"sort_by": "popularity.desc", "page": page})
        data = r.json()
        movies.extend(data["results"])
        if page >= data.get("total_pages", 1):
            break
        time.sleep(0.04)

    rows = []
    for m in movies:
        g = [genre_map.get(x, "Unknown") for x in m.get("genre_ids", [])]
        rows.append({
            "movie_id": m.get("id"), "title": m.get("title"),
            "original_language": m.get("original_language"),
            "release_date": m.get("release_date"), "popularity": m.get("popularity"),
            "vote_average": m.get("vote_average"), "vote_count": m.get("vote_count"),
            "genre_ids": json.dumps(m.get("genre_ids", [])),
            "genre_names": "|".join(g), "primary_genre": g[0] if g else None,
            "adult": m.get("adult"), "_fetched_at": pd.Timestamp.utcnow().isoformat(),
        })

    out = "/tmp/tmdb_movies_latest.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    log.info(f"Fetched {len(rows):,} movies → {out}")
    # Push path to XCom for downstream task
    context["ti"].xcom_push(key="csv_path", value=out)


fetch_task = PythonOperator(
    task_id         = "fetch_movies",
    python_callable = fetch_movies,
    dag             = dag,
    provide_context = True,
)


# ── Task 2: Load to Snowflake ─────────────────────────────────
def load_snowflake(**context):
    import pandas as pd, snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    csv_path = context["ti"].xcom_pull(task_ids="fetch_movies", key="csv_path")
    df = pd.read_csv(csv_path)
    df.columns = [c.upper() for c in df.columns]
    df["RELEASE_DATE"] = pd.to_datetime(df["RELEASE_DATE"], errors="coerce")
    df["_FETCHED_AT"]  = pd.to_datetime(df["_FETCHED_AT"],  errors="coerce")

    conn = snowflake.connector.connect(
        account   = Variable.get("SNOWFLAKE_ACCOUNT"),
        user      = Variable.get("SNOWFLAKE_USER"),
        password  = Variable.get("SNOWFLAKE_PASSWORD"),
        warehouse = "COMPUTE_WH",
        database  = "STREAMING_DB",
        schema    = "RAW",
        role      = "SYSADMIN",
    )
    success, _, nrows, _ = write_pandas(conn, df, "TMDB_MOVIES",
                                        auto_create_table=False, overwrite=True)
    conn.close()
    log.info(f"Loaded {nrows:,} rows (success={success})")


load_task = PythonOperator(
    task_id         = "load_snowflake",
    python_callable = load_snowflake,
    dag             = dag,
    provide_context = True,
)


# ── Task 3: dbt run ───────────────────────────────────────────
dbt_run_task = BashOperator(
    task_id      = "dbt_run",
    bash_command = "cd /opt/airflow/dbt/streaming_analytics && dbt run --profiles-dir /opt/airflow/dbt",
    dag          = dag,
)

# ── Task 4: dbt test ──────────────────────────────────────────
dbt_test_task = BashOperator(
    task_id      = "dbt_test",
    bash_command = "cd /opt/airflow/dbt/streaming_analytics && dbt test --profiles-dir /opt/airflow/dbt",
    dag          = dag,
)

# ── Task 5: Log summary ───────────────────────────────────────
def notify(**context):
    run_date = context["ds"]
    log.info(f"Pipeline complete for {run_date}. Snowflake marts refreshed. Tableau auto-refreshes via live connection.")

notify_task = PythonOperator(
    task_id         = "notify_success",
    python_callable = notify,
    dag             = dag,
    provide_context = True,
)

# ── Dependency chain ──────────────────────────────────────────
fetch_task >> load_task >> dbt_run_task >> dbt_test_task >> notify_task
