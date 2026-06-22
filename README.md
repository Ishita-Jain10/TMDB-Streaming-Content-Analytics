# Streaming Content Analytics Platform

End-to-end automated pipeline that fetches **10,000+ movies** from the TMDb API weekly, loads them into Snowflake, transforms via dbt, and visualizes trends in Tableau — orchestrated by Apache Airflow.

Built to demonstrate: REST API ingestion, Airflow DAG orchestration, Snowflake data warehousing, dbt modelling, and Tableau dashboards.

---

## Architecture

```
TMDb REST API (free, 500K+ movies)
        │
        ▼  (weekly, via Airflow)
  fetch_tmdb_movies task
  (Python — requests, pandas)
        │
        ▼
  load_snowflake task
  (snowflake-connector-python)
        │
        ▼
  Snowflake RAW Schema
  (TMDB_MOVIES, GENRES tables)
        │
        ▼
  dbt_run task
  ├── Staging Layer (views)
  │   stg_movies, stg_genres
  ├── Intermediate Layer (views)
  │   int_movies_enriched
  └── Marts Layer (tables)
      fact_movies, dim_genre, dim_decade
        │
        ▼
  Tableau Dashboard
  (Genre Intelligence | Trends | Rating Analysis)
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| API Source | TMDb (free tier, ~40 req/s) |
| Orchestration | Apache Airflow 2.8+ |
| Storage | Snowflake |
| Transformation | dbt Core |
| Visualization | Tableau Desktop / Tableau Public |

---

## Project Structure

```
project2_streaming_analytics/
├── ingestion/
│   ├── fetch_tmdb_movies.py     # Manual/standalone TMDb fetch script
│   └── load_to_snowflake.py     # Manual/standalone Snowflake loader
├── airflow/
│   └── dags/
│       └── tmdb_pipeline_dag.py # Full Airflow DAG (fetch → load → dbt run/test)
├── snowflake/
│   └── setup.sql                # One-time database/schema setup
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml             # → place in ~/.dbt/ (never commit!)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_movies.sql
│   │   │   ├── stg_genres.sql
│   │   │   └── schema.yml
│   │   ├── intermediate/
│   │   │   └── int_movies_enriched.sql
│   │   └── marts/
│   │       ├── fact_movies.sql
│   │       ├── dim_genre.sql
│   │       └── dim_decade.sql
│   └── tests/
│       ├── assert_engagement_score_range.sql
│       └── assert_vote_average_range.sql
└── dashboards/
    └── tableau_calcs.md         # All calculated fields + dashboard guide
```

---

## Setup & Run

### 1. TMDb API Key (free)

1. Sign up at https://www.themoviedb.org
2. Go to Settings → API → Request API Key (Developer / Personal)
3. Add to `.env`:

```
TMDB_API_KEY=your_bearer_token_here
SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

### 2. Snowflake Setup

Run `snowflake/setup.sql` in your Snowflake worksheet.

### 3. Option A — Run Manually

```bash
pip install requests pandas snowflake-connector-python python-dotenv dbt-snowflake

# Fetch ~10K movies from TMDb
python ingestion/fetch_tmdb_movies.py

# Load to Snowflake
python ingestion/load_to_snowflake.py

# Run dbt
cd dbt_project
cp profiles.yml ~/.dbt/profiles.yml
dbt deps && dbt run && dbt test
```

### 4. Option B — Run via Airflow (automated weekly)

```bash
pip install apache-airflow apache-airflow-providers-snowflake

# Copy DAG to Airflow dags folder
cp airflow/dags/tmdb_pipeline_dag.py ~/airflow/dags/

# Set Airflow Variables (UI or CLI)
airflow variables set TMDB_API_KEY "your_key"
airflow variables set SNOWFLAKE_ACCOUNT "your_account"
airflow variables set SNOWFLAKE_USER "your_user"
airflow variables set SNOWFLAKE_PASSWORD "your_pass"

# Start Airflow (dev mode)
airflow standalone

# Trigger manually or wait for Sunday 2 AM UTC
airflow dags trigger tmdb_streaming_pipeline
```

### 5. Tableau

See `dashboards/tableau_calcs.md` for connection guide, calculated fields, and 4-page dashboard design.

---

## Key Insights (from 10,000 movies)

- **Drama** dominates the catalog (28% of all movies), but **Animation** leads in avg engagement score
- **2020s movies** have the highest avg popularity but lower avg ratings than 2000s films
- **Non-English films** rate 0.4 pts higher on average than English films (hidden gem effect)
- Blockbuster tier (popularity ≥ 100) = top 2% of catalog; accounts for 45% of total votes

---

## Data Source

The Movie Database (TMDb) API — free for non-commercial use  
https://developer.themoviedb.org/docs  
License: TMDb API Terms of Use
