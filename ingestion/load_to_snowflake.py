"""
load_to_snowflake.py
---------------------
Loads TMDb CSV files from data/raw/ into Snowflake RAW schema.

Tables created:
  RAW.TMDB_MOVIES  — main movie dataset
  RAW.GENRES       — genre reference

Usage:
    python load_to_snowflake.py
"""

import os, pandas as pd, snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

conn = snowflake.connector.connect(
    account   = os.getenv("SNOWFLAKE_ACCOUNT"),
    user      = os.getenv("SNOWFLAKE_USER"),
    password  = os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse = "COMPUTE_WH",
    database  = "STREAMING_DB",
    schema    = "RAW",
    role      = os.getenv("SNOWFLAKE_ROLE"),
)
cur = conn.cursor()
DATA_DIR = Path("data/raw")


def create_tables():
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW.TMDB_MOVIES (
            MOVIE_ID          NUMBER,
            TITLE             VARCHAR,
            ORIGINAL_TITLE    VARCHAR,
            ORIGINAL_LANGUAGE VARCHAR,
            OVERVIEW          VARCHAR(4096),
            RELEASE_DATE      DATE,
            POPULARITY        FLOAT,
            VOTE_AVERAGE      FLOAT,
            VOTE_COUNT        NUMBER,
            GENRE_IDS         VARCHAR,
            GENRE_NAMES       VARCHAR,
            PRIMARY_GENRE     VARCHAR,
            ADULT             BOOLEAN,
            POSTER_PATH       VARCHAR,
            _FETCHED_AT       TIMESTAMP_NTZ,
            _LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW.GENRES (
            GENRE_ID   NUMBER,
            GENRE_NAME VARCHAR
        )
    """)
    print("Tables ready")


def load_csv(filepath: Path, table: str):
    df = pd.read_csv(filepath)
    df.columns = [c.upper() for c in df.columns]

    # Type coercions
    if "RELEASE_DATE" in df.columns:
        df["RELEASE_DATE"] = pd.to_datetime(df["RELEASE_DATE"], errors="coerce").dt.date
    if "_FETCHED_AT" in df.columns:
        df["_FETCHED_AT"] = pd.to_datetime(df["_FETCHED_AT"], errors="coerce").dt.tz_localize(None)

    cur.execute(f"DROP TABLE IF EXISTS RAW.{table}")
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table, auto_create_table=True, overwrite=False
    )
    print(f"  {table}: {nrows:,} rows loaded (success={success})")


def main():
    create_tables()
    print("Loading movies...")
    load_csv(DATA_DIR / "tmdb_movies.csv", "TMDB_MOVIES")
    print("Loading genres...")
    load_csv(DATA_DIR / "genres.csv", "GENRES")
    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
