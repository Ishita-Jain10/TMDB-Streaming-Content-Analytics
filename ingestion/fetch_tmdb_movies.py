"""
fetch_tmdb_movies.py
--------------------
Fetches movie metadata from TMDb API → saves CSV for Snowflake ingestion.
Free API key: https://www.themoviedb.org/settings/api

Output: data/raw/tmdb_movies.csv  (up to 10,000 movies per run)

Usage:
    pip install requests pandas python-dotenv
    python fetch_tmdb_movies.py
"""

import os, time, json, requests, pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_KEY  = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3"
OUT_DIR  = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"accept": "application/json"}
PARAMS_BASE = {"api_key": API_KEY}


def fetch_genres() -> dict:
    r = requests.get(f"{BASE_URL}/genre/movie/list", headers=HEADERS, params=PARAMS_BASE)
    r.raise_for_status()
    return {g["id"]: g["name"] for g in r.json()["genres"]}


def fetch_movies(max_pages: int = 500) -> list[dict]:
    """TMDb caps discover at 500 pages × 20 = 10,000 results."""
    movies = []
    for page in range(1, max_pages + 1):
        params = {
            "sort_by": "popularity.desc",
            "include_adult": "false",
            "page": page,
            "vote_count.gte": 10,
        }
        r = requests.get(f"{BASE_URL}/discover/movie", headers=HEADERS, params={**PARAMS_BASE, **params})
        if r.status_code == 429:
            print(f"\nRate limited p{page}, sleeping 10s...")
            time.sleep(10)
            r = requests.get(f"{BASE_URL}/discover/movie", headers=HEADERS, params={**PARAMS_BASE, **params})
        r.raise_for_status()
        data = r.json()
        movies.extend(data["results"])
        print(f"  p{page}/{min(max_pages, data['total_pages'])} — {len(movies)} movies", end="\r")
        if page >= data["total_pages"]:
            break
        time.sleep(0.04)   # ~25 req/s, under free-tier limit of 40
    print(f"\nFetched {len(movies):,} movies")
    return movies


def flatten(movies: list[dict], genre_map: dict) -> pd.DataFrame:
    rows = []
    for m in movies:
        genres = [genre_map.get(g, "Unknown") for g in m.get("genre_ids", [])]
        rows.append({
            "movie_id":          m.get("id"),
            "title":             m.get("title"),
            "original_title":    m.get("original_title"),
            "original_language": m.get("original_language"),
            "overview":          m.get("overview"),
            "release_date":      m.get("release_date"),
            "popularity":        m.get("popularity"),
            "vote_average":      m.get("vote_average"),
            "vote_count":        m.get("vote_count"),
            "genre_ids":         json.dumps(m.get("genre_ids", [])),
            "genre_names":       "|".join(genres),
            "primary_genre":     genres[0] if genres else None,
            "adult":             m.get("adult"),
            "poster_path":       m.get("poster_path"),
            "_fetched_at":       pd.Timestamp.utcnow().isoformat(),
        })
    return pd.DataFrame(rows)


def main():
    print("Fetching genres...")
    genre_map = fetch_genres()
    pd.DataFrame([{"genre_id": k, "genre_name": v} for k, v in genre_map.items()])\
      .to_csv(OUT_DIR / "genres.csv", index=False)
    print(f"  {len(genre_map)} genres saved")

    print("Fetching movies...")
    movies = fetch_movies(max_pages=500)

    df = flatten(movies, genre_map)
    df.to_csv(OUT_DIR / "tmdb_movies.csv", index=False)
    print(f"Saved {len(df):,} rows → {OUT_DIR}/tmdb_movies.csv")


if __name__ == "__main__":
    main()
