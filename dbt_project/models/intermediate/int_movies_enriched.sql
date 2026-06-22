-- ============================================================
-- int_movies_enriched.sql  |  INTERMEDIATE layer
-- Adds decade bucketing, genre count, and engagement score.
-- Feeds directly into fact_movies and dim_genre_bridge.
-- ============================================================

WITH movies AS (

    SELECT * FROM {{ ref('stg_movies') }}

),

enriched AS (

    SELECT
        -- ── Identifiers ───────────────────────────────────
        movie_id,
        title,
        original_title,
        original_language,

        -- ── Dates ─────────────────────────────────────────
        release_date,
        release_year,
        release_month,

        -- Decade bucket (1990s, 2000s, 2010s, 2020s)
        CASE
            WHEN release_year < 1990 THEN 'Pre-1990'
            WHEN release_year < 2000 THEN '1990s'
            WHEN release_year < 2010 THEN '2000s'
            WHEN release_year < 2020 THEN '2010s'
            ELSE '2020s'
        END                                             AS decade,

        -- ── Scores ────────────────────────────────────────
        popularity_score,
        vote_average,
        vote_count,
        popularity_tier,
        rating_tier,
        primary_genre,
        genre_names,

        -- Number of genres this movie belongs to
        ARRAY_SIZE(SPLIT(NULLIF(genre_names,''), '|'))  AS genre_count,

        -- Engagement score: weighted combo of popularity + votes + rating
        -- Scale: 0–100 (normalized)
        ROUND(
            (popularity_score / NULLIF(
                MAX(popularity_score) OVER (), 1) * 40)   -- 40% weight: popularity
            + (vote_average / 10.0 * 40)                  -- 40% weight: rating
            + (LEAST(vote_count, 10000) / 10000.0 * 20),  -- 20% weight: vote volume (capped)
        2)                                                AS engagement_score,

        -- ── Flags ─────────────────────────────────────────
        is_adult,
        CASE WHEN original_language = 'en' THEN TRUE ELSE FALSE END AS is_english,

        -- ── Lineage ───────────────────────────────────────
        poster_path,
        _fetched_at

    FROM movies

)

SELECT * FROM enriched
