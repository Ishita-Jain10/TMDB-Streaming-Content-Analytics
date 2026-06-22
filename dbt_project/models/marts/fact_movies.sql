-- ============================================================
-- fact_movies.sql  |  MARTS layer  (materialized: table)
-- One row per movie. Core fact table for Tableau dashboards.
-- ============================================================

SELECT
    -- ── Keys ──────────────────────────────────────────────
    movie_id,
    primary_genre,

    -- ── Descriptors ───────────────────────────────────────
    title,
    original_language,
    release_date,
    release_year,
    release_month,
    decade,

    -- ── Measures ──────────────────────────────────────────
    popularity_score,
    vote_average,
    vote_count,
    engagement_score,
    genre_count,

    -- ── Segments ──────────────────────────────────────────
    popularity_tier,
    rating_tier,
    is_adult,
    is_english,

    -- ── Raw genre string (for Tableau SPLIT / LOD) ────────
    genre_names,

    -- ── Lineage ───────────────────────────────────────────
    _fetched_at

FROM {{ ref('int_movies_enriched') }}
