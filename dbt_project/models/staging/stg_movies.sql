-- ============================================================
-- stg_movies.sql  |  STAGING layer
-- Cleans and type-casts raw TMDb movie data.
-- ============================================================

WITH source AS (

    SELECT * FROM {{ source('raw', 'tmdb_movies') }}

),

cleaned AS (

    SELECT
        movie_id,
        TRIM(title)                                         AS title,
        TRIM(original_title)                                AS original_title,
        LOWER(original_language)                            AS original_language,

        -- Release date handling
        TRY_CAST(release_date AS DATE)                      AS release_date,
        EXTRACT(YEAR  FROM TRY_CAST(release_date AS DATE))  AS release_year,
        EXTRACT(MONTH FROM TRY_CAST(release_date AS DATE))  AS release_month,

        -- Scores
        ROUND(CAST(popularity   AS FLOAT), 3)               AS popularity_score,
        ROUND(CAST(vote_average AS FLOAT), 1)               AS vote_average,
        CAST(vote_count AS INTEGER)                         AS vote_count,

        -- Genres (pipe-delimited string → kept for downstream SPLIT usage)
        genre_ids,
        genre_names,
        primary_genre,

        -- Flags
        COALESCE(CAST(adult AS BOOLEAN), FALSE)             AS is_adult,

        -- Popularity tier (useful for segmentation)
        CASE
            WHEN CAST(popularity AS FLOAT) >= 100 THEN 'Blockbuster'
            WHEN CAST(popularity AS FLOAT) >= 20  THEN 'Popular'
            WHEN CAST(popularity AS FLOAT) >= 5   THEN 'Mainstream'
            ELSE 'Niche'
        END                                                 AS popularity_tier,

        -- Vote quality tier
        CASE
            WHEN CAST(vote_average AS FLOAT) >= 8.0 THEN 'Exceptional'
            WHEN CAST(vote_average AS FLOAT) >= 7.0 THEN 'Good'
            WHEN CAST(vote_average AS FLOAT) >= 6.0 THEN 'Average'
            WHEN CAST(vote_average AS FLOAT) >= 5.0 THEN 'Below Average'
            ELSE 'Poor'
        END                                                 AS rating_tier,

        poster_path,
        _fetched_at

    FROM source

    WHERE
        movie_id   IS NOT NULL
        AND title  IS NOT NULL
        AND CAST(vote_count AS INTEGER) >= 10
    QUALIFY ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY popularity_score DESC) = 1

)

SELECT * FROM cleaned
