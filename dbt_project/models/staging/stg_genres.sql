-- ============================================================
-- stg_genres.sql  |  STAGING layer
-- Cleans TMDb genre reference table.
-- ============================================================

SELECT
    CAST(genre_id AS INTEGER)   AS genre_id,
    INITCAP(TRIM(genre_name))   AS genre_name
FROM {{ source('raw', 'genres') }}
WHERE genre_id IS NOT NULL
