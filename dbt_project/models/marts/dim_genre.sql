-- ============================================================
-- dim_genre.sql  |  MARTS layer  (materialized: table)
-- Genre dimension with aggregated stats per genre.
-- ============================================================

WITH genre_stats AS (

    SELECT
        primary_genre                       AS genre_name,
        COUNT(*)                            AS total_movies,
        ROUND(AVG(vote_average), 2)         AS avg_rating,
        ROUND(AVG(popularity_score), 2)     AS avg_popularity,
        ROUND(AVG(engagement_score), 2)     AS avg_engagement,
        MAX(vote_average)                   AS max_rating,
        SUM(vote_count)                     AS total_votes

    FROM {{ ref('int_movies_enriched') }}
    WHERE primary_genre IS NOT NULL
    GROUP BY 1

),

genre_ref AS (

    SELECT genre_id, genre_name FROM {{ ref('stg_genres') }}

)

SELECT
    r.genre_id,
    s.genre_name,
    s.total_movies,
    s.avg_rating,
    s.avg_popularity,
    s.avg_engagement,
    s.max_rating,
    s.total_votes,

    -- Rank by total votes (audience reach)
    RANK() OVER (ORDER BY s.total_votes DESC)   AS audience_reach_rank,
    RANK() OVER (ORDER BY s.avg_rating DESC)    AS quality_rank

FROM genre_stats s
LEFT JOIN genre_ref r ON LOWER(s.genre_name) = LOWER(r.genre_name)
