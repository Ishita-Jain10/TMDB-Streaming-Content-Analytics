-- ============================================================
-- dim_decade.sql  |  MARTS layer  (materialized: table)
-- Decade dimension with aggregated film stats.
-- ============================================================

SELECT
    decade,
    COUNT(*)                            AS total_movies,
    ROUND(AVG(vote_average), 2)         AS avg_rating,
    ROUND(AVG(popularity_score), 2)     AS avg_popularity,
    ROUND(AVG(engagement_score), 2)     AS avg_engagement,
    MIN(release_year)                   AS earliest_year,
    MAX(release_year)                   AS latest_year,

    -- Sort key for Tableau ordering
    CASE decade
        WHEN 'Pre-1990' THEN 1
        WHEN '1990s'    THEN 2
        WHEN '2000s'    THEN 3
        WHEN '2010s'    THEN 4
        WHEN '2020s'    THEN 5
        ELSE 6
    END                                 AS decade_sort

FROM {{ ref('int_movies_enriched') }}
WHERE decade IS NOT NULL
GROUP BY decade
