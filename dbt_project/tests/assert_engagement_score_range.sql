-- Engagement score must be between 0 and 100
SELECT *
FROM {{ ref('fact_movies') }}
WHERE engagement_score < 0 OR engagement_score > 100
