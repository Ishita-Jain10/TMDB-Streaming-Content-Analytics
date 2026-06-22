-- Vote average must be 0–10
SELECT *
FROM {{ ref('fact_movies') }}
WHERE vote_average < 0 OR vote_average > 10
