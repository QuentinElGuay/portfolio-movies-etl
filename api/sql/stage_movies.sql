CREATE TEMPORARY TABLE stage_movies AS

WITH clean_json AS (
    SELECT
        id,
        original_title,
        overview,
        (REPLACE(genres, '''', '"')::JSON)::STRUCT(id INT, name VARCHAR)[] AS genres
    FROM stage_metadata
)

SELECT
    id AS movie_id,
    original_title AS movie_title,
    overview AS movie_overview,
    UNNEST(genres).id AS genre_id,
    UNNEST(genres).name AS genre_name
FROM clean_json
;
