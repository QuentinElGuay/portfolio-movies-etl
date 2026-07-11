CREATE TABLE movies AS
SELECT
    id,
    original_title,
    original_language,
    (REPLACE(genres, '''', '"')::JSON)::STRUCT(id INT, name VARCHAR)[] AS genres,
    overview,
    release_date,
    revenue
FROM stage_metadata
