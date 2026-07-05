CREATE TABLE genres AS
SELECT DISTINCT
    genre_id AS id,
    genre_name AS name
FROM stage_movies
;
