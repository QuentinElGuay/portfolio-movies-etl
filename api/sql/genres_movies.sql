CREATE TABLE genres_movies AS
SELECT DISTINCT
    genre_id,
    movie_id
FROM stage_movies
;
