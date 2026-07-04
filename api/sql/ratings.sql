CREATE TABLE ratings AS
SELECT DISTINCT
    movieId AS movie_id,
    rating,
    "timestamp"
FROM stage_ratings
;
