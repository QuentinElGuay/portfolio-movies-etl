CREATE TABLE ratings AS
SELECT DISTINCT
    userId AS user_id,
    movieId AS movie_id,
    rating,
    "timestamp"
FROM stage_ratings
;
