CREATE TABLE genres AS
SELECT DISTINCT
    UNNEST(genres).id AS id,
    UNNEST(genres).name AS name

FROM movies
;
