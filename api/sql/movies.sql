CREATE TABLE movies AS
SELECT DISTINCT
    id,
    original_title,
    original_language,
    genres,
    overview,
    release_date,
    revenue
    FROM stage_metadata
;
