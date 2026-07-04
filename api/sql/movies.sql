CREATE TABLE movies AS
SELECT DISTINCT
    id,
    original_title,
    overview,
    release_date,
    revenue
    FROM stage_metadata
;
