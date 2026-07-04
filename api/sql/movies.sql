CREATE TABLE movies AS
SELECT DISTINCT
    id,
    original_title,
    overview
    FROM stage_metadata
;
