CREATE TABLE IF NOT EXISTS dim_movie (
      id INT PRIMARY KEY
    , title VARCHAR(200) NOT NULL
    , release_date DATE NULL
    , original_language VARCHAR(50) NULL
    , overview TEXT NULL
    , revenue BIGINT NULL

    , UNIQUE (title, release_date)
)
;
