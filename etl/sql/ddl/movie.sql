CREATE TABLE IF NOT EXISTS movie (
    id INT PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    genres VARCHAR(50)[] NOT NULL,
    release_date DATE DEFAULT NULL,
    language VARCHAR(50) DEFAULT NULL,
    overview TEXT,
    revenue BIGINT DEFAULT NULL,
    qty_ratings INT NOT NULL DEFAULT 0,
    avg_rating NUMERIC(3, 2),
    min_rating INT,
    max_rating INT,

    UNIQUE (title, release_date)
);
