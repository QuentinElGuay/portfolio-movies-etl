CREATE TABLE IF NOT EXISTS movie (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    genre VARCHAR(50) NOT NULL,
    release_date DATE NOT NULL,
    qty_ratings INT NOT NULL DEFAULT 0,
    avg_rating NUMERIC(3, 2),
    min_rating INT,
    max_rating INT,
    
    UNIQUE (name, release_date)
);
