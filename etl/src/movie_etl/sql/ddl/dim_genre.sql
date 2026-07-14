CREATE TABLE IF NOT EXISTS dim_genre (
    id INT PRIMARY KEY,
    name VARCHAR(20) NOT NULL,

    UNIQUE (name)
);
