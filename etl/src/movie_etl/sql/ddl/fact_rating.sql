CREATE TABLE IF NOT EXISTS fact_rating (
    user_id INT NOT NULL,
    movie_id INT NOT NULL,
    rating SMALLINT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,

    CONSTRAINT pk_rating PRIMARY KEY (user_id, movie_id),
    CONSTRAINT chk_rating_range CHECK (rating BETWEEN 0 AND 5),
    CONSTRAINT fk_movie FOREIGN KEY (movie_id) REFERENCES dim_movie(id) ON DELETE CASCADE
);
