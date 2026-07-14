CREATE TABLE IF NOT EXISTS bridge_movie_genre (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
   
    CONSTRAINT pk_bridge_movie_genre PRIMARY KEY (movie_id, genre_id),
    CONSTRAINT fk_movie FOREIGN KEY (movie_id) REFERENCES dim_movie(id) ON DELETE CASCADE,
    CONSTRAINT fk_genre FOREIGN KEY (genre_id) REFERENCES dim_genre(id) ON DELETE CASCADE
);
