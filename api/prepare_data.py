from pathlib import Path

import duckdb
import kagglehub


def download_to_stage(
    table_name: str,
    kaggle_handle: str,
    kaggle_path: str,
    columns: str = '*',
    ignore_errors=True,
):

    file_path = kagglehub.dataset_download(kaggle_handle, path=kaggle_path)

    rel = conn.read_csv(file_path, ignore_errors)

    conn.sql(f"""
    CREATE TEMPORARY TABLE stage_{table_name} AS
    SELECT {columns}
    FROM rel
    """)


data_folder = Path('data')
data_folder.mkdir(parents=True, exist_ok=True)
conn = duckdb.connect(data_folder / 'movies.db')

movies_file_path = kagglehub.dataset_download(
    'rounakbanik/the-movies-dataset', path='movies_metadata.csv'
)

ratings_file_path = kagglehub.dataset_download(
    'rounakbanik/the-movies-dataset', path='ratings_small.csv'
)

rel_metadata = conn.read_csv(movies_file_path, ignore_errors=True)
conn.sql("""
CREATE TEMPORARY TABLE stage_metadata AS
SELECT id, original_title, genres, overview
FROM rel_metadata
""")

conn.sql("""
CREATE TEMPORARY TABLE stage_movies AS                

WITH clean_json AS (
    SELECT
        id,
        original_title,
        overview,
        (REPLACE(genres, '''', '"')::JSON)::STRUCT(id INT, name VARCHAR)[] AS genres
    FROM stage_metadata
)

SELECT
    id AS movie_id,
    original_title AS movie_title,
    overview AS movie_overview,
    UNNEST(genres).id AS genre_id,
    UNNEST(genres).name AS genre_name
FROM clean_json      
""")

conn.sql("""
CREATE TABLE genres AS                
SELECT DISTINCT
    genre_id AS id,
    genre_name AS name
FROM stage_movies      
""")

conn.sql("""
CREATE TABLE genres_movies AS                
SELECT DISTINCT
    genre_id,
    movie_id
FROM stage_movies      
""")

conn.sql("""
CREATE TABLE movies AS                
SELECT DISTINCT
    id,
    original_title,
    overview
    FROM stage_metadata  
""")

rel_ratings = conn.read_csv(ratings_file_path, ignore_errors=True)
conn.sql('CREATE TEMPORARY TABLE stage_ratings AS SELECT * FROM rel_ratings')

conn.sql("""
CREATE TABLE ratings AS                
SELECT DISTINCT
    movieId AS movie_id,
    rating,
    timestamp
FROM stage_ratings      
""")

conn.close()
