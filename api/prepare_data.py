import logging
import os
from pathlib import Path
import sys

import duckdb
from dotenv import load_dotenv
import kagglehub

load_dotenv()

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
)

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO').upper())
logger.addHandler(handler)


KAGGLE_HANDLE = 'rounakbanik/the-movies-dataset'
REQUIRED_TABLES = {'movies', 'ratings', 'genres', 'genres_movies'}


def download_to_stage(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    kaggle_handle: str,
    kaggle_path: str,
    columns: list[str] = ['*'],
    ignore_errors=True,
) -> None:

    file_path = kagglehub.dataset_download(kaggle_handle, path=kaggle_path)

    conn.sql(
        f"""
    CREATE TEMPORARY TABLE stage_{table_name} AS
    SELECT {','.join(columns)}
    FROM read_csv_auto('{file_path}', ignore_errors={str(ignore_errors).lower()})
    """
    )


def database_is_ready(db_path: Path) -> bool:

    if not db_path.exists():
        return False

    try:
        with duckdb.connect(db_path, read_only=True) as conn:
            tables = {row[0] for row in conn.sql('SHOW TABLES').fetchall()}

            if not REQUIRED_TABLES.issubset(tables):
                return False

            # Basic sanity check
            if conn.sql('SELECT COUNT(*) FROM movies').fetchone()[0] == 0:
                return False

            return True

    except duckdb.Error:
        return False


def prepare_database(db_path: Path) -> None:
    data_folder = db_path.parent
    data_folder.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(db_path) as conn:
        download_to_stage(
            conn,
            'metadata',
            KAGGLE_HANDLE,
            'movies_metadata.csv',
            columns=[
                'id',
                'original_title',
                'original_language',
                'genres',
                'overview',
                'release_date',
                'revenue',
            ],
        )

        download_to_stage(conn, 'ratings', KAGGLE_HANDLE, 'ratings_small.csv')

        conn.sql(Path('sql/stage_movies.sql').read_text())
        conn.sql(Path('sql/genres.sql').read_text())
        conn.sql(Path('sql/genres_movies.sql').read_text())
        conn.sql(Path('sql/movies.sql').read_text())
        conn.sql(Path('sql/ratings.sql').read_text())

        logger.info('Database prepared successfully.')


def main():

    MOVIES_DB_PATH = Path(os.environ['MOVIES_DB_PATH'])

    if database_is_ready(MOVIES_DB_PATH):
        logger.info('Database already prepared.')
        return

    prepare_database(MOVIES_DB_PATH)


if __name__ == '__main__':
    main()
