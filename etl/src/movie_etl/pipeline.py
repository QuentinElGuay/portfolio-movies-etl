from datetime import date
import logging
import os
from pathlib import Path
from urllib3.util.retry import Retry

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from movie_etl.api import (
    AUTH_ENDPOINT,
    GENRES_ENDPOINT,
    MOVIE_RATINGS_ENDPOINT,
    MOVIES_ENDPOINT,
    ApiClient,
)
from movie_etl.config import Settings
from movie_etl.database import Database
from movie_etl.storage import NdjsonReader, NdjsonWriter, StorageFactory

logger = logging.getLogger(__name__)


def get_genres(api_client: ApiClient) -> str:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of genres returned by the API endpoints.
    """
    prefix = f'genres/date={date.today().isoformat()}/'

    with NdjsonWriter(StorageFactory.create('local'), prefix=prefix) as writer:
        nb_records = 0
        for genre in api_client.get_endpoint(GENRES_ENDPOINT):
            r = writer.write(genre)
            nb_records += 1
        logger.info('Downloaded %d genres from endpoint.', nb_records)

    return prefix


def get_movies(api_client: ApiClient) -> tuple[str, str]:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of relations movies returned by the API endpoints.
    """
    prefix_movies = f'movies/date={date.today().isoformat()}/'
    prefix_genres_movies = f'genres_movies/date={date.today().isoformat()}/'

    with (
        NdjsonWriter(
            StorageFactory.create('local'), prefix=prefix_movies
        ) as movies_writer,
        NdjsonWriter(
            StorageFactory.create('local'), prefix=prefix_genres_movies
        ) as genres_movies_writer,
    ):
        nb_records_movies = 0
        nb_records_genres_movies = 0

        for movie in api_client.get_endpoint(MOVIES_ENDPOINT):
            genres = movie.pop('genres')

            movies_writer.write(movie)
            nb_records_movies += 1

            for genre in genres:
                genres_movies_writer.write(
                    {'genre_id': genre['id'], 'movie_id': movie['id']}
                )
                nb_records_genres_movies += 1
        logger.info('Downloaded %d movies from endpoint.', nb_records_movies)

    return (
        prefix_movies,
        prefix_genres_movies,
    )


def get_movie_ratings(api_client: ApiClient) -> str:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of relations movie/ratings returned by the API endpoints.
    """
    prefix = f'ratings/date={date.today().isoformat()}/'

    with NdjsonWriter(StorageFactory.create('local'), prefix=prefix) as writer:
        nb_records = 0
        for movie_ratings in api_client.get_endpoint(MOVIE_RATINGS_ENDPOINT):
            writer.write(movie_ratings)
            nb_records += 1

    logger.info('Downloaded %d ratings from endpoint.', nb_records)

    return prefix


def extract(
    settings: Settings,
) -> dict[str, str]:
    """
    Execute the Extract step of the ETL process
    """
    logger.info('- STARTING EXTRACT STEP -')

    with requests.Session() as session:
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET', 'POST'],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        api_client = ApiClient(session, settings)

        token = api_client.get_auth(
            AUTH_ENDPOINT,
            settings.api_username,
            settings.api_password,
        )

        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

        session.headers.update(headers)

        movies_dataset, genres_movies_dataset = get_movies(api_client)
        dataset_paths = {
            'genres': get_genres(api_client),
            'genres_movies': genres_movies_dataset,
            'movies': movies_dataset,
            'movies_ratings': get_movie_ratings(api_client),
        }

    logger.info(dataset_paths)

    logger.info('- EXTRACT STEP EXECUTED WITH SUCCESS -')

    return dataset_paths


def transform(datasets: dict[str, str]) -> pd.DataFrame:
    """
    Execute the Transform step of the ETL process
    """
    logger.info('- STARTING TRANSFORMATION STEP -')

    storage = StorageFactory.create('local')

    # TODO: replace hardcoded path using storage object
    df_genres = (
        NdjsonReader(Path(os.environ['LOCAL_STORAGE']) / datasets['genres']).read_all()
    ).rename(columns={'name': 'genre'})

    # TODO: replace hardcoded path using storage object
    df_movies = (
        NdjsonReader(Path(os.environ['LOCAL_STORAGE']) / datasets['movies'])
        .read_all()
        .drop_duplicates(subset=['id'])
    )

    # TODO: replace hardcoded path using storage object
    df_genres_movies = (
        NdjsonReader(Path(os.environ['LOCAL_STORAGE']) / datasets['genres_movies'])
        .read_all()
        .drop_duplicates(subset=['genre_id', 'movie_id'])
    )

    # TODO: replace hardcoded path using storage object
    df_movies_ratings = NdjsonReader(
        Path(os.environ['LOCAL_STORAGE']) / datasets['movies_ratings']
    ).read_all()

    # Aggregate ratings by movie
    df_aggregations = df_movies_ratings.groupby(['movie_id'], as_index=False).agg(
        qty_ratings=('rating', 'count'),
        avg_rating=('rating', 'mean'),
        min_rating=('rating', 'min'),
        max_rating=('rating', 'max'),
    )

    # Join DataFrames into one
    df_merge = (
        df_genres_movies.merge(df_genres, left_on='genre_id', right_on='id')
        .drop(columns=['genre_id'])
        .groupby('movie_id')['genre']
        .agg(list)
        .reset_index()
        .merge(df_movies, left_on='movie_id', right_on='id')
        .drop(columns=['id'])
        .merge(df_aggregations, how='left', on='movie_id')
        .rename(
            columns={
                'movie_id': 'id',
                'genre': 'genres',
            }
        )
    )

    df_exportation = df_merge.reindex(
        columns=[
            'id',
            'title',
            'genres',
            'overview',
            'qty_ratings',
            'avg_rating',
            'min_rating',
            'max_rating',
        ]
    )

    # Fill missing data for movies without rating
    df_exportation['qty_ratings'] = df_exportation['qty_ratings'].fillna(0).astype(int)

    logger.debug('Length of df_exportation: %s', len(df_exportation))
    logger.info('- TRANSFORM STEP EXECUTED WITH SUCCESS -')

    return df_exportation


def load(df: pd.DataFrame, settings: Settings):
    """
    Execute the Load step from the ETL process.
    """
    logger.info('- STARTING LOAD STEP -')

    database = Database(settings)
    database.create_movie_table()

    logger.info(
        'The "movie" table currently contains %s line(s)', database.count_movies()
    )
    database.load_movies(df)
    logger.info('The "movie" table now contains %s line(s)', database.count_movies())

    logger.info('- LOAD STEP EXECUTED WITH SUCCCESS -')


def run():

    # Loading settings
    settings = Settings.from_env()

    # Running the ETL process
    logger.info('-- STARTING ETL PROCESS --')
    load(transform(extract(settings)), settings)
    logger.info('-- ETL PROCESS EXECUTED WITH SUCCESS --')

    logger.info('Hopefully you liked my work.')


if __name__ == '__main__':
    run()
