from dataclasses import dataclass
from datetime import datetime, timezone
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
from movie_etl.storage import NdjsonReader, NdjsonWriter, ObjectStorage, StorageFactory

logger = logging.getLogger(__name__)


@dataclass
class ExecutionContext:
    """Object containing shared execution information"""

    run_start_time: datetime
    settings: Settings
    storage: ObjectStorage


def get_genres(context: ExecutionContext, api_client: ApiClient) -> str:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of genres returned by the API endpoints.
    """
    prefix = f'bronze/genres/date={context.run_start_time.date().isoformat()}/'

    with NdjsonWriter(context.storage, prefix=prefix) as writer:
        nb_records = 0
        for genre in api_client.get_endpoint(GENRES_ENDPOINT):
            writer.write(genre)
            nb_records += 1
        logger.info('Downloaded %d genres from endpoint.', nb_records)

    return prefix


def get_movies(context: ExecutionContext, api_client: ApiClient) -> tuple[str, str]:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of relations movies returned by the API endpoints.
    """
    prefix_movies = f'bronze/movies/date={context.run_start_time.date().isoformat()}/'
    prefix_genres_movies = (
        f'bronze/genres_movies/date={context.run_start_time.date().isoformat()}/'
    )

    with (
        NdjsonWriter(context.storage, prefix=prefix_movies) as movies_writer,
        NdjsonWriter(
            context.storage, prefix=prefix_genres_movies
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
                    {'movie_id': movie['id'], 'genre_id': genre['id']}
                )
                nb_records_genres_movies += 1
        logger.info('Downloaded %d movies from endpoint.', nb_records_movies)

    return (
        prefix_movies,
        prefix_genres_movies,
    )


def get_ratings(context: ExecutionContext, api_client: ApiClient) -> str:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of relations movie/ratings returned by the API endpoints.
    """
    prefix = f'bronze/ratings/date={context.run_start_time.date().isoformat()}/'

    with NdjsonWriter(context.storage, prefix=prefix) as writer:
        nb_records = 0
        for movie_ratings in api_client.get_endpoint(MOVIE_RATINGS_ENDPOINT):
            writer.write(movie_ratings)
            nb_records += 1

    logger.info('Downloaded %d ratings from endpoint.', nb_records)

    return prefix


def extract(context: ExecutionContext) -> dict[str, str]:
    """
    Execute the Extract step of the ETL process
    """
    logger.info('STARTING EXTRACT STEP')

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

        api_client = ApiClient(session, context.settings)

        token = api_client.get_auth(
            AUTH_ENDPOINT,
            context.settings.api_username,
            context.settings.api_password,
        )

        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

        session.headers.update(headers)

        movies_dataset, genres_movies_dataset = get_movies(context, api_client)
        dataset_paths = {
            'genres': get_genres(context, api_client),
            'genres_movies': genres_movies_dataset,
            'movies': movies_dataset,
            'ratings': get_ratings(context, api_client),
        }

    logger.debug(dataset_paths)

    logger.info('EXTRACT STEP EXECUTED WITH SUCCESS')

    return dataset_paths


def transform(
    context: ExecutionContext,
    datasets: dict[str, str],
) -> list[dict[str, str]]:
    """
    Execute the Transform step of the ETL process
    """
    logger.info('STARTING TRANSFORMATION STEP')

    results = []

    # TODO: create specific functions for each dataset
    df_dim_genre = (
        NdjsonReader(context.storage)
        .read_all(datasets['genres'])
        .reindex(columns=['id', 'name'])
    )

    df_dim_movie = (
        NdjsonReader(context.storage)
        .read_all(datasets['movies'])
        .assign(
            release_date=lambda df: (
                pd.to_datetime(
                    df['release_date'],
                    errors='coerce',
                ).dt.date
            )
        )
        .drop_duplicates(subset=['id'])
        .reindex(
            columns=[
                'id',
                'title',
                'release_date',
                'original_language',
                'overview',
                'revenue',
            ]
        )
    )

    df_bridge_movie_genre = (
        NdjsonReader(context.storage)
        .read_all(datasets['genres_movies'])
        .drop_duplicates(subset=['genre_id', 'movie_id'])
        .reindex(columns=['movie_id', 'genre_id'])
    )

    df_rating = (
        NdjsonReader(context.storage)
        .read_all(datasets['ratings'])
        .dropna()
        .reindex(columns=['user_id', 'movie_id', 'rating', 'timestamp'])
    )

    df_fact_rating = df_rating[df_rating['movie_id'].isin(df_dim_movie['id'])]

    dfs = {
        'dim_genre': df_dim_genre,
        'dim_movie': df_dim_movie,
        'bridge_movie_genre': df_bridge_movie_genre,
        'fact_rating': df_fact_rating,
    }

    for name, df in dfs.items():
        output_path = context.storage.uri(
            f'silver/{name}/{context.run_start_time.date().isoformat()}/'
        )
        Path(output_path).mkdir(parents=True, exist_ok=True)

        df.to_parquet(f'{output_path}/part-0000.parquet', compression='snappy')
        results.append({'name': name, 'path': output_path})

    logger.info('TRANSFORM STEP EXECUTED WITH SUCCESS')

    return results


def load(
    context: ExecutionContext,
    datasets: list[dict[str, str]],
):
    """
    Execute the Load step from the ETL process.
    """
    logger.info('- STARTING LOAD STEP -')

    database = Database(context.settings)

    tables = {
        dataset['name']: pd.read_parquet(context.storage.uri(dataset['path']))
        for dataset in datasets
    }

    for table, df in tables.items():
        database.create_table(table)
        database.upsert(table, df)

    # logger.info(
    #     'The "movie" table currently contains %s line(s)', database.count_movies()
    # )
    # logger.info('The "movie" table now contains %s line(s)', database.count_movies())

    logger.info('LOAD STEP EXECUTED WITH SUCCCESS')


def run():

    # Loading settings
    context = ExecutionContext(
        run_start_time=datetime.now(timezone.utc),
        settings=Settings.from_env(),
        storage=StorageFactory.create('local', os.environ['LOCAL_STORAGE']),
    )

    # Running the ETL process
    logger.info('STARTING ETL PROCESS')
    ingestion_result = extract(context)
    transformation_result = transform(context, ingestion_result)
    load(context, transformation_result)

    logger.info('ETL PROCESS EXECUTED WITH SUCCESS')

    logger.info('Hopefully you liked my work.')


if __name__ == '__main__':
    run()
