from dataclasses import dataclass
from datetime import UTC, datetime, timezone
import logging
import os
from pathlib import Path
from urllib3.util.retry import Retry

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from movie_etl.ingestion.api import (
    AUTH_ENDPOINT,
    GENRES_ENDPOINT,
    RATINGS_ENDPOINT,
    MOVIES_ENDPOINT,
    ApiClient,
    Endpoint,
)
from movie_etl.ingestion.api_models import Genre, Movie, MovieGenre
from movie_etl.ingestion.validation import validate_records
from movie_etl.config import Settings
from etl.src.movie_etl.load.database import Database
from etl.src.movie_etl.files.storage import (
    NdjsonReader,
    NdjsonWriter,
    ObjectStorage,
    StorageFactory,
)

logger = logging.getLogger(__name__)


@dataclass
class ExecutionContext:
    """Object containing shared execution information"""

    run_start_time: datetime
    settings: Settings
    storage: ObjectStorage


def download_endpoint(
    context: ExecutionContext, api_client: ApiClient, endpoint: Endpoint
) -> str:
    """
    Returns a string containing the prefix to the NdJson files created by the download of the
    endpoint.
    """
    start_time = context.run_start_time.date().isoformat()
    success_prefix = f'movie_api/bronze/{endpoint.name}/date={start_time}/'
    error_prefix = f'movie_api/quarantine/{endpoint.name}/date={start_time}/'

    successes = 0
    errors = 0

    with (
        NdjsonWriter(context.storage, prefix=success_prefix) as success_writer,
        NdjsonWriter(context.storage, prefix=error_prefix) as error_writer,
    ):
        for genre in api_client.get_endpoint(endpoint.path):
            validation_result = validate_records(genre, Genre)
            if validation_result.model:
                success_writer.write(validation_result.model)
                successes += 1
            else:
                error_writer.write(
                    {
                        'endpoint': endpoint.path,
                        'retrieved_at': datetime.now(UTC).isoformat(),
                        'record': validation_result.record,
                        'validation_errors': validation_result.errors,
                    }
                )
                errors += 1

        logger.info('Downloaded %d %s(s) from endpoint.', successes, endpoint.name)
        logger.info('%d downloaded %s(s) went to quarantine.', errors, endpoint.name)

    return success_prefix


# TODO: check if it would be possible to use the generic download_endpoint function using
# recursivity
def download_movies(
    context: ExecutionContext, api_client: ApiClient
) -> tuple[str, str]:
    """
    Returns a list of strings containing the path to the downloaded ndjson files of relations movies returned by the API endpoints.
    """
    start_time = context.run_start_time.date().isoformat()
    prefix_movies_success = f'movie_api/bronze/movies/date={start_time}/'
    prefix_movies_error = f'movie_api/quarantine/movies/date={start_time}/'
    prefix_movies_genres_success = f'movie_api/bronze/movies_genres/date={start_time}/'
    prefix_movies_genres_error = (
        f'movie_api/quarantine/movies_genres/date={start_time}/'
    )

    movies_successes = 0
    movies_errors = 0
    movies_genres_successes = 0
    movies_genres_errors = 0

    with (
        NdjsonWriter(
            context.storage, prefix=prefix_movies_success
        ) as movies_success_writer,
        NdjsonWriter(
            context.storage, prefix=prefix_movies_error
        ) as movies_error_writer,
        NdjsonWriter(
            context.storage, prefix=prefix_movies_genres_success
        ) as movies_genres_success_writer,
        NdjsonWriter(
            context.storage, prefix=prefix_movies_genres_error
        ) as movies_genres_error_writer,
    ):
        for movie in api_client.get_endpoint(MOVIES_ENDPOINT.path):
            validation_result = validate_records(movie, Movie)
            if validation_result.model:
                movies_success_writer.write(validation_result.model)
                movies_successes += 1

                # TODO: check how to create the MovieGenre models in the movie model automatically
                genres = movie.pop('genres')
                for genre in genres:
                    validation_result = validate_records(
                        {'movie_id': movie['id'], 'genre_id': genre['id']}, MovieGenre
                    )
                    if validation_result.model:
                        movies_genres_success_writer.write(validation_result.model)
                        movies_genres_successes += 1
                    else:
                        movies_genres_error_writer.write(
                            {
                                'endpoint': MOVIES_ENDPOINT.path,
                                'retrieved_at': datetime.now(UTC).isoformat(),
                                'record': validation_result.record,
                                'validation_errors': validation_result.errors,
                            }
                        )
                        movies_genres_errors += 1

            else:
                movies_error_writer.write(
                    {
                        'endpoint': MOVIES_ENDPOINT,
                        'retrieved_at': datetime.now(UTC).isoformat(),
                        'record': validation_result.record,
                        'validation_errors': validation_result.errors,
                    }
                )
                movies_errors += 1

        logger.info('Downloaded %d movie(s) from endpoint.', movies_successes)
        logger.info('%d downloaded movie(s) went to quarantine.', movies_errors)

    return (
        prefix_movies_success,
        prefix_movies_genres_success,
    )


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
            AUTH_ENDPOINT.path,
            context.settings.api_username,
            context.settings.api_password,
        )

        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

        session.headers.update(headers)

        movies_dataset, genres_movies_dataset = download_movies(context, api_client)
        dataset_paths = {
            'genres': download_endpoint(context, api_client, GENRES_ENDPOINT),
            'genres_movies': genres_movies_dataset,
            'movies': movies_dataset,
            'ratings': download_endpoint(context, api_client, MOVIES_ENDPOINT),
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
