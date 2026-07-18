from dataclasses import dataclass
from datetime import UTC, datetime, timezone
import logging
import os
from pathlib import Path
from urllib3.util.retry import Retry

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from pipeline.datalake import DataLakeConfig, Dataset, FileFormat, LayerConfig
from pipeline.ingestion.api import (
    AUTH_ENDPOINT,
    GENRES_ENDPOINT,
    RATINGS_ENDPOINT,
    MOVIES_ENDPOINT,
    ApiClient,
    Endpoint,
)
from pipeline.ingestion.validation import validate_records
from pipeline.config import Settings
from pipeline.datalake import DataLake, Layer
from pipeline.load.database import Database
from pipeline.serialization.reader import NdjsonReader
from pipeline.serialization.writer import NdjsonWriter, WritersManager
from pipeline.storage.object_storage import ObjectStorage, StorageFactory

logger = logging.getLogger(__name__)


@dataclass
class ExecutionContext:
    """Object containing shared execution information"""

    datalake: DataLake
    run_start_time: datetime
    settings: Settings
    storage: ObjectStorage


def download_endpoint(
    context: ExecutionContext, api_client: ApiClient, endpoint: Endpoint
) -> Dataset:
    """
    Returns a string containing the prefix to the NdJson files created by the download of the
    endpoint.
    """
    bronze_dataset = Dataset(endpoint.name, Layer.BRONZE)
    quarantine = Dataset(endpoint.name, Layer.QUARANTINE)

    successes = 0
    errors = 0

    with WritersManager(
        datalake=context.datalake, writer_factory=NdjsonWriter
    ) as writers:
        for record in api_client.get_endpoint(endpoint.path):
            validation_result = validate_records(record, endpoint.model)
            if validation_result.model:
                writers.write(
                    dataset=bronze_dataset,
                    record=validation_result.model.model_dump(mode='json'),
                )
                successes += 1
            else:
                writers.write(
                    quarantine,
                    {
                        'endpoint': endpoint.path,
                        'retrieved_at': datetime.now(UTC).isoformat(),
                        'record': validation_result.record,
                        'validation_errors': validation_result.errors,
                    },
                )
                errors += 1

        logger.info('Downloaded %d %s from endpoint.', successes, endpoint.name)
        logger.info('%d downloaded %s went to quarantine.', errors, endpoint.name)

    return bronze_dataset.to_dict()


def extract(context: ExecutionContext) -> dict[str, Dataset]:
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

        datasets = {
            'genres': download_endpoint(context, api_client, GENRES_ENDPOINT),
            'movies': download_endpoint(context, api_client, MOVIES_ENDPOINT),
            'ratings': download_endpoint(context, api_client, RATINGS_ENDPOINT),
        }

    logger.info('EXTRACT STEP EXECUTED WITH SUCCESS')

    return datasets


def transform(
    context: ExecutionContext,
    datasets: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    """
    Execute the Transform step of the ETL process
    """
    logger.info('STARTING TRANSFORMATION STEP')

    results = []

    # TODO: create specific functions for each dataset
    df_dim_genre = (
        NdjsonReader(context.storage)
        .read_all(context.datalake, Dataset(**datasets['genres']))
        .reindex(columns=['id', 'name'])
    )

    df_dim_movie = (
        NdjsonReader(context.storage)
        .read_all(context.datalake, Dataset(**datasets['movies']))
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

# TODO: This dataset must be derived from "movies"
#    df_bridge_movie_genre = (
#        NdjsonReader(context.storage)
#        .read_all(datasets['genres_movies'])
#        .drop_duplicates(subset=['genre_id', 'movie_id'])
#        .reindex(columns=['movie_id', 'genre_id'])
#    )
    df_bridge_movie_genre = pd.DataFrame()

    df_rating = (
        NdjsonReader(context.storage)
        .read_all(context.datalake, Dataset(**datasets['ratings']))
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

    # TODO: this config should be defined in a file
    DATALAKE_CONFIG = DataLakeConfig(
        max_file_size_mb=128,
        layers={
            Layer.BRONZE: LayerConfig(
                root='bronze',
                format=FileFormat.NDJSON,
            ),
            Layer.QUARANTINE: LayerConfig(
                root='quarantine',
                format=FileFormat.NDJSON,
            ),
            Layer.SILVER: LayerConfig(
                root='silver',
                format=FileFormat.PARQUET,
            ),
            Layer.GOLD: LayerConfig(
                root='gold',
                format=FileFormat.PARQUET,
            ),
        },
    )

    STORAGE = StorageFactory.create('local', os.environ['LOCAL_STORAGE'])

    # Loading settings
    context = ExecutionContext(
        datalake=DataLake(DATALAKE_CONFIG, STORAGE),
        run_start_time=datetime.now(timezone.utc),
        settings=Settings.from_env(),
        storage=STORAGE,  # TODO: storage is probably not required since already in datalake
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
