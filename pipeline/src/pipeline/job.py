from dataclasses import dataclass
from datetime import UTC, date, datetime, timezone  # TODO: use TZ configuration
from enum import StrEnum
import logging
import os
from pathlib import Path
from pydantic import BaseModel
from urllib3.util.retry import Retry
from uuid import uuid7

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
from pipeline.ingestion.validation import validate_record
from pipeline.config import Settings
from pipeline.datalake import DataLake, Layer
from pipeline.load.database import Database
from pipeline.serialization.reader import NdjsonReader
from pipeline.serialization.writer import NdjsonWriter, WritersManager
from pipeline.storage.object_storage import ObjectStorage, StorageFactory

logger = logging.getLogger(__name__)


class IngestionStatus(StrEnum):
    """Endpoint download status."""

    SUCCESS = 'success'
    PARTIAL = 'partial'
    FAILED = 'failed'


@dataclass
class ExecutionContext:
    """Object containing shared execution information"""

    datalake: DataLake
    run_start_time: datetime
    settings: Settings
    storage: ObjectStorage
    run_id: str


class IngestionMetadata(BaseModel):
    run_id: str
    dataset: str
    layer: str
    endpoint: str
    snapshot_date: date

    started_at: datetime
    finished_at: datetime

    status: IngestionStatus

    records_valid: int
    records_invalid: int
    unexpected_fields: set[str]

    schema_version: int = 1


@dataclass(frozen=True)
class IngestionResult:
    dataset: Dataset
    endpoint: str

    started_at: datetime
    finished_at: datetime

    status: IngestionStatus

    records_valid: int
    records_invalid: int
    unexpected_fields: set[str]


@dataclass(frozen=True)
class DatasetReference:
    dataset_name: str
    dataset_uri: str


@dataclass(frozen=True)
class IngestionOutput:
    run_id: str
    datasets: list[DatasetReference]


def ingest_endpoint(
    context: ExecutionContext, api_client: ApiClient, endpoint: Endpoint
) -> IngestionResult:
    """
    Ingest the data returned by a collection endpoint by writing the result in the datalake
    """
    bronze_dataset = Dataset(
        endpoint.name,
        Layer.BRONZE,
        context.run_start_time.date(),
        context.run_id,
    )
    quarantine = Dataset(
        endpoint.name,
        Layer.QUARANTINE,
        context.run_start_time.date(),
        context.run_id,
    )

    status = IngestionStatus.SUCCESS
    started_at = datetime.now(UTC)
    records_valid = 0
    records_invalid = 0

    expected_fields = frozenset(endpoint.model.model_fields)
    unexpected_fields: set[str] = set()

    try:
        with WritersManager(
            datalake=context.datalake, writer_factory=NdjsonWriter
        ) as writers:
            for record in api_client.get_endpoint(endpoint.path):
                unexpected_fields.update(record.keys() - expected_fields)

                validation_result = validate_record(record, endpoint.model)
                if validation_result.model:
                    writers.write(
                        dataset=bronze_dataset,
                        record=validation_result.model.model_dump(mode='json'),
                    )
                    records_valid += 1
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
                    records_invalid += 1
    except Exception:
        status = IngestionStatus.FAILED
        raise

    logger.info('Downloaded %d %s from endpoint.', records_valid, endpoint.name)

    if records_invalid > 0:
        logger.warning(
            '%d downloaded %s went to quarantine.', records_invalid, endpoint.name
        )

        status = IngestionStatus.PARTIAL

    # TODO: temporary, unexpected_fields should raise an alert and be stored in the manifest file
    if len(unexpected_fields) > 0:
        logger.warning(
            'Found %s unexpected field(s) for endpoint %s: %s.',
            len(unexpected_fields),
            endpoint.name,
            unexpected_fields,
        )

    ingestion_result = IngestionResult(
        dataset=bronze_dataset,
        endpoint=endpoint.path,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        status=status,
        records_valid=records_valid,
        records_invalid=records_invalid,
        unexpected_fields=unexpected_fields,
    )

    return ingestion_result


def extract(context: ExecutionContext) -> dict[str, Dataset]:
    """
    Execute the Extract step of the ETL process
    """
    logger.info('STARTING EXTRACT STEP')

    endpoints = (GENRES_ENDPOINT, MOVIES_ENDPOINT, RATINGS_ENDPOINT)

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

        dataset_references: list[DatasetReference] = []

        for endpoint in endpoints:
            result = ingest_endpoint(context, api_client, endpoint)

            dataset_reference = DatasetReference(
                dataset_name=result.dataset.name,
                dataset_uri=context.datalake.prefix(result.dataset),
            )
            dataset_references.append(dataset_reference)

            metadata = IngestionMetadata(
                    run_id=context.run_id,
                    dataset=result.dataset.name,
                    layer=result.dataset.layer.value,
                    endpoint=endpoint.path,
                    snapshot_date=result.dataset.snapshot_date,
                    started_at=result.started_at,
                    finished_at=result.finished_at,
                    status=result.status,
                    records_valid=result.records_valid,
                    records_invalid=result.records_invalid,
                    unexpected_fields=result.unexpected_fields,
                )

            context.storage.write_json(
                path=f'{dataset_reference.dataset_uri}/manifest.json',
                value=metadata.model_dump(mode='json'),
            )

    logger.info('EXTRACT STEP EXECUTED WITH SUCCESS')

    return IngestionOutput(
        run_id=context.run_id,
        datasets=tuple(dataset_references),
    )


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

    df_movies = (
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
    )

    df_dim_movie = df_movies.reindex(
        columns=[
            'id',
            'title',
            'release_date',
            'original_language',
            'overview',
            'revenue',
        ]
    )

    df_bridge_movie_genre = (
        df_movies[['id', 'genres']]
        .explode('genres', ignore_index=True)
        .dropna(subset=['genres'])
        .rename(columns={'id': 'movie_id'})
        .assign(genre_id=lambda d: d['genres'].str['id'])[['movie_id', 'genre_id']]
        .drop_duplicates()
    )

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
        run_id=str(uuid7()),
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
