import json
import gzip
import logging
from math import ceil
from pathlib import Path
import shutil
import tempfile
from typing import Any, Protocol

from pipeline.models.pyarrow import schema_from_pydantic
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.datalake import DataLake, Dataset
from pipeline.storage.object_storage import ObjectStorage
from pydantic import BaseModel


logger = logging.getLogger(f'pipeline.{__name__}')

JsonDict = dict[str, Any]


def compress_file(source_path: Path, output_path: Path):
    """Helper function to compress an existing file in chunks using gzip"""

    with open(source_path, 'rb') as f_in:
        with gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.debug('Compressed %s into %s', source_path, output_path)


class FileWriter(Protocol):
    """Protocol implemented by writers that persist dataset records.

    Writers are responsible for buffering records, creating output files,
    and writing them to the configured destination storage.

    Implementations must support incremental writes through `write()` and
    release any resources or flush pending data through `close()`.
    """

    def write(self, record: dict[str, Any]) -> None: ...

    def close(self) -> None: ...

    def __enter__(self) -> 'FileWriter': ...

    def __exit__(self, *args: object) -> None: ...


class WriterFactory(Protocol):
    def __call__(
        self,
        *,
        object_storage: ObjectStorage,
        prefix: str,
        max_file_size_mb: int,
    ) -> FileWriter: ...


class WritersManager:
    def __init__(
        self,
        *,
        datalake: DataLake,
        writer_factory: WriterFactory,
        model: type[BaseModel],
    ) -> None:
        self.datalake = datalake
        self.writer_factory = writer_factory
        self.model = model

        self._writers: dict[Dataset, FileWriter] = {}

    def write(
        self,
        *,
        dataset: Dataset,
        record: JsonDict,
    ) -> None:
        writer = self._get_writer(dataset)
        writer.write(record)

    def _get_writer(
        self,
        dataset: Dataset,
    ) -> FileWriter:

        writer = self._writers.get(dataset)

        if writer is None:
            writer = self.writer_factory(
                object_storage=self.datalake.storage,
                prefix=self.datalake.prefix(dataset),
                max_file_size_mb=self.datalake.config.max_file_size_mb,
                model=self.model,
            )

            self._writers[dataset] = writer

        return writer

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()

        self._writers.clear()

    def __enter__(self) -> 'WritersManager':
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class NdjsonWriter:
    """Writer implementation that stores records as newline-delimited JSON.

    Each record is serialized independently as a JSON object and appended to
    NDJSON files. Files are rotated when they reach the configured size limit
    and uploaded to the destination storage.

    This implementation is suitable for raw ingestion layers where preserving
    the original JSON representation is required.
    """

    @property
    def size(self) -> int:
        return ceil(self.current_file_size / 1024 / 1024)

    def __init__(
        self,
        object_storage: ObjectStorage,
        prefix: str,
        model: type[BaseModel],
        max_file_size_mb: int = 64,
        file_part: int = 0,
        local_folder: Path | None = None,
    ):
        self.storage = object_storage
        self.prefix = prefix
        self.schema = model  # TODO: evaluate if required for something

        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.current_file_part = file_part

        if local_folder is None:
            self._temp_dir = tempfile.TemporaryDirectory()
            self.destination_folder = Path(self._temp_dir.name)
        else:
            self.destination_folder = Path(local_folder)
            self.destination_folder.mkdir(parents=True, exist_ok=True)

        self._init_file()

    def _init_file(self) -> None:
        self.current_file_size = 0
        file_path = (
            self.destination_folder / f'part-{self.current_file_part:04d}.ndjson'
        )
        self.file = file_path.open('w', encoding='utf-8')

    # TODO: Should upload be handled externally?
    def upload_file(self, compression=True):
        """
        Upload a file to an object storage provider

        Args:
            compression (bool): Define if the file must be compressed before upload.
        """

        self.file.close()

        file_path = Path(self.file.name)
        file_key = Path(self.prefix) / f'part-{self.current_file_part:04d}.ndjson'

        if compression:
            output_path = file_path.with_name(file_path.name + '.gz')
            compress_file(file_path, output_path)
            file_path = output_path
            file_key = file_key.with_name(file_key.name + '.gz')

        self.storage.upload(file_path, str(file_key))

    def write(self, record: dict[str, Any]) -> None:
        """Write a single JSON record to the current file."""
        line = json.dumps(
            record,
            ensure_ascii=False,
            separators=(',', ':'),
        )

        self.file.write(line)
        self.file.write('\n')

        self.current_file_size += len(line) + 1  # +1 for the newline character

        if self.current_file_size >= self.max_file_size:
            self.upload_file()
            self.current_file_part += 1
            self._init_file()

    def close(self) -> None:
        if self.current_file_size == 0:
            self.file.close()
        else:
            self.upload_file()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# TODO:
# Notes from AI agent:
# I would rework one point before using this in production: the write() method
# currently rebuilds the entire pyarrow.Table every record to estimate size.
# That becomes expensive for large datasets.


# A better implementation is to track an approximate buffered size from the input
# records or flush based on row count (for example 100k rows), letting Parquet
# compression handle final sizing. I would also make the schema mandatory because
# Silver datasets should be schema-driven from the model registry.
class ParquetWriter:
    """Writer implementation that stores records as Apache Parquet files.

    Records are buffered and converted into PyArrow tables using the provided
    schema before being written as columnar Parquet files. Files are rotated
    according to the configured buffering strategy and uploaded to the
    destination storage.

    This implementation is suitable for structured datasets where schema
    enforcement and analytical columnar storage are required.
    """

    def __init__(
        self,
        object_storage: ObjectStorage,
        prefix: str,
        model: type[BaseModel],
        max_file_size_mb: int = 128,
        file_part: int = 0,
        local_folder: Path | None = None,
    ):
        self.storage = object_storage
        self.prefix = prefix
        self.schema = schema_from_pydantic(model)

        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.current_file_part = file_part

        if local_folder is None:
            self._temp_dir = tempfile.TemporaryDirectory()
            self.destination_folder = Path(self._temp_dir.name)
        else:
            self.destination_folder = Path(local_folder)
            self.destination_folder.mkdir(parents=True, exist_ok=True)

        self.buffer: list[dict[str, Any]] = []
        self.current_file_size = 0

    def _write_table(self, table: pa.Table) -> None:
        file_path = (
            self.destination_folder / f'part-{self.current_file_part:04d}.parquet'
        )

        pq.write_table(
            table,
            file_path,
            compression='snappy',
        )

        file_key = Path(self.prefix) / f'part-{self.current_file_part:04d}.parquet'

        self.storage.upload(file_path, str(file_key))

        self.current_file_part += 1
        self.current_file_size = 0

    def _flush(self) -> None:
        if not self.buffer:
            return

        table = pa.Table.from_pylist(
            self.buffer,
            schema=self.schema,
        )

        self._write_table(table)
        self.buffer.clear()

    def write(self, record: dict[str, Any]) -> None:
        self.buffer.append(record)

        estimated_size = pa.Table.from_pylist(
            self.buffer,
            schema=self.schema,
        ).nbytes

        self.current_file_size = estimated_size

        if self.current_file_size >= self.max_file_size:
            self._flush()

    def close(self) -> None:
        self._flush()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
