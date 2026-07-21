import json
import gzip
import logging
from math import ceil
from pathlib import Path
import shutil
import tempfile
from typing import Any, Protocol

from pipeline.datalake import DataLake, Dataset
from pipeline.storage.object_storage import ObjectStorage


logger = logging.getLogger(f'pipeline.{__name__}')

JsonDict = dict[str, Any]


def compress_file(source_path: Path, output_path: Path):
    """Helper function to compress an existing file in chunks using gzip"""

    with open(source_path, 'rb') as f_in:
        with gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.debug('Compressed %s into %s', source_path, output_path)


class FileWriter(Protocol):
    """Protocol for all the writers"""

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
    ) -> None:
        self.datalake = datalake
        self.writer_factory = writer_factory

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
    @property
    def size(self) -> int:
        return ceil(self.current_file_size / 1024 / 1024)

    def __init__(
        self,
        object_storage: ObjectStorage,
        prefix: str,
        max_file_size_mb: int = 64,
        file_part: int = 0,
        local_folder: Path | None = None,
    ):
        self.storage = object_storage
        self.prefix = prefix

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
