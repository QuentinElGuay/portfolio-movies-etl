from abc import ABC, abstractmethod
import gzip
import json
import logging
from glob import glob
from math import ceil
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any
from urllib.parse import urljoin

import pandas as pd

logger = logging.getLogger(f'movie_etl.{__name__}')


def compress_file(source_path: Path, output_path: Path):
    """Helper function to compress an existing file in chunks using gzip"""

    with open(source_path, 'rb') as f_in:
        with gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.debug('Compressed %s into %s', source_path, output_path)


class ObjectStorage(ABC):
    @abstractmethod
    def upload(self, local_path: Path, remote_name: str) -> str:
        pass

    @abstractmethod
    def download(self, remote_name: str) -> Path:
        pass


class LocalStorage(ObjectStorage):
    def __init__(self):
        self.folder = Path(os.environ['LOCAL_STORAGE'])

    def upload(self, local_path: Path, remote_name: str) -> str:
        target_path = self.folder / remote_name
        target_dir = target_path.parent
        target_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(str(local_path), str(target_path))

        logging.info('Uploading file to %s', target_path)

        return str(target_path)

    def download(self, remote_name: str) -> Path:
        object_path = Path(self.folder) / remote_name
        logging.info('Downloading file from %s', object_path)

        return object_path


class S3Storage(ObjectStorage):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def upload(self, local_path: Path, remote_name: str) -> str:
        raise NotImplementedError

    def download(self, remote_name: str) -> Path:
        raise NotImplementedError


class StorageFactory:
    _providers: dict[str, type[ObjectStorage]] = {
        'local': LocalStorage,
        's3': S3Storage,
        # 'gcs': GCSStorage,
        # 'azure': AzureBlobStorage,
    }

    @classmethod
    def create(cls, provider: str, **kwargs) -> ObjectStorage:
        try:
            storage_cls = cls._providers[provider]
        except KeyError:
            raise ValueError(f'Unsupported provider: {provider}')

        return storage_cls(**kwargs)


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
        line = json.dumps(record)
        self.file.write(line)
        self.file.write('\n')

        self.current_file_size += len(line) + 1  # +1 for the newline character

        if self.current_file_size >= self.max_file_size:
            self.upload_file()
            self.current_file_part += 1
            self._init_file()

    def close(self) -> None:
        self.file.close()
        self.upload_file()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class NdjsonReader:
    """
    A utility class to read and combine multiple NDJSON files from a folder into a single pandas DataFrame.
    """

    def __init__(
        self,
        folder_path: Path,
        extensions: list[str] = [
            '.ndjson',
            '.jsonl',
            '.ndjson.gz',
            '.jsonl.gz',
        ],
    ):
        """
        Initializes the reader with a target folder.

        Args:
            folder_path (Path): Path to the directory containing the files.
            extensions (list[str]): List of file extensions to search for.
                           Defaults to ['.ndjson', '.jsonl', '.ndjson.gz',
                           '.jsonl.gz'].
        """
        self.folder_path = folder_path
        self.extensions = extensions

    def _find_files(self) -> list[str]:
        """Internal method to scan the directory for matching files."""
        all_files = []
        base_path = os.path.join(self.folder_path, '*')

        for ext in self.extensions:
            all_files.extend(glob(f'{base_path}{ext}'))
            all_files.extend(glob(f'{base_path}{ext.upper()}'))

        return list(set(all_files))

    def read_all(self, columns: list = [], **kwargs) -> pd.DataFrame:
        """
        Reads all found NDJSON files and concatenates them into one DataFrame.

        Args:
            columns (list[str]): Optional list of column names to keep (saves memory).
            kwargs (Any): Additional arguments passed directly to pd.read_json() (e.g., encoding='utf-8', dtype={'id': int}).

        Returns:
            A single combined pandas DataFrame.
        """
        files = self._find_files()

        if not files:
            logging.warning(
                'Warning: No matching NDJSON files found in %s', self.folder_path
            )

        df_list = []
        for file in files:
            try:
                df_chunk = pd.read_json(file, lines=True, compression='infer', **kwargs)

                if len(columns):
                    existing_cols = [c for c in columns if c in df_chunk.columns]
                    df_chunk = df_chunk[existing_cols]

                if not df_chunk.empty:
                    df_list.append(df_chunk)

            except Exception as e:
                logging.error('Error reading file %s: %s', file, e)
                continue

        if not df_list:
            return pd.DataFrame()

        return pd.concat(df_list, ignore_index=True)
