from abc import ABC, abstractmethod
import json
import logging
from math import ceil
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any
from urllib.parse import urljoin


logger = logging.getLogger(__name__)


class ObjectStorage(ABC):
    @abstractmethod
    def upload(self, local_path: Path, remote_name: str):
        pass

    @abstractmethod
    def download(self, remote_name: str, local_path: Path):
        pass


class LocalStorage(ObjectStorage):
    def __init__(self):
        self.folder = Path(os.environ['LOCAL_STORAGE'])

    def upload(self, local_path: Path, remote_name: str):
        target_path = self.folder / remote_name
        target_dir = target_path.parent
        target_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(str(local_path.name), str(target_path))
    
        logging.info('Uploading file to %s', target_path)

        return target_path

    def download(self, remote_name: str, local_path: Path):
        object_path = Path(self.folder) / remote_name
        logging.info('Downloading file from %s', object_path)

        return object_path


class S3Storage(ObjectStorage):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def upload(self, local_path: Path, remote_name: str) -> None:
        raise NotImplementedError

    def download(self, remote_name: str, local_path: Path) -> None:
        raise NotImplementedError


class StorageFactory:
    _providers: dict[str, type[ObjectStorage]] = {
        "local": LocalStorage,
        "s3": S3Storage,
 #       "gcs": GCSStorage,
 #       "azure": AzureBlobStorage,
    }

    @classmethod
    def create(cls, provider: str, **kwargs) -> ObjectStorage:
        try:
            storage_cls = cls._providers[provider]
        except KeyError:
            raise ValueError(f"Unsupported provider: {provider}")

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
        local_folder: Path = None,
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

    def upload_file(self):
        self.file.close()
        self.storage.upload(self.file, urljoin(self.prefix, f'part-{self.current_file_part:04d}.ndjson'))

    def write(self, record: dict[str, Any]) -> None:
        """Write a single JSON record to the current file."""
        line = json.dumps(record)
        self.file.write(line)
        self.file.write('\n')

        self.current_file_size += (
            len(line) + 1
        )  # +1 for the newline character

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
