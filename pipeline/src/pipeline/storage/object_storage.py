from abc import ABC, abstractmethod
import logging
from pathlib import Path
import shutil

logger = logging.getLogger(f'pipeline.{__name__}')


class ObjectStorage(ABC):
    def __init__(self, root: str):
        self.root = root

    @abstractmethod
    def upload(self, local_path: Path, remote_name: str) -> str:
        """Upload a local file and return its URI"""
        ...

    @abstractmethod
    def uri(self, path: str) -> str:
        """Return the URI of an object."""
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check whether an object exists."""
        ...

    @abstractmethod
    def list(self, prefix: str, suffix: str | None = None) -> list[str]:
        """List of the files matching with the prefix and suffix arguments"""


class LocalStorage(ObjectStorage):
    """Simulate an object storage on your local file system"""

    def __init__(self, root: str):
        self.root = root

    def list(
        self,
        prefix: str,
        suffix: str | None = None,
    ) -> list[str]:
        root = Path(self.root) / prefix

        if suffix is None:
            return sorted(str(p) for p in root.iterdir() if p.is_file())

        return sorted(
            str(p)
            for p in root.iterdir()
            if p.is_file() and p.name.lower().endswith(suffix.lower())
        )

    def upload(self, local_path: Path, remote_name: str) -> str:
        destination = Path(self.root) / remote_name
        destination.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(local_path, destination)

        logger.debug('Uploaded %s to %s', local_path, destination)

        return str(destination)

    def uri(self, path: str) -> str:
        return str(Path(self.root) / path)

    def exists(self, path: str) -> bool:
        return (Path(self.root) / path).exists()


# TODO: implement this class
class S3Storage(ObjectStorage):
    """Not implemented yet"""

    def __init__(self, root: str):
        self.root = root

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
    def create(cls, provider: str, root: str, **kwargs) -> ObjectStorage:
        try:
            storage_cls = cls._providers[provider]
        except KeyError:
            raise ValueError(f'Unsupported provider: {provider}')

        return storage_cls(root, **kwargs)
