from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import PurePosixPath

from pipeline.storage.object_storage import ObjectStorage


class FileFormat(StrEnum):
    """Supported dataset file formats."""

    NDJSON = 'ndjson'
    PARQUET = 'parquet'


class Layer(StrEnum):
    """Medallion architecture layers."""

    BRONZE = 'bronze'
    SILVER = 'silver'
    GOLD = 'gold'
    QUARANTINE = 'quarantine'


@dataclass
class LayerConfig:
    root: str
    format: FileFormat


type LayerConfigs = dict[str, LayerConfig]


@dataclass(frozen=True, slots=True)
class DataLakeConfig:
    max_file_size_mb: int
    layers: dict[Layer, LayerConfig]


@dataclass(frozen=True, slots=True)
class Dataset:
    """Logical representation of a dataset."""

    name: str
    layer: Layer
    snapshot_date: date = date.today()

    def to_dict(self) -> dict[str, str]:
        return {
            'name': self.name,
            'layer': self.layer.value,
            'snapshot_date': self.snapshot_date,
        }

    @classmethod
    def from_dict(cls, value: dict[str, str]) -> 'Dataset':
        return cls(
            name=value['name'],
            layer=Layer(value['layer']),
        )


class DataLake:
    """
    Organizes datasets within an object storage.

    Layout:
        <layer>/<dataset>/date=<YYYY-MM-DD>/
    """

    def __init__(self, datalake_config: DataLakeConfig, storage: ObjectStorage):
        self.config = datalake_config
        self.storage = storage

    def dataset(self, name: str, layer: Layer) -> Dataset:
        """Return a dataset declaration."""
        return Dataset(name=name, layer=layer)

    def prefix(self, dataset: Dataset) -> str:
        """
        Return the dataset folder relative to the storage root.

        Example:
            silver/dim_movie/date=2026-07-17/
        """

        return str(
            PurePosixPath(dataset.layer)
            / dataset.name
            / f'date={dataset.snapshot_date.isoformat()}'
        )

    def uri(self, dataset: Dataset) -> str:
        """Return the storage URI for a dataset."""
        return self.storage.uri(
            self.config.layers[dataset.layer].root,
            self.prefix(dataset),
        )
