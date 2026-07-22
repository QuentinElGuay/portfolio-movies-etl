from abc import ABC, abstractmethod
from collections.abc import Iterator

from collections.abc import Mapping

from pipeline.clean.extractor.genres import GenreExtractor
from pipeline.clean.extractor.movies import MovieExtractor
from pipeline.clean.extractor.ratings import RatingExtractor
from pipeline.datalake import Dataset

import pyarrow as pa


class Extractor(ABC):
    """Transforms a Bronze table into one or more Silver tables."""

    def __call__(self, table: pa.Table) -> Iterator[pa.Table]:
        yield from self.extract(table)

    @abstractmethod
    def extract(self, table: pa.Table) -> Iterator[pa.Table]:
        """Transform a Bronze table into one or more output tables."""
        raise NotImplementedError


class ExtractorFactory:
    _registry: Mapping[str, type[Extractor]] = {
        'movies': MovieExtractor,
        'genres': GenreExtractor,
        'ratings': RatingExtractor,
    }

    @classmethod
    def create(cls, dataset: Dataset) -> Extractor:
        try:
            extractor_cls = cls._registry[dataset.name]
        except KeyError as exc:
            raise ValueError(
                f'No extractor registered for dataset "{dataset.name}".'
            ) from exc

        return extractor_cls()
