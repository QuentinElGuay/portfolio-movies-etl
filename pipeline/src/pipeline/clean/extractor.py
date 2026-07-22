from collections.abc import Mapping

from pipeline.models.ingest import Genre, Movie, Rating
from pipeline.datalake import Dataset
from pydantic import BaseModel


class Extractor: ...


class GenreExtractor: ...


class MovieExtractor: ...


class RatingExtractor: ...


class ExtractorFactory:
    _registry: Mapping[str, tuple[type[Extractor], type[BaseModel]]] = {
        'movies': (MovieExtractor, Movie),
        'genres': (GenreExtractor, Genre),
        'ratings': (RatingExtractor, Rating),
    }

    @classmethod
    def create(cls, dataset: Dataset) -> Extractor:
        try:
            extractor_cls, schema = cls._registry[dataset.name]
        except KeyError as exc:
            raise ValueError(
                f'No extractor registered for dataset "{dataset.name}".'
            ) from exc

        return extractor_cls(schema=schema)
