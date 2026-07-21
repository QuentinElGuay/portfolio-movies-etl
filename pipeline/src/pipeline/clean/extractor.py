from collections.abc import Iterator
from typing import Protocol

from pipeline.models.clean import DatasetRecord


class Extractor[T](Protocol):
    def __call__(
        self,
        model: T,
    ) -> Iterator[DatasetRecord]: ...
