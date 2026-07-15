from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Iterator, TypeVar

from pydantic import BaseModel, ValidationError


T = TypeVar('T', bound=BaseModel)


@dataclass(slots=True)
class ValidationResult[T]:
    record: dict[str, Any]
    model: T | None
    errors: list[Any] | None


def validate_records(record: dict[str, Any], model: type[T]) -> ValidationResult:
    """
    Validate API record and yield the validation outcome for each record.
    """
    try:
        obj = model.model_validate(record)
        return ValidationResult(record, obj, None)
    except ValidationError as exc:
        return ValidationResult(record, None, exc.errors(include_url=False))
