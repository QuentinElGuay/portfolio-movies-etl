from dataclasses import dataclass
import logging
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError


T = TypeVar('T', bound=BaseModel)

logger = logging.getLogger(f'pipeline.{__name__}')


@dataclass(slots=True)
class ValidationResult[T]:
    record: dict[str, Any]
    model: T | None
    errors: list[Any] | None


def validate_record[T: BaseModel](
    record: dict[str, Any], model: type[T]
) -> ValidationResult[T]:
    """
    Validate API record and yield the validation outcome for each record.
    """
    try:
        obj = model.model_validate(record)
        return ValidationResult(record, obj, None)
    except ValidationError as exc:
        return ValidationResult(record, None, exc.errors(include_url=False))
