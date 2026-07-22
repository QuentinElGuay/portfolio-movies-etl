from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import NoneType
from typing import Annotated, Any, get_args, get_origin
from uuid import UUID

import pyarrow as pa
from pydantic import BaseModel
from pydantic.fields import FieldInfo


_ARROW_TYPES: dict[type, callable] = {
    str: pa.string,
    int: pa.int64,
    float: pa.float64,
    bool: pa.bool_,
    bytes: pa.binary,
    UUID: pa.string,
    date: pa.date32,
    time: lambda: pa.time64('us'),
}


def schema_from_pydantic(
    model: type[BaseModel],
    *,
    default_timezone: str = 'UTC',
) -> pa.Schema:
    """Generate a PyArrow schema from a Pydantic model."""
    return pa.schema(
        [
            _model_field_to_arrow(name, field, default_timezone=default_timezone)
            for name, field in model.model_fields.items()
        ]
    )


def _model_field_to_arrow(name: str, field: FieldInfo, *, default_timezone) -> pa.Field:
    annotation, nullable, metadata = _unwrap_annotation(field.annotation)

    return pa.field(
        name=name,
        type=_annotation_to_arrow(
            annotation, metadata, default_timezone=default_timezone
        ),
        nullable=nullable,
    )


def _unwrap_annotation(
    annotation: Any,
) -> tuple[Any, bool, list[Any]]:
    """Return (annotation, nullable, metadata)."""
    nullable = False
    metadata: list[Any] = []

    while True:
        origin = get_origin(annotation)

        if origin is Annotated:
            annotation, *extra = get_args(annotation)
            metadata.extend(extra)
            continue

        if origin is not None:
            args = get_args(annotation)
            if NoneType in args:
                nullable = True
                args = tuple(arg for arg in args if arg is not NoneType)
                if len(args) != 1:
                    raise TypeError(f'Unsupported union type: {annotation!r}')
                annotation = args[0]
                continue

        break

    return annotation, nullable, metadata


def _annotation_to_arrow(
    annotation: Any,
    metadata: list[Any],
    *,
    default_timezone: str,
) -> pa.DataType:
    origin = get_origin(annotation)

    if origin is list:
        (item_type,) = get_args(annotation)
        item_type, _, item_metadata = _unwrap_annotation(item_type)
        return pa.list_(_annotation_to_arrow(item_type, item_metadata))

    if origin is dict:
        key_type, value_type = get_args(annotation)

        if key_type is not str:
            raise TypeError('Only dict[str, T] is supported.')

        value_type, _, value_metadata = _unwrap_annotation(value_type)

        return pa.map_(
            pa.string(),
            _annotation_to_arrow(value_type, value_metadata),
        )

    if isinstance(annotation, type):
        if issubclass(annotation, BaseModel):
            return pa.struct(
                [
                    _model_field_to_arrow(name, field)
                    for name, field in annotation.model_fields.items()
                ]
            )

        if issubclass(annotation, Enum):
            return pa.string()

    if annotation is Decimal:
        return _decimal_to_arrow(metadata)

    if annotation is datetime:
        return pa.timestamp('us', tz=default_timezone)

    try:
        return _ARROW_TYPES[annotation]()
    except KeyError as exc:
        raise TypeError(f'Unsupported type: {annotation!r}') from exc


def _decimal_to_arrow(metadata: list[Any]) -> pa.DataType:
    field = next(
        (item for item in metadata if isinstance(item, FieldInfo)),
        None,
    )

    if field is None:
        raise TypeError(
            'Decimal fields must use Field(max_digits=..., decimal_places=...).'
        )

    if field.max_digits is None or field.decimal_places is None:
        raise TypeError('Decimal fields must specify max_digits and decimal_places.')

    return pa.decimal128(
        field.max_digits,
        field.decimal_places,
    )
