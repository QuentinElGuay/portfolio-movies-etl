from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from pipeline.models.types import GenreId, MovieId, RunId, UserId
from pydantic import BaseModel, Field, ConfigDict, field_validator


class DatasetRecord(BaseModel):
    model_config = ConfigDict(extra='forbid')

    run_id: RunId
    snapshot_date: date

    @field_validator("run_id")
    @classmethod
    def validate_uuid_version(cls, value: RunId) -> RunId:
        if value.version != 7:
            raise ValueError("run_id must be UUIDv7")
        return value


class Genre(DatasetRecord):
    model_config = ConfigDict(extra='forbid')

    id: GenreId
    name: Annotated[str, Field(min_length=1, max_length=20)]


class Movie(DatasetRecord):
    model_config = ConfigDict(extra='forbid')

    id: MovieId
    title: Annotated[str, Field(min_length=1, max_length=200)]
    release_date: date | None = None
    original_language: Annotated[str, Field(max_length=50)] | None = None
    overview: str | None = None
    revenue: int | None = Field(default=None, ge=0)


class MovieGenre(DatasetRecord):
    model_config = ConfigDict(extra='forbid')

    movie_id: MovieId
    genre_id: GenreId


class Rating(DatasetRecord):
    model_config = ConfigDict(extra='forbid')

    user_id: UserId
    movie_id: MovieId
    rating: Annotated[
        Decimal,
        Field(
            ge=Decimal('0.0'),
            le=Decimal('5.0'),
            max_digits=2,
            decimal_places=1,
        ),
    ]
    timestamp: datetime
