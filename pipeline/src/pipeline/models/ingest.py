from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from pipeline.models.types import GenreId, MovieId, UserId
from pydantic import BaseModel, Field, ConfigDict, field_validator


class Genre(BaseModel):
    model_config = ConfigDict(extra='ignore')

    id: GenreId
    name: Annotated[str, Field(min_length=1, max_length=20)]


class Movie(BaseModel):
    model_config = ConfigDict(extra='ignore')

    id: MovieId
    title: Annotated[str, Field(min_length=1, max_length=200)]
    release_date: date | None = None
    original_language: Annotated[str, Field(max_length=50)] | None = None
    overview: str | None = None
    revenue: int | None = Field(default=None, ge=0)
    genres: list[Genre]

    @field_validator('release_date', mode='before')
    @classmethod
    def parse_release_date(cls, value):
        if value is None:
            return None
        else:
            return datetime.strptime(
                value,
                '%a, %d %b %Y %H:%M:%S GMT',
            ).date()


class Rating(BaseModel):
    model_config = ConfigDict(extra='ignore')

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
