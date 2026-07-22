from typing import Annotated
from uuid import UUID

from pydantic import Field


type GenreId = Annotated[int, Field(gt=0)]
type MovieId = Annotated[int, Field(gt=0)]
type UserId = Annotated[int, Field(gt=0)]
type RunId = UUID
