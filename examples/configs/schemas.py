from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class _BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class CareerBattingOut(_BaseSchema):
    games: int
    at_bats: int
    runs: int
    hits: int
    doubles: int
    triples: int
    home_runs: int
    rbi: int
    walks: int
    strikeouts: int
    stolen_bases: int
    caught_stealing: int | None

    avg: float = Field(..., ge=0)
    obp: float = Field(..., ge=0)
    slg: float = Field(..., ge=0)
    ops: float = Field(..., ge=0)
    hits_per_game: float | None = Field(default=None, ge=0)


class CareerBattingUpdate(BaseModel):
    games: int | None = None
    at_bats: int | None = None
    runs: int | None = None
    hits: int | None = None
    doubles: int | None = None
    triples: int | None = None
    home_runs: int | None = None
    rbi: int | None = None
    walks: int | None = None
    strikeouts: int | None = None
    stolen_bases: int | None = None
    caught_stealing: int | None = None

    avg: float | None = None
    obp: float | None = None
    slg: float | None = None
    ops: float | None = None


class PlayerOut(_BaseSchema):
    id: UUID
    name: str
    primary_position: str
    career_batting: CareerBattingOut | None


class PlayerDetailOut(PlayerOut):
    description: str


class PlayerUpdate(BaseModel):
    name: str | None = None
    primary_position: str | None = None
    career_batting: CareerBattingUpdate | None = None


class ImportResult(BaseModel):
    inserted_players: int
    updated_players: int
