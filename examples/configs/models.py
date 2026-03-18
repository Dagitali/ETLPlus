from __future__ import annotations

import uuid
from datetime import UTC
from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from .db import Base


class Player(Base):
    __tablename__ = 'players'

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    primary_position: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        nullable=False,
    )

    career_batting: Mapped[PlayerCareerBatting] = relationship(
        back_populates='player',
        uselist=False,
        cascade='all, delete-orphan',
    )


class PlayerCareerBatting(Base):
    __tablename__ = 'player_career_batting'

    player_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey('players.id', ondelete='CASCADE'),
        primary_key=True,
    )

    games: Mapped[int] = mapped_column(Integer, nullable=False)
    at_bats: Mapped[int] = mapped_column(Integer, nullable=False)
    runs: Mapped[int] = mapped_column(Integer, nullable=False)
    hits: Mapped[int] = mapped_column(Integer, nullable=False)
    doubles: Mapped[int] = mapped_column(Integer, nullable=False)
    triples: Mapped[int] = mapped_column(Integer, nullable=False)
    home_runs: Mapped[int] = mapped_column(Integer, nullable=False)
    rbi: Mapped[int] = mapped_column(Integer, nullable=False)
    walks: Mapped[int] = mapped_column(Integer, nullable=False)
    strikeouts: Mapped[int] = mapped_column(Integer, nullable=False)
    stolen_bases: Mapped[int] = mapped_column(Integer, nullable=False)
    caught_stealing: Mapped[int | None] = mapped_column(Integer)

    avg: Mapped[float] = mapped_column(Numeric(5, 3), nullable=False)
    obp: Mapped[float] = mapped_column(Numeric(5, 3), nullable=False)
    slg: Mapped[float] = mapped_column(Numeric(5, 3), nullable=False)
    ops: Mapped[float] = mapped_column(Numeric(5, 3), nullable=False)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        nullable=False,
    )

    player: Mapped[Player] = relationship(back_populates='career_batting')
