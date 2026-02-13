"""Pydantic v2 schema definitions for ETL pipeline."""

from typing import Optional, Union
from pydantic import BaseModel, Field, field_validator, ConfigDict


class Score(BaseModel):
    """Score model for individual play records."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    score_id: int
    user_id: int
    beatmap_id: int
    pp: float = Field(ge=0, description="Performance points, must be >= 0")
    mods_key: str
    speed_mod: Optional[str] = None
    accuracy: float = Field(ge=0, le=100, description="Accuracy percentage, 0-100")
    score: int
    data: Optional[str] = None

    @field_validator("score_id", "user_id", "beatmap_id", "score", mode="before")
    @classmethod
    def coerce_int(cls, v):
        """Coerce string values to int."""
        if v is None:
            return v
        if isinstance(v, str):
            return int(v)
        return v

    @field_validator("pp", "accuracy", mode="before")
    @classmethod
    def coerce_float(cls, v):
        """Coerce string values to float."""
        if v is None:
            return v
        if isinstance(v, str):
            return float(v)
        return v


class Beatmap(BaseModel):
    """Beatmap model for map metadata."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    beatmap_id: int
    beatmapset_id: int
    version: str
    difficultyrating: float = Field(ge=0, description="Star rating, must be >= 0")
    bpm: Optional[float] = None
    total_length: Optional[int] = None
    playmode: int = Field(
        default=0, ge=0, le=3, description="Game mode: 0=osu, 1=taiko, 2=catch, 3=mania"
    )

    @field_validator(
        "beatmap_id", "beatmapset_id", "total_length", "playmode", mode="before"
    )
    @classmethod
    def coerce_int(cls, v):
        """Coerce string values to int."""
        if v is None:
            return v
        if isinstance(v, str):
            return int(v)
        return v

    @field_validator("difficultyrating", "bpm", mode="before")
    @classmethod
    def coerce_float(cls, v):
        """Coerce string values to float."""
        if v is None:
            return v
        if isinstance(v, str):
            return float(v)
        return v


class User(BaseModel):
    """User model for player statistics."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    user_id: int
    username: str
    pp_raw: float = Field(ge=0, description="Raw PP, must be >= 0")
    accuracy: float = Field(ge=0, le=100, description="Overall accuracy, 0-100")
    playcount: int = Field(ge=0, description="Total play count, must be >= 0")

    @field_validator("user_id", "playcount", mode="before")
    @classmethod
    def coerce_int(cls, v):
        """Coerce string values to int."""
        if v is None:
            return v
        if isinstance(v, str):
            return int(v)
        return v

    @field_validator("username", mode="before")
    @classmethod
    def coerce_str(cls, v):
        """Coerce any value to string for username."""
        if v is None:
            return v
        return str(v)

    @field_validator("pp_raw", "accuracy", mode="before")
    @classmethod
    def coerce_float(cls, v):
        """Coerce string values to float."""
        if v is None:
            return v
        if isinstance(v, str):
            return float(v)
        return v


class Playcount(BaseModel):
    """Playcount model for user-beatmap play statistics."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    user_id: int
    beatmap_id: int
    playcount: int = Field(ge=0, description="Play count, must be >= 0")

    @field_validator("user_id", "beatmap_id", "playcount", mode="before")
    @classmethod
    def coerce_int(cls, v):
        """Coerce string values to int."""
        if v is None:
            return v
        if isinstance(v, str):
            return int(v)
        return v
