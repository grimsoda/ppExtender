"""Validation utilities for ETL pipeline."""

import os
import logging
from enum import Enum
from typing import Optional, Dict, Any, Type
from pydantic import BaseModel, ValidationError

from .schemas import Score, Beatmap, User, Playcount


logger = logging.getLogger(__name__)


class ValidationMode(Enum):
    """Validation mode for row processing."""

    STRICT = "strict"  # Raise on validation errors
    LENIENT = "lenient"  # Return None on validation errors, log warning
    NONE = "none"  # Skip validation entirely (fastest)


# Mapping of table names to model classes
TABLE_MODELS: Dict[str, Type[BaseModel]] = {
    "scores": Score,
    "beatmaps": Beatmap,
    "users": User,
    "playcount": Playcount,
    # Alias mappings for common variations
    "score": Score,
    "beatmap": Beatmap,
    "user": User,
    "osu_scores": Score,
    "osu_beatmaps": Beatmap,
    "osu_user_stats": User,
    "osu_user_beatmap_playcount": Playcount,
}


def get_validation_mode() -> ValidationMode:
    """Get validation mode from environment variable.

    Returns:
        ValidationMode: Current validation mode based on PYDANTIC_VALIDATION env var.
    """
    mode_str = os.getenv("PYDANTIC_VALIDATION", "strict").lower()
    try:
        return ValidationMode(mode_str)
    except ValueError:
        logger.warning(f"Invalid PYDANTIC_VALIDATION value: {mode_str}, using 'strict'")
        return ValidationMode.STRICT


def validate_row(
    table_name: str, row: Dict[str, Any], mode: Optional[ValidationMode] = None
) -> Optional[BaseModel]:
    """Validate a row dictionary against the appropriate schema.

    Args:
        table_name: Name of the table (e.g., 'scores', 'beatmaps', 'users')
        row: Dictionary containing the row data
        mode: Validation mode (defaults to env var PYDANTIC_VALIDATION)

    Returns:
        Validated model instance, or None if validation fails in lenient mode

    Raises:
        ValueError: If table_name is not recognized
        ValidationError: If validation fails in strict mode
    """
    # Use environment mode if not specified
    if mode is None:
        mode = get_validation_mode()

    # Skip validation entirely if mode is NONE
    if mode == ValidationMode.NONE:
        return None

    # Get the appropriate model class
    model_class = TABLE_MODELS.get(table_name)
    if model_class is None:
        available = ", ".join(sorted(TABLE_MODELS.keys()))
        raise ValueError(
            f"Unknown table name: '{table_name}'. Available tables: {available}"
        )

    try:
        # Attempt to validate the row
        return model_class(**row)
    except ValidationError as e:
        if mode == ValidationMode.STRICT:
            # Re-raise the error in strict mode
            raise
        elif mode == ValidationMode.LENIENT:
            # Log the error and return None in lenient mode
            logger.warning(
                f"Validation failed for {table_name}: {e.errors()}. Row data: {row}"
            )
            return None
        else:
            # Should not reach here, but just in case
            raise


def validate_batch(
    table_name: str, rows: list[Dict[str, Any]], mode: Optional[ValidationMode] = None
) -> list[BaseModel]:
    """Validate a batch of rows.

    Args:
        table_name: Name of the table
        rows: List of row dictionaries
        mode: Validation mode (defaults to env var PYDANTIC_VALIDATION)

    Returns:
        List of validated model instances (excluding failed validations in lenient mode)
    """
    validated = []
    for row in rows:
        result = validate_row(table_name, row, mode)
        if result is not None:
            validated.append(result)
    return validated
