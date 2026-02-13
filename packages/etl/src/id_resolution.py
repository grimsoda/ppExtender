"""Score ID resolution logic for mapping legacy IDs to new IDs.

This module provides functionality to map legacy score IDs (from osu_scores_high table)
to new score IDs (from scores table) during the ETL pipeline migration.

Resolution Strategy:
    1. Exact Match: Match by (user_id, beatmap_id, mods_key, pp) - all fields match exactly
    2. Fuzzy Match: Match by (user_id, beatmap_id, mods_key) with closest pp value
    3. Unmatched: Log IDs that cannot be resolved for manual review
"""

import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple, Union, Optional
import pyarrow.parquet as pq

from src.schemas import ScoreIdMapping

logger = logging.getLogger(__name__)


class ResolutionError(Exception):
    """Exception raised for ID resolution errors."""

    pass


class IdResolutionStats:
    """Statistics tracking for ID resolution process.

    Tracks the number of total legacy scores, exact matches, fuzzy matches,
    and unmatched IDs for reporting and debugging purposes.
    """

    def __init__(self):
        self.total_legacy = 0
        self.exact_matches = 0
        self.fuzzy_matches = 0
        self.unmatched = 0
        self.unmatched_ids: List[int] = []

    def increment_total(self, count: int = 1) -> None:
        """Increment total legacy count."""
        self.total_legacy += count

    def increment_exact(self, count: int = 1) -> None:
        """Increment exact matches count."""
        self.exact_matches += count

    def increment_fuzzy(self, count: int = 1) -> None:
        """Increment fuzzy matches count."""
        self.fuzzy_matches += count

    def increment_unmatched(
        self, count: int = 1, legacy_id: Optional[int] = None
    ) -> None:
        """Increment unmatched count and optionally track ID."""
        self.unmatched += count
        if legacy_id is not None:
            self.unmatched_ids.append(legacy_id)

    def to_dict(self) -> Dict[str, Union[int, float]]:
        """Convert stats to dictionary for serialization."""
        total_matched = self.exact_matches + self.fuzzy_matches
        return {
            "total_legacy": self.total_legacy,
            "exact_matches": self.exact_matches,
            "fuzzy_matches": self.fuzzy_matches,
            "unmatched": self.unmatched,
            "exact_rate": self.exact_matches / self.total_legacy
            if self.total_legacy > 0
            else 0.0,
            "fuzzy_rate": self.fuzzy_matches / self.total_legacy
            if self.total_legacy > 0
            else 0.0,
            "unmatched_rate": self.unmatched / self.total_legacy
            if self.total_legacy > 0
            else 0.0,
            "total_matched": total_matched,
            "match_rate": total_matched / self.total_legacy
            if self.total_legacy > 0
            else 0.0,
        }

    def __str__(self) -> str:
        """String representation of stats."""
        data = self.to_dict()
        return (
            f"IdResolutionStats(total={data['total_legacy']}, "
            f"exact={data['exact_matches']} ({data['exact_rate']:.1%}), "
            f"fuzzy={data['fuzzy_matches']} ({data['fuzzy_rate']:.1%}), "
            f"unmatched={data['unmatched']} ({data['unmatched_rate']:.1%}))"
        )


def _load_scores(
    scores_source: Union[str, Path, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Load scores from various sources.

    Args:
        scores_source: Either a path to parquet file or a list of score dictionaries

    Returns:
        List of score dictionaries

    Raises:
        ResolutionError: If file cannot be loaded
    """
    if isinstance(scores_source, (str, Path)):
        path = Path(scores_source)
        if not path.exists():
            raise ResolutionError(f"Score file not found: {path}")

        try:
            table = pq.read_table(path)
            # Convert to list of dictionaries
            scores = []
            for i in range(len(table)):
                row = {}
                for col in table.column_names:
                    val = table.column(col)[i].as_py()
                    row[col] = val
                scores.append(row)
            return scores
        except Exception as e:
            raise ResolutionError(f"Failed to load parquet file {path}: {e}")
    else:
        return scores_source


def _normalize_score(score: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize score data types and handle None values.

    Args:
        score: Raw score dictionary

    Returns:
        Normalized score dictionary
    """
    normalized = dict(score)

    # Convert numeric fields
    for field in ["score_id", "user_id", "beatmap_id"]:
        if field in normalized and normalized[field] is not None:
            normalized[field] = int(normalized[field])

    if "pp" in normalized and normalized["pp"] is not None:
        normalized["pp"] = float(normalized["pp"])

    # Normalize mods_key None to empty string
    if normalized.get("mods_key") is None:
        normalized["mods_key"] = ""

    return normalized


def _make_key(user_id: int, beatmap_id: int, mods_key: str) -> Tuple[int, int, str]:
    """Create lookup key from user_id, beatmap_id, and mods_key.

    Args:
        user_id: Player ID
        beatmap_id: Beatmap ID
        mods_key: Mod string

    Returns:
        Tuple suitable for dictionary key
    """
    return (user_id, beatmap_id, mods_key)


def build_id_lookup(
    high_scores: Union[str, Path, List[Dict[str, Any]]],
    new_scores: Union[str, Path, List[Dict[str, Any]]],
) -> Tuple[Dict[int, int], IdResolutionStats]:
    """Build lookup dictionary mapping legacy score IDs to new score IDs.

    Resolution Strategy:
        1. Exact Match: (user_id, beatmap_id, mods_key, pp) all match exactly
        2. Fuzzy Match: (user_id, beatmap_id, mods_key) match, closest pp value
        3. Unmatched: Log for manual review

    Args:
        high_scores: Legacy scores from osu_scores_high table (parquet path or list of dicts)
        new_scores: New scores from scores table (parquet path or list of dicts)

    Returns:
        Tuple of (lookup_dict, stats) where:
            - lookup_dict: {legacy_id: new_id} mapping
            - stats: IdResolutionStats with match statistics

    Raises:
        ResolutionError: If duplicate legacy IDs detected or file loading fails
    """
    stats = IdResolutionStats()

    # Load scores from files if paths provided
    high_scores_list = _load_scores(high_scores)
    new_scores_list = _load_scores(new_scores)

    # Check for duplicate legacy IDs
    legacy_ids = [s.get("score_id") for s in high_scores_list]
    if len(legacy_ids) != len(set(legacy_ids)):
        seen = set()
        duplicates = []
        for lid in legacy_ids:
            if lid in seen:
                duplicates.append(lid)
            seen.add(lid)
        raise ResolutionError(
            f"Duplicate legacy score IDs detected: {duplicates[:5]}..."
        )

    stats.increment_total(len(high_scores_list))

    # Normalize all scores
    high_scores_normalized = [_normalize_score(s) for s in high_scores_list]
    new_scores_normalized = [_normalize_score(s) for s in new_scores_list]

    # Build index for new scores: key -> list of (new_id, pp)
    new_scores_index: Dict[Tuple[int, int, str], List[Tuple[int, float]]] = {}
    for score in new_scores_normalized:
        key = _make_key(
            score["user_id"],
            score["beatmap_id"],
            score["mods_key"],
        )
        if key not in new_scores_index:
            new_scores_index[key] = []
        new_scores_index[key].append((score["score_id"], score["pp"]))

    # Build lookup
    lookup: Dict[int, int] = {}

    for high_score in high_scores_normalized:
        legacy_id = high_score["score_id"]
        target_pp = high_score["pp"]
        key = _make_key(
            high_score["user_id"],
            high_score["beatmap_id"],
            high_score["mods_key"],
        )

        matched = False

        if key in new_scores_index:
            candidates = new_scores_index[key]

            # Check for exact match first (handle None pp values)
            if target_pp is None:
                # If target pp is None, match with first candidate (can't compare)
                # This is a fuzzy match since we can't verify equality
                lookup[legacy_id] = candidates[0][0]
                stats.increment_fuzzy()
                matched = True
            else:
                exact_matches = [
                    (nid, pp)
                    for nid, pp in candidates
                    if pp is not None and abs(pp - target_pp) < 1e-6
                ]

                if exact_matches:
                    # Use first exact match
                    lookup[legacy_id] = exact_matches[0][0]
                    stats.increment_exact()
                    matched = True
                else:
                    # Fuzzy match: find closest pp (filter out None values)
                    valid_candidates = [
                        (nid, pp) for nid, pp in candidates if pp is not None
                    ]
                    if valid_candidates:
                        closest = min(
                            valid_candidates, key=lambda x: abs(x[1] - target_pp)
                        )
                        lookup[legacy_id] = closest[0]
                        stats.increment_fuzzy()
                        matched = True

        if not matched:
            stats.increment_unmatched(legacy_id=legacy_id)
            logger.warning(
                f"Unmatched legacy score ID {legacy_id}: "
                f"user={high_score['user_id']}, "
                f"beatmap={high_score['beatmap_id']}, "
                f"mods='{high_score['mods_key']}', pp={target_pp}"
            )

    # Log summary
    logger.info(f"ID Resolution Complete: {stats}")

    return lookup, stats
