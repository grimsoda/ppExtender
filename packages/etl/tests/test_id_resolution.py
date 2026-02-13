"""Tests for score ID resolution logic.

TDD approach - write failing tests first (RED phase).
Tests cover:
- Exact match resolution (user_id + beatmap_id + mods_key + pp)
- Fuzzy match resolution (user_id + beatmap_id + mods_key with closest pp)
- Unmatched handling
- Statistics tracking
- Edge cases and error handling
"""

import pytest
from typing import Dict, List, Any
from unittest.mock import Mock, patch
import tempfile
import os
import json

from src.id_resolution import (
    build_id_lookup,
    IdResolutionStats,
    ResolutionError,
)
from src.schemas import ScoreIdMapping


class TestScoreIdMappingSchema:
    """Test cases for ScoreIdMapping Pydantic model."""

    def test_score_id_mapping_valid(self):
        """Test ScoreIdMapping with valid data."""
        mapping = ScoreIdMapping(
            legacy_id=1001,
            new_id=2001,
            user_id=123,
            beatmap_id=456,
            mods_key="DT",
            pp=350.5,
            resolution_type="exact",
        )
        assert mapping.legacy_id == 1001
        assert mapping.new_id == 2001
        assert mapping.resolution_type == "exact"

    def test_score_id_mapping_type_coercion(self):
        """Test ScoreIdMapping type coercion from strings."""
        mapping = ScoreIdMapping(
            legacy_id="1001",  # String input
            new_id="2001",  # String input
            user_id="123",  # String input
            beatmap_id="456",  # String input
            mods_key="DT",
            pp="350.5",  # String input
            resolution_type="exact",
        )
        assert mapping.legacy_id == 1001
        assert mapping.new_id == 2001
        assert isinstance(mapping.pp, float)

    def test_score_id_mapping_invalid_resolution_type(self):
        """Test ScoreIdMapping with invalid resolution_type."""
        with pytest.raises(ValueError):
            ScoreIdMapping(
                legacy_id=1001,
                new_id=2001,
                user_id=123,
                beatmap_id=456,
                mods_key="DT",
                pp=350.5,
                resolution_type="invalid",  # Should be 'exact', 'fuzzy', or 'unmatched'
            )


class TestIdResolutionStats:
    """Test cases for IdResolutionStats."""

    def test_stats_initialization(self):
        """Test stats initialization with zero values."""
        stats = IdResolutionStats()
        assert stats.total_legacy == 0
        assert stats.exact_matches == 0
        assert stats.fuzzy_matches == 0
        assert stats.unmatched == 0

    def test_stats_update(self):
        """Test stats update methods."""
        stats = IdResolutionStats()
        stats.increment_total(10)
        stats.increment_exact()
        stats.increment_exact()
        stats.increment_fuzzy()
        stats.increment_unmatched()

        assert stats.total_legacy == 10
        assert stats.exact_matches == 2
        assert stats.fuzzy_matches == 1
        assert stats.unmatched == 1

    def test_stats_to_dict(self):
        """Test stats conversion to dictionary."""
        stats = IdResolutionStats()
        stats.increment_total(100)
        stats.increment_exact(50)
        stats.increment_fuzzy(30)
        stats.increment_unmatched(20)

        result = stats.to_dict()
        assert result["total_legacy"] == 100
        assert result["exact_matches"] == 50
        assert result["fuzzy_matches"] == 30
        assert result["unmatched"] == 20
        assert result["exact_rate"] == 0.5
        assert result["fuzzy_rate"] == 0.3
        assert result["unmatched_rate"] == 0.2
        assert result["total_matched"] == 80
        assert result["match_rate"] == 0.8

    def test_stats_str(self):
        """Test stats string representation."""
        stats = IdResolutionStats()
        stats.increment_total(100)
        stats.increment_exact(50)
        stats.increment_fuzzy(30)
        stats.increment_unmatched(20)

        str_repr = str(stats)
        assert "100" in str_repr
        assert "50" in str_repr
        assert "exact" in str_repr.lower()


class TestBuildIdLookupExactMatch:
    """Test cases for exact match resolution."""

    def test_exact_match_single(self):
        """Test exact match with single score."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            }
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            }
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001
        assert stats.exact_matches == 1
        assert stats.fuzzy_matches == 0
        assert stats.unmatched == 0

    def test_exact_match_multiple(self):
        """Test exact match with multiple scores."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 1002,
                "user_id": 124,
                "beatmap_id": 457,
                "pp": 400.0,
                "mods_key": "HR",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 2002,
                "user_id": 124,
                "beatmap_id": 457,
                "pp": 400.0,
                "mods_key": "HR",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001
        assert lookup[1002] == 2002
        assert stats.exact_matches == 2

    def test_exact_match_same_user_different_beatmaps(self):
        """Test exact match for same user on different beatmaps."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 1002,
                "user_id": 123,
                "beatmap_id": 457,
                "pp": 400.0,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 2002,
                "user_id": 123,
                "beatmap_id": 457,
                "pp": 400.0,
                "mods_key": "DT",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001
        assert lookup[1002] == 2002
        assert stats.exact_matches == 2

    def test_exact_match_same_beatmap_different_mods(self):
        """Test exact match for same beatmap with different mods."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 1002,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 380.0,
                "mods_key": "HR",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 2002,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 380.0,
                "mods_key": "HR",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001
        assert lookup[1002] == 2002
        assert stats.exact_matches == 2


class TestBuildIdLookupFuzzyMatch:
    """Test cases for fuzzy match resolution."""

    def test_fuzzy_match_closest_pp(self):
        """Test fuzzy match finds closest pp value."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.0,
                "mods_key": "DT",
            },  # Closest
            {
                "score_id": 2002,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 400.0,
                "mods_key": "DT",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001  # Should match closest PP (350.0)
        assert stats.exact_matches == 0
        assert stats.fuzzy_matches == 1

    def test_fuzzy_match_multiple_candidates(self):
        """Test fuzzy match with multiple candidates, picks closest."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 340.0,
                "mods_key": "DT",
            },  # 10.5 diff
            {
                "score_id": 2002,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 355.0,
                "mods_key": "DT",
            },  # 4.5 diff - closest
            {
                "score_id": 2003,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 360.0,
                "mods_key": "DT",
            },  # 9.5 diff
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2002  # Should match PP closest to 350.5 (355.0)
        assert stats.fuzzy_matches == 1

    def test_fuzzy_match_not_exact_but_close(self):
        """Test that exact pp match takes precedence over fuzzy."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },  # Exact
            {
                "score_id": 2002,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.0,
                "mods_key": "DT",
            },  # Close
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001  # Should match exact
        assert stats.exact_matches == 1
        assert stats.fuzzy_matches == 0

    def test_fuzzy_match_different_users_not_mixed(self):
        """Test fuzzy match doesn't mix different users."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 999,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert 1001 not in lookup  # Different user, should not match
        assert stats.unmatched == 1


class TestBuildIdLookupUnmatched:
    """Test cases for unmatched handling."""

    def test_unmatched_no_match(self):
        """Test unmatched when no match exists."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 999,
                "beatmap_id": 999,
                "pp": 999.0,
                "mods_key": "HR",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert 1001 not in lookup
        assert stats.unmatched == 1
        assert stats.exact_matches == 0
        assert stats.fuzzy_matches == 0

    def test_unmatched_logged(self):
        """Test that unmatched IDs are logged."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        new_scores = []

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert 1001 not in lookup
        assert stats.unmatched == 1
        assert len(stats.unmatched_ids) == 1
        assert 1001 in stats.unmatched_ids


class TestBuildIdLookupEdgeCases:
    """Test cases for edge cases."""

    def test_empty_inputs(self):
        """Test with empty inputs."""
        lookup, stats = build_id_lookup([], [])
        assert lookup == {}
        assert stats.total_legacy == 0

    def test_empty_high_scores(self):
        """Test with empty high_scores."""
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        lookup, stats = build_id_lookup([], new_scores)
        assert lookup == {}
        assert stats.total_legacy == 0

    def test_empty_new_scores(self):
        """Test with empty new_scores."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]
        lookup, stats = build_id_lookup(high_scores, [])
        assert lookup == {}
        assert stats.unmatched == 1

    def test_duplicate_high_score_ids(self):
        """Test handling of duplicate legacy score IDs."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 457,
                "pp": 400.0,
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]

        # Should raise error or handle gracefully
        with pytest.raises(ResolutionError):
            build_id_lookup(high_scores, new_scores)

    def test_string_pp_values(self):
        """Test handling of string PP values."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": "350.5",
                "mods_key": "DT",
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": "350.5",
                "mods_key": "DT",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001
        assert stats.exact_matches == 1

    def test_none_mods_key(self):
        """Test handling of None mods_key."""
        high_scores = [
            {
                "score_id": 1001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": None,
            },
        ]
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "",
            },
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert lookup[1001] == 2001  # None and "" should match


class TestBuildIdLookupLargeDataset:
    """Test cases for performance with larger datasets."""

    def test_large_dataset_performance(self):
        """Test with moderately large dataset."""
        n = 1000
        high_scores = [
            {
                "score_id": 1000 + i,
                "user_id": i % 100,
                "beatmap_id": 456 + (i % 10),
                "pp": 300.0 + (i % 50),
                "mods_key": "DT" if i % 2 == 0 else "HR",
            }
            for i in range(n)
        ]
        new_scores = [
            {
                "score_id": 2000 + i,
                "user_id": i % 100,
                "beatmap_id": 456 + (i % 10),
                "pp": 300.0 + (i % 50),
                "mods_key": "DT" if i % 2 == 0 else "HR",
            }
            for i in range(n)
        ]

        lookup, stats = build_id_lookup(high_scores, new_scores)

        assert len(lookup) == n
        assert stats.exact_matches == n


class TestBuildIdLookupFromFiles:
    """Test cases for loading from file paths."""

    def test_from_parquet_paths(self, tmp_path):
        """Test loading from parquet file paths."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create test parquet files
        high_scores_data = {
            "score_id": [1001, 1002],
            "user_id": [123, 124],
            "beatmap_id": [456, 457],
            "pp": [350.5, 400.0],
            "mods_key": ["DT", "HR"],
        }
        new_scores_data = {
            "score_id": [2001, 2002],
            "user_id": [123, 124],
            "beatmap_id": [456, 457],
            "pp": [350.5, 400.0],
            "mods_key": ["DT", "HR"],
        }

        high_path = tmp_path / "high_scores.parquet"
        new_path = tmp_path / "new_scores.parquet"

        pq.write_table(pa.table(high_scores_data), high_path)
        pq.write_table(pa.table(new_scores_data), new_path)

        lookup, stats = build_id_lookup(str(high_path), str(new_path))

        assert lookup[1001] == 2001
        assert lookup[1002] == 2002
        assert stats.exact_matches == 2

    def test_from_parquet_high_only(self, tmp_path):
        """Test with only high_scores as parquet path."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        high_scores_data = {
            "score_id": [1001],
            "user_id": [123],
            "beatmap_id": [456],
            "pp": [350.5],
            "mods_key": ["DT"],
        }
        new_scores = [
            {
                "score_id": 2001,
                "user_id": 123,
                "beatmap_id": 456,
                "pp": 350.5,
                "mods_key": "DT",
            },
        ]

        high_path = tmp_path / "high_scores.parquet"
        pq.write_table(pa.table(high_scores_data), high_path)

        lookup, stats = build_id_lookup(str(high_path), new_scores)

        assert lookup[1001] == 2001

    def test_invalid_file_path(self):
        """Test with invalid file path raises error."""
        with pytest.raises(ResolutionError):
            build_id_lookup("/nonexistent/path.parquet", [])


class TestIdResolutionLogging:
    """Test cases for logging behavior."""

    def test_logs_unmatched_ids(self, caplog):
        """Test that unmatched IDs are logged."""
        import logging

        with caplog.at_level(logging.WARNING):
            high_scores = [
                {
                    "score_id": 1001,
                    "user_id": 123,
                    "beatmap_id": 456,
                    "pp": 350.5,
                    "mods_key": "DT",
                },
            ]
            new_scores = []

            build_id_lookup(high_scores, new_scores)

            assert "unmatched" in caplog.text.lower() or "1001" in caplog.text

    def test_logs_summary(self, caplog):
        """Test that summary stats are logged."""
        import logging

        with caplog.at_level(logging.INFO):
            high_scores = [
                {
                    "score_id": 1001,
                    "user_id": 123,
                    "beatmap_id": 456,
                    "pp": 350.5,
                    "mods_key": "DT",
                },
            ]
            new_scores = [
                {
                    "score_id": 2001,
                    "user_id": 123,
                    "beatmap_id": 456,
                    "pp": 350.5,
                    "mods_key": "DT",
                },
            ]

            build_id_lookup(high_scores, new_scores)

            assert "exact" in caplog.text.lower() or "match" in caplog.text
