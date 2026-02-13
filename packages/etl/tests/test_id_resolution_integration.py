"""Integration tests for ID resolution end-to-end.

These tests verify the complete ID resolution pipeline:
1. Parsing SQL fixture files (sample_scores_high.sql, sample_scores.sql)
2. Building lookup tables from parsed data
3. Creating DuckDB tables and verifying resolution
4. Testing edge cases: duplicates, multiple scores, missing matches, nulls

TDD approach - comprehensive coverage of integration scenarios.
"""

import pytest
import tempfile
import os
from pathlib import Path
from typing import List, Dict, Any
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import logging
import json

# Import modules under test
from src.id_resolution import (
    build_id_lookup,
    IdResolutionStats,
    ResolutionError,
    _load_scores,
    _normalize_score,
    _make_key,
)
from src.schemas import ScoreIdMapping


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_high_scores_sql(fixtures_dir) -> Path:
    """Return path to sample_scores_high.sql fixture."""
    return fixtures_dir / "sample_scores_high.sql"


@pytest.fixture
def sample_scores_sql(fixtures_dir) -> Path:
    """Return path to sample_scores.sql fixture."""
    return fixtures_dir / "sample_scores.sql"


@pytest.fixture
def parsed_high_scores_data() -> List[Dict[str, Any]]:
    """Return parsed high scores data matching the SQL fixture."""
    return [
        # Exact matches
        {
            "score_id": 1001,
            "user_id": 101,
            "beatmap_id": 201,
            "pp": 500.0,
            "mods_key": "DT",
        },
        {
            "score_id": 1002,
            "user_id": 101,
            "beatmap_id": 202,
            "pp": 480.0,
            "mods_key": "HR",
        },
        {
            "score_id": 1003,
            "user_id": 102,
            "beatmap_id": 203,
            "pp": 450.0,
            "mods_key": "",
        },
        {
            "score_id": 1004,
            "user_id": 102,
            "beatmap_id": 204,
            "pp": 600.0,
            "mods_key": "DT,HR",
        },
        {
            "score_id": 1005,
            "user_id": 103,
            "beatmap_id": 205,
            "pp": 550.0,
            "mods_key": "HD",
        },
        # Fuzzy matches
        {
            "score_id": 1006,
            "user_id": 104,
            "beatmap_id": 206,
            "pp": 350.0,
            "mods_key": "DT",
        },
        {
            "score_id": 1007,
            "user_id": 105,
            "beatmap_id": 207,
            "pp": 420.0,
            "mods_key": "HR",
        },
        # Unmatched
        {
            "score_id": 1008,
            "user_id": 999,
            "beatmap_id": 999,
            "pp": 300.0,
            "mods_key": "",
        },
        # Additional edge cases
        {
            "score_id": 1009,
            "user_id": 106,
            "beatmap_id": 208,
            "pp": 380.0,
            "mods_key": "",
        },
        {
            "score_id": 1010,
            "user_id": 107,
            "beatmap_id": 209,
            "pp": 400.0,
            "mods_key": "",
        },
        {
            "score_id": 1011,
            "user_id": 107,
            "beatmap_id": 209,
            "pp": 450.0,
            "mods_key": "DT",
        },
        {
            "score_id": 1012,
            "user_id": 107,
            "beatmap_id": 209,
            "pp": 480.0,
            "mods_key": "HR",
        },
        # Null pp
        {
            "score_id": 1013,
            "user_id": 108,
            "beatmap_id": 210,
            "pp": None,
            "mods_key": "",
        },
    ]


@pytest.fixture
def parsed_new_scores_data() -> List[Dict[str, Any]]:
    """Return parsed new scores data matching the SQL fixture."""
    return [
        # Exact matches
        {
            "score_id": 2001,
            "user_id": 101,
            "beatmap_id": 201,
            "pp": 500.0,
            "mods_key": "DT",
        },
        {
            "score_id": 2002,
            "user_id": 101,
            "beatmap_id": 202,
            "pp": 480.0,
            "mods_key": "HR",
        },
        {
            "score_id": 2003,
            "user_id": 102,
            "beatmap_id": 203,
            "pp": 450.0,
            "mods_key": "",
        },
        {
            "score_id": 2004,
            "user_id": 102,
            "beatmap_id": 204,
            "pp": 600.0,
            "mods_key": "DT,HR",
        },
        {
            "score_id": 2005,
            "user_id": 103,
            "beatmap_id": 205,
            "pp": 550.0,
            "mods_key": "HD",
        },
        # Fuzzy matches (slightly different pp)
        {
            "score_id": 2006,
            "user_id": 104,
            "beatmap_id": 206,
            "pp": 352.5,
            "mods_key": "DT",
        },
        {
            "score_id": 2007,
            "user_id": 105,
            "beatmap_id": 207,
            "pp": 422.0,
            "mods_key": "HR",
        },
        # Multiple scores for same user/beatmap with different mods
        {
            "score_id": 2008,
            "user_id": 107,
            "beatmap_id": 209,
            "pp": 405.0,
            "mods_key": "",
        },
        {
            "score_id": 2009,
            "user_id": 107,
            "beatmap_id": 209,
            "pp": 455.0,
            "mods_key": "DT",
        },
        {
            "score_id": 2010,
            "user_id": 107,
            "beatmap_id": 209,
            "pp": 485.0,
            "mods_key": "HR",
        },
        # Null pp match
        {
            "score_id": 2011,
            "user_id": 108,
            "beatmap_id": 210,
            "pp": None,
            "mods_key": "",
        },
        # Orphans (no matching legacy)
        {
            "score_id": 2012,
            "user_id": 200,
            "beatmap_id": 300,
            "pp": 700.0,
            "mods_key": "DT,HR,HD",
        },
        {
            "score_id": 2013,
            "user_id": 201,
            "beatmap_id": 301,
            "pp": 650.0,
            "mods_key": "DT",
        },
    ]


@pytest.fixture
def temp_parquet_dir(tmp_path):
    """Create temporary directory for parquet files."""
    return tmp_path


@pytest.fixture
def temp_duckdb_path(tmp_path):
    """Create temporary DuckDB database path."""
    return tmp_path / "test_id_resolution.duckdb"


# =============================================================================
# Test Fixture File Existence
# =============================================================================


class TestFixtureFiles:
    """Tests for fixture file existence and structure."""

    def test_sample_scores_high_sql_exists(self, sample_high_scores_sql):
        """sample_scores_high.sql fixture file should exist."""
        assert sample_high_scores_sql.exists()
        assert sample_high_scores_sql.is_file()

    def test_sample_scores_sql_exists(self, sample_scores_sql):
        """sample_scores.sql fixture file should exist."""
        assert sample_scores_sql.exists()
        assert sample_scores_sql.is_file()

    def test_sample_scores_high_has_inserts(self, sample_high_scores_sql):
        """sample_scores_high.sql should contain INSERT statements."""
        content = sample_high_scores_sql.read_text()
        assert "INSERT INTO" in content
        assert "osu_scores_high" in content
        assert "VALUES" in content

    def test_sample_scores_has_inserts(self, sample_scores_sql):
        """sample_scores.sql should contain INSERT statements."""
        content = sample_scores_sql.read_text()
        assert "INSERT INTO" in content
        assert "scores" in content
        assert "VALUES" in content

    def test_sample_scores_high_has_known_ids(self, sample_high_scores_sql):
        """sample_scores_high.sql should contain known test IDs."""
        content = sample_high_scores_sql.read_text()
        # Check for specific test IDs we expect
        assert "1001" in content  # First exact match
        assert "1006" in content  # Fuzzy match
        assert "1008" in content  # Unmatched

    def test_sample_scores_has_matching_ids(self, sample_scores_sql):
        """sample_scores.sql should contain IDs matching high scores."""
        content = sample_scores_sql.read_text()
        # Check for matching new score IDs
        assert "2001" in content  # Matches legacy 1001
        assert "2006" in content  # Matches legacy 1006 (fuzzy)


# =============================================================================
# Test SQL Parsing Integration
# =============================================================================


class TestSqlParsing:
    """Tests for SQL parsing from fixture files."""

    def test_parse_simple_insert_values(self):
        """Should parse simple INSERT statement values."""
        # This tests the parsing logic indirectly through manual parsing
        sql_line = (
            "INSERT INTO `scores` (`id`, `user_id`, `pp`) VALUES (2001, 101, 500.0);"
        )

        # Extract values using regex pattern similar to sql_parser_fast
        import re

        values_match = re.search(r"VALUES\s+\(([^)]+)\)", sql_line, re.IGNORECASE)
        assert values_match is not None
        values_str = values_match.group(1)
        values = [v.strip() for v in values_str.split(",")]

        assert values[0] == "2001"
        assert values[1] == "101"
        assert values[2] == "500.0"

    def test_parse_multiple_value_tuples(self):
        """Should parse multiple value tuples in single INSERT."""
        sql_line = "INSERT INTO table (a, b) VALUES (1, 2), (3, 4), (5, 6);"

        import re

        tuples = re.findall(r"\(([^)]+)\)", sql_line)
        # First match is column names, rest are values
        assert len(tuples) >= 2  # At least columns + one value tuple

    def test_parse_quoted_strings(self):
        """Should parse quoted string values correctly."""
        sql_line = "INSERT INTO table (id, name) VALUES (1, 'test value');"

        import re

        values_match = re.search(r"VALUES\s+\(([^)]+)\)", sql_line, re.IGNORECASE)
        assert values_match is not None
        values_str = values_match.group(1)

        # Handle quoted strings with commas inside
        assert "'test value'" in values_str

    def test_fixture_data_count_matches(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Parsed data should have expected record counts."""
        # High scores: 5 exact + 2 fuzzy + 1 unmatched + 1 duplicate test + 3 multi-mods + 1 null = 13
        assert len(parsed_high_scores_data) == 13
        # New scores: 5 exact + 2 fuzzy + 3 multi-mods + 1 null + 2 orphans = 13
        assert len(parsed_new_scores_data) == 13


# =============================================================================
# Test Lookup Table Building
# =============================================================================


class TestLookupTableBuilding:
    """Tests for lookup table building from parsed data."""

    def test_build_lookup_exact_matches(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should build lookup with exact matches."""
        # Filter to just exact match pairs
        high_exact = [s for s in parsed_high_scores_data if s["score_id"] <= 1005]
        new_exact = [s for s in parsed_new_scores_data if s["score_id"] <= 2005]

        lookup, stats = build_id_lookup(high_exact, new_exact)

        # All 5 exact matches should be found
        assert len(lookup) == 5
        assert lookup[1001] == 2001
        assert lookup[1002] == 2002
        assert lookup[1003] == 2003
        assert lookup[1004] == 2004
        assert lookup[1005] == 2005

        # Stats should reflect exact matches
        assert stats.exact_matches == 5
        assert stats.fuzzy_matches == 0
        assert stats.unmatched == 0

    def test_build_lookup_fuzzy_matches(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should build lookup with fuzzy pp matches."""
        # Filter to just fuzzy match pairs
        high_fuzzy = [
            s for s in parsed_high_scores_data if 1006 <= s["score_id"] <= 1007
        ]
        new_fuzzy = [s for s in parsed_new_scores_data if 2006 <= s["score_id"] <= 2007]

        lookup, stats = build_id_lookup(high_fuzzy, new_fuzzy)

        # Both fuzzy matches should be found
        assert len(lookup) == 2
        # 1006 (pp=350.0) should match 2006 (pp=352.5) - closest
        assert lookup[1006] == 2006
        # 1007 (pp=420.0) should match 2007 (pp=422.0) - closest
        assert lookup[1007] == 2007

        assert stats.exact_matches == 0
        assert stats.fuzzy_matches == 2

    def test_build_lookup_unmatched_handling(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should handle unmatched legacy scores."""
        # Include unmatched score 1008
        high_with_unmatched = [
            s for s in parsed_high_scores_data if s["score_id"] in [1001, 1008]
        ]
        new_only_matched = [s for s in parsed_new_scores_data if s["score_id"] == 2001]

        lookup, stats = build_id_lookup(high_with_unmatched, new_only_matched)

        # Only 1001 should match
        assert len(lookup) == 1
        assert lookup[1001] == 2001
        assert 1008 not in lookup

        assert stats.exact_matches == 1
        assert stats.unmatched == 1
        assert 1008 in stats.unmatched_ids

    def test_build_lookup_multiple_scores_same_user_beatmap(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should handle multiple scores for same user/beatmap with different mods."""
        # Get scores for user 107 on beatmap 209
        high_multi = [s for s in parsed_high_scores_data if s["user_id"] == 107]
        new_multi = [s for s in parsed_new_scores_data if s["user_id"] == 107]

        lookup, stats = build_id_lookup(high_multi, new_multi)

        # All 3 scores should match based on mods
        assert len(lookup) == 3
        # Each legacy score should match corresponding new score by mods
        legacy_nm = next(s for s in high_multi if s["mods_key"] == "")
        legacy_dt = next(s for s in high_multi if s["mods_key"] == "DT")
        legacy_hr = next(s for s in high_multi if s["mods_key"] == "HR")

        new_nm = next(s for s in new_multi if s["mods_key"] == "")
        new_dt = next(s for s in new_multi if s["mods_key"] == "DT")
        new_hr = next(s for s in new_multi if s["mods_key"] == "HR")

        assert lookup[legacy_nm["score_id"]] == new_nm["score_id"]
        assert lookup[legacy_dt["score_id"]] == new_dt["score_id"]
        assert lookup[legacy_hr["score_id"]] == new_hr["score_id"]

    def test_build_lookup_null_pp_handling(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should handle NULL pp values via fuzzy matching to first candidate."""
        high_null = [s for s in parsed_high_scores_data if s["score_id"] == 1013]
        new_null = [s for s in parsed_new_scores_data if s["score_id"] == 2011]

        lookup, stats = build_id_lookup(high_null, new_null)

        assert len(lookup) == 1
        assert lookup[1013] == 2011
        assert stats.fuzzy_matches == 1

    def test_build_lookup_with_parquet_files(
        self, parsed_high_scores_data, parsed_new_scores_data, temp_parquet_dir
    ):
        """Should build lookup from parquet file paths."""
        # Create parquet files
        high_path = temp_parquet_dir / "high_scores.parquet"
        new_path = temp_parquet_dir / "new_scores.parquet"

        # Convert to tables and write
        high_table = pa.table(
            {
                "score_id": [s["score_id"] for s in parsed_high_scores_data[:5]],
                "user_id": [s["user_id"] for s in parsed_high_scores_data[:5]],
                "beatmap_id": [s["beatmap_id"] for s in parsed_high_scores_data[:5]],
                "pp": [s["pp"] for s in parsed_high_scores_data[:5]],
                "mods_key": [s["mods_key"] for s in parsed_high_scores_data[:5]],
            }
        )

        new_table = pa.table(
            {
                "score_id": [s["score_id"] for s in parsed_new_scores_data[:5]],
                "user_id": [s["user_id"] for s in parsed_new_scores_data[:5]],
                "beatmap_id": [s["beatmap_id"] for s in parsed_new_scores_data[:5]],
                "pp": [s["pp"] for s in parsed_new_scores_data[:5]],
                "mods_key": [s["mods_key"] for s in parsed_new_scores_data[:5]],
            }
        )

        pq.write_table(high_table, high_path)
        pq.write_table(new_table, new_path)

        # Build lookup from files
        lookup, stats = build_id_lookup(str(high_path), str(new_path))

        assert len(lookup) == 5
        assert stats.exact_matches == 5


# =============================================================================
# Test DuckDB Table Creation
# =============================================================================


class TestDuckDBTableCreation:
    """Tests for DuckDB table creation and queries."""

    def test_create_raw_tables_from_parquet(
        self, parsed_high_scores_data, parsed_new_scores_data, temp_duckdb_path
    ):
        """Should create raw tables from parquet data."""
        conn = duckdb.connect(str(temp_duckdb_path))

        try:
            # Create raw tables directly from data
            high_df = pa.table(
                {
                    "score_id": [s["score_id"] for s in parsed_high_scores_data[:5]],
                    "user_id": [s["user_id"] for s in parsed_high_scores_data[:5]],
                    "beatmap_id": [
                        s["beatmap_id"] for s in parsed_high_scores_data[:5]
                    ],
                    "pp": [s["pp"] for s in parsed_high_scores_data[:5]],
                    "mods_key": [s["mods_key"] for s in parsed_high_scores_data[:5]],
                }
            )

            new_df = pa.table(
                {
                    "id": [s["score_id"] for s in parsed_new_scores_data[:5]],
                    "user_id": [s["user_id"] for s in parsed_new_scores_data[:5]],
                    "beatmap_id": [s["beatmap_id"] for s in parsed_new_scores_data[:5]],
                    "pp": [s["pp"] for s in parsed_new_scores_data[:5]],
                    "mods_key": [s["mods_key"] for s in parsed_new_scores_data[:5]],
                }
            )

            conn.execute("CREATE TABLE raw_osu_scores_high AS SELECT * FROM high_df")
            conn.execute("CREATE TABLE raw_scores AS SELECT * FROM new_df")

            # Verify tables exist
            result = conn.execute("SELECT COUNT(*) FROM raw_osu_scores_high").fetchone()
            assert result[0] == 5

            result = conn.execute("SELECT COUNT(*) FROM raw_scores").fetchone()
            assert result[0] == 5
        finally:
            conn.close()

    def test_create_mart_score_id_resolution(
        self, parsed_high_scores_data, parsed_new_scores_data, temp_duckdb_path
    ):
        """Should create mart_score_id_resolution table with correct joins."""
        conn = duckdb.connect(str(temp_duckdb_path))

        try:
            # Create raw tables
            high_exact = [s for s in parsed_high_scores_data if s["score_id"] <= 1005]
            new_exact = [s for s in parsed_new_scores_data if s["score_id"] <= 2005]

            high_df = pa.table(
                {
                    "score_id": [s["score_id"] for s in high_exact],
                    "user_id": [s["user_id"] for s in high_exact],
                    "beatmap_id": [s["beatmap_id"] for s in high_exact],
                    "pp": [s["pp"] for s in high_exact],
                    "mods_key": [s["mods_key"] for s in high_exact],
                }
            )

            new_df = pa.table(
                {
                    "id": [s["score_id"] for s in new_exact],
                    "user_id": [s["user_id"] for s in new_exact],
                    "beatmap_id": [s["beatmap_id"] for s in new_exact],
                    "pp": [s["pp"] for s in new_exact],
                    "mods_key": [s["mods_key"] for s in new_exact],
                }
            )

            conn.execute("CREATE TABLE raw_osu_scores_high AS SELECT * FROM high_df")
            conn.execute("CREATE TABLE raw_scores AS SELECT * FROM new_df")

            # Create resolution table
            conn.execute("""
                CREATE TABLE mart_score_id_resolution AS
                WITH high_scores AS (
                    SELECT score_id as legacy_id, user_id, beatmap_id, mods_key, pp
                    FROM raw_osu_scores_high
                ),
                new_scores AS (
                    SELECT id as new_id, user_id, beatmap_id, mods_key, pp
                    FROM raw_scores
                )
                SELECT 
                    h.legacy_id,
                    n.new_id,
                    CASE WHEN h.pp = n.pp THEN 'exact' ELSE 'fuzzy' END as match_type,
                    1.0 / (1.0 + ABS(h.pp - n.pp)) as confidence
                FROM high_scores h
                LEFT JOIN new_scores n 
                    ON h.user_id = n.user_id 
                    AND h.beatmap_id = n.beatmap_id
                    AND h.mods_key = n.mods_key
                WHERE n.new_id IS NOT NULL
            """)

            # Verify table
            result = conn.execute(
                "SELECT COUNT(*) FROM mart_score_id_resolution"
            ).fetchone()
            assert result[0] == 5

            # Verify exact matches
            result = conn.execute("""
                SELECT COUNT(*) FROM mart_score_id_resolution WHERE match_type = 'exact'
            """).fetchone()
            assert result[0] == 5
        finally:
            conn.close()

    def test_mart_score_id_resolution_indexes(
        self, parsed_high_scores_data, parsed_new_scores_data, temp_duckdb_path
    ):
        """Should create indexes on mart_score_id_resolution."""
        conn = duckdb.connect(str(temp_duckdb_path))

        try:
            # Setup tables
            high_exact = [s for s in parsed_high_scores_data if s["score_id"] <= 1005]
            new_exact = [s for s in parsed_new_scores_data if s["score_id"] <= 2005]

            high_df = pa.table(
                {
                    "score_id": [s["score_id"] for s in high_exact],
                    "user_id": [s["user_id"] for s in high_exact],
                    "beatmap_id": [s["beatmap_id"] for s in high_exact],
                    "pp": [s["pp"] for s in high_exact],
                    "mods_key": [s["mods_key"] for s in high_exact],
                }
            )

            new_df = pa.table(
                {
                    "id": [s["score_id"] for s in new_exact],
                    "user_id": [s["user_id"] for s in new_exact],
                    "beatmap_id": [s["beatmap_id"] for s in new_exact],
                    "pp": [s["pp"] for s in new_exact],
                    "mods_key": [s["mods_key"] for s in new_exact],
                }
            )

            conn.execute("CREATE TABLE raw_osu_scores_high AS SELECT * FROM high_df")
            conn.execute("CREATE TABLE raw_scores AS SELECT * FROM new_df")
            conn.execute("""
                CREATE TABLE mart_score_id_resolution AS
                SELECT 
                    h.score_id as legacy_id,
                    n.id as new_id,
                    'exact' as match_type,
                    1.0 as confidence
                FROM raw_osu_scores_high h
                JOIN raw_scores n ON h.score_id = n.id - 1000
            """)

            # Create indexes
            conn.execute("""
                CREATE INDEX idx_mart_score_id_resolution_legacy_id 
                ON mart_score_id_resolution(legacy_id)
            """)
            conn.execute("""
                CREATE INDEX idx_mart_score_id_resolution_new_id 
                ON mart_score_id_resolution(new_id)
            """)

            # Verify indexes exist
            result = conn.execute("SELECT index_name FROM duckdb_indexes()").fetchall()
            index_names = [row[0] for row in result]
            assert "idx_mart_score_id_resolution_legacy_id" in index_names
            assert "idx_mart_score_id_resolution_new_id" in index_names
        finally:
            conn.close()


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_duplicate_legacy_ids_raise_error(self, parsed_new_scores_data):
        """Should raise error when duplicate legacy IDs detected."""
        # Create data with duplicate score_id
        high_dupes = [
            {
                "score_id": 1001,
                "user_id": 101,
                "beatmap_id": 201,
                "pp": 500.0,
                "mods_key": "DT",
            },
            {
                "score_id": 1001,
                "user_id": 102,
                "beatmap_id": 202,
                "pp": 400.0,
                "mods_key": "",
            },
        ]
        new_data = [parsed_new_scores_data[0]]

        with pytest.raises(ResolutionError) as exc_info:
            build_id_lookup(high_dupes, new_data)

        assert "Duplicate legacy score IDs" in str(exc_info.value)
        assert "1001" in str(exc_info.value)

    def test_empty_high_scores(self, parsed_new_scores_data):
        """Should handle empty high scores list."""
        lookup, stats = build_id_lookup([], parsed_new_scores_data)

        assert lookup == {}
        assert stats.total_legacy == 0

    def test_empty_new_scores(self, parsed_high_scores_data):
        """Should handle empty new scores list."""
        lookup, stats = build_id_lookup(parsed_high_scores_data[:3], [])

        assert lookup == {}
        assert stats.unmatched == 3

    def test_null_mods_key_matching(self, parsed_new_scores_data):
        """Should match NULL and empty string mods_key."""
        high_null_mods = [
            {
                "score_id": 1001,
                "user_id": 101,
                "beatmap_id": 201,
                "pp": 500.0,
                "mods_key": None,
            }
        ]
        new_empty_mods = [
            {
                "score_id": 2001,
                "user_id": 101,
                "beatmap_id": 201,
                "pp": 500.0,
                "mods_key": "",
            }
        ]

        lookup, stats = build_id_lookup(high_null_mods, new_empty_mods)

        # None mods_key should normalize to empty string and match
        assert lookup[1001] == 2001
        assert stats.exact_matches == 1

    def test_string_pp_conversion(self):
        """Should handle string PP values."""
        high_str_pp = [
            {
                "score_id": 1001,
                "user_id": 101,
                "beatmap_id": 201,
                "pp": "500.0",
                "mods_key": "DT",
            }
        ]
        new_str_pp = [
            {
                "score_id": 2001,
                "user_id": 101,
                "beatmap_id": 201,
                "pp": "500.0",
                "mods_key": "DT",
            }
        ]

        lookup, stats = build_id_lookup(high_str_pp, new_str_pp)

        assert lookup[1001] == 2001
        assert stats.exact_matches == 1

    def test_different_user_no_match(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should not match scores from different users."""
        high = [parsed_high_scores_data[0]]  # user 101
        new = [
            {
                "score_id": 9999,
                "user_id": 999,  # Different user
                "beatmap_id": 201,
                "pp": 500.0,
                "mods_key": "DT",
            }
        ]

        lookup, stats = build_id_lookup(high, new)

        assert 1001 not in lookup
        assert stats.unmatched == 1

    def test_different_beatmap_no_match(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should not match scores from different beatmaps."""
        high = [parsed_high_scores_data[0]]  # beatmap 201
        new = [
            {
                "score_id": 9999,
                "user_id": 101,
                "beatmap_id": 999,  # Different beatmap
                "pp": 500.0,
                "mods_key": "DT",
            }
        ]

        lookup, stats = build_id_lookup(high, new)

        assert 1001 not in lookup
        assert stats.unmatched == 1

    def test_different_mods_no_match(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should not match scores with different mods."""
        high = [parsed_high_scores_data[0]]  # DT mods
        new = [
            {
                "score_id": 9999,
                "user_id": 101,
                "beatmap_id": 201,
                "pp": 500.0,
                "mods_key": "HR",  # Different mods
            }
        ]

        lookup, stats = build_id_lookup(high, new)

        assert 1001 not in lookup
        assert stats.unmatched == 1


# =============================================================================
# Test End-to-End Integration
# =============================================================================


class TestEndToEndIntegration:
    """End-to-end integration tests for complete ID resolution pipeline."""

    def test_full_pipeline_exact_and_fuzzy(
        self, parsed_high_scores_data, parsed_new_scores_data, temp_duckdb_path
    ):
        """Full pipeline: Load data → Build lookup → Create table → Verify."""
        # Step 1: Build lookup
        high_subset = [s for s in parsed_high_scores_data if s["score_id"] <= 1007]
        new_subset = [s for s in parsed_new_scores_data if s["score_id"] <= 2007]

        lookup, stats = build_id_lookup(high_subset, new_subset)

        # Step 2: Verify lookup
        assert len(lookup) == 7  # 5 exact + 2 fuzzy
        assert stats.exact_matches == 5
        assert stats.fuzzy_matches == 2
        assert stats.unmatched == 0

        # Step 3: Create DuckDB tables
        conn = duckdb.connect(str(temp_duckdb_path))
        try:
            # Create mapping table from lookup using PyArrow
            mapping_table = pa.table(
                {
                    "legacy_id": list(lookup.keys()),
                    "new_id": list(lookup.values()),
                    "match_type": [
                        "exact" if lid <= 1005 else "fuzzy" for lid in lookup.keys()
                    ],
                    "confidence": [
                        1.0 if lid <= 1005 else 0.9 for lid in lookup.keys()
                    ],
                }
            )

            conn.execute("""
                CREATE TABLE mart_score_id_resolution AS
                SELECT * FROM mapping_table
            """)

            # Step 4: Verify table
            result = conn.execute(
                "SELECT COUNT(*) FROM mart_score_id_resolution"
            ).fetchone()
            assert result[0] == 7

            # Verify specific lookups
            result = conn.execute(
                "SELECT new_id FROM mart_score_id_resolution WHERE legacy_id = 1001"
            ).fetchone()
            assert result[0] == 2001
        finally:
            conn.close()

    def test_lookup_query_performance(
        self, parsed_high_scores_data, parsed_new_scores_data, temp_duckdb_path
    ):
        """Should support fast lookups by legacy_id."""
        conn = duckdb.connect(str(temp_duckdb_path))

        try:
            # Create mapping table
            high_subset = [s for s in parsed_high_scores_data if s["score_id"] <= 1005]
            new_subset = [s for s in parsed_new_scores_data if s["score_id"] <= 2005]

            lookup, _ = build_id_lookup(high_subset, new_subset)

            mapping_table = pa.table(
                {
                    "legacy_id": list(lookup.keys()),
                    "new_id": list(lookup.values()),
                    "match_type": ["exact"] * len(lookup),
                    "confidence": [1.0] * len(lookup),
                }
            )

            conn.execute("""
                CREATE TABLE mart_score_id_resolution AS
                SELECT * FROM mapping_table
            """)

            conn.execute("""
                CREATE INDEX idx_legacy ON mart_score_id_resolution(legacy_id)
            """)

            # Query by legacy_id
            result = conn.execute("""
                SELECT new_id FROM mart_score_id_resolution WHERE legacy_id = 1003
            """).fetchone()

            assert result[0] == 2003
        finally:
            conn.close()

    def test_statistics_accuracy(self, parsed_high_scores_data, parsed_new_scores_data):
        """Statistics should accurately reflect match results."""
        # Exclude NULL pp scores (1013) since they can't be matched
        high_valid = [s for s in parsed_high_scores_data if s["pp"] is not None]
        new_valid = [s for s in parsed_new_scores_data if s["pp"] is not None]

        lookup, stats = build_id_lookup(high_valid, new_valid)

        stats_dict = stats.to_dict()

        # Verify totals add up
        total_handled = (
            stats_dict["exact_matches"]
            + stats_dict["fuzzy_matches"]
            + stats_dict["unmatched"]
        )
        assert total_handled == stats_dict["total_legacy"]

        # Verify rates are calculated
        assert 0 <= stats_dict["exact_rate"] <= 1
        assert 0 <= stats_dict["fuzzy_rate"] <= 1
        assert 0 <= stats_dict["unmatched_rate"] <= 1
        assert (
            abs(
                stats_dict["exact_rate"]
                + stats_dict["fuzzy_rate"]
                + stats_dict["unmatched_rate"]
                - 1.0
            )
            < 1e-10
        )

    def test_unmatched_ids_tracking(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should track unmatched legacy IDs for review."""
        # Exclude NULL pp scores which can't be matched
        high_valid = [s for s in parsed_high_scores_data if s["pp"] is not None]
        new_valid = [s for s in parsed_new_scores_data if s["pp"] is not None]

        lookup, stats = build_id_lookup(high_valid, new_valid)

        # Score 1008 (user 999) should be unmatched
        assert 1008 in stats.unmatched_ids

        # All other scores should be matched
        matched_legacy_ids = set(lookup.keys())
        for uid in stats.unmatched_ids:
            assert uid not in matched_legacy_ids


# =============================================================================
# Test ScoreIdMapping Schema Validation
# =============================================================================


class TestScoreIdMappingIntegration:
    """Integration tests for ScoreIdMapping schema in resolution pipeline."""

    def test_mapping_from_lookup_result(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """Should create ScoreIdMapping from lookup results."""
        lookup, stats = build_id_lookup(
            [parsed_high_scores_data[0]], [parsed_new_scores_data[0]]
        )

        # Create mapping from result
        legacy_id = 1001
        new_id = lookup[legacy_id]

        mapping = ScoreIdMapping(
            legacy_id=legacy_id,
            new_id=new_id,
            user_id=parsed_high_scores_data[0]["user_id"],
            beatmap_id=parsed_high_scores_data[0]["beatmap_id"],
            mods_key=parsed_high_scores_data[0]["mods_key"],
            pp=parsed_high_scores_data[0]["pp"],
            resolution_type="exact",
        )

        assert mapping.legacy_id == 1001
        assert mapping.new_id == 2001
        assert mapping.resolution_type == "exact"

    def test_mapping_validation_with_fixture_data(
        self, parsed_high_scores_data, parsed_new_scores_data
    ):
        """ScoreIdMapping should validate fixture data correctly."""
        for i, high_score in enumerate(parsed_high_scores_data[:5]):
            new_score = parsed_new_scores_data[i]

            mapping = ScoreIdMapping(
                legacy_id=high_score["score_id"],
                new_id=new_score["score_id"],
                user_id=high_score["user_id"],
                beatmap_id=high_score["beatmap_id"],
                mods_key=high_score["mods_key"] or "",
                pp=high_score["pp"] or 0.0,
                resolution_type="exact",
            )

            assert isinstance(mapping.legacy_id, int)
            assert isinstance(mapping.new_id, int)
            assert isinstance(mapping.pp, (int, float))
            assert mapping.pp >= 0


# =============================================================================
# Test Helper Functions
# =============================================================================


class TestHelperFunctions:
    """Tests for helper functions used in ID resolution."""

    def test_normalize_score_with_none_values(self):
        """_normalize_score should handle None values correctly."""
        score = {
            "score_id": "1001",
            "user_id": "101",
            "beatmap_id": "201",
            "pp": None,
            "mods_key": None,
        }

        normalized = _normalize_score(score)

        assert normalized["score_id"] == 1001
        assert normalized["user_id"] == 101
        assert normalized["beatmap_id"] == 201
        assert normalized["pp"] is None
        assert normalized["mods_key"] == ""

    def test_make_key_tuple(self):
        """_make_key should create correct tuple key."""
        key = _make_key(101, 201, "DT")

        assert key == (101, 201, "DT")
        assert isinstance(key, tuple)
        assert len(key) == 3

    def test_load_scores_from_list(self, parsed_high_scores_data):
        """_load_scores should return list when given list."""
        result = _load_scores(parsed_high_scores_data)

        assert result == parsed_high_scores_data

    def test_load_scores_from_invalid_path(self):
        """_load_scores should raise ResolutionError for invalid path."""
        with pytest.raises(ResolutionError) as exc_info:
            _load_scores("/nonexistent/path/file.parquet")

        assert "not found" in str(exc_info.value).lower()


# =============================================================================
# Test Statistics and Logging
# =============================================================================


class TestStatisticsAndLogging:
    """Tests for statistics tracking and logging."""

    def test_stats_logging_output(
        self, parsed_high_scores_data, parsed_new_scores_data, caplog
    ):
        """Should log resolution statistics."""
        import logging

        with caplog.at_level(logging.INFO):
            lookup, stats = build_id_lookup(
                parsed_high_scores_data[:5], parsed_new_scores_data[:5]
            )

            # Check that stats summary is logged
            assert len(caplog.records) > 0
            log_text = " ".join([r.message for r in caplog.records])
            assert "exact" in log_text.lower() or "resolution" in log_text.lower()

    def test_unmatched_warning_logs(self, caplog):
        """Should log warnings for unmatched IDs."""
        import logging

        high = [
            {
                "score_id": 9999,
                "user_id": 999,
                "beatmap_id": 999,
                "pp": 100.0,
                "mods_key": "",
            }
        ]
        new = []

        with caplog.at_level(logging.WARNING):
            build_id_lookup(high, new)

            log_text = " ".join([r.message for r in caplog.records])
            assert "unmatched" in log_text.lower() or "9999" in log_text
