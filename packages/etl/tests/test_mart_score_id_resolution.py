"""
Tests for mart_score_id_resolution table creation.

This module tests the ID resolution table creation:
- Table creation with legacy_id, new_id, match_type, confidence columns
- Exact vs fuzzy match classification
- Index creation for fast lookups
- Statistics logging after creation
"""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def legacy_scores_high_data():
    """Sample legacy scores from osu_scores_high table."""
    return {
        "score_id": [1001, 1002, 1003, 1004, 1005],
        "user_id": [101, 101, 102, 102, 103],
        "beatmap_id": [201, 202, 203, 204, 205],
        "mods_key": ["DT", "HR", "", "DT,HR", "HD"],
        "pp": [500.0, 480.0, 450.0, 600.0, 550.0],
    }


@pytest.fixture
def new_scores_data():
    """Sample new scores from scores table for matching."""
    return {
        "id": [2001, 2002, 2003, 2004, 2005],
        "user_id": [101, 101, 102, 102, 103],
        "beatmap_id": [201, 202, 203, 204, 205],
        "score": [1000000, 950000, 980000, 990000, 970000],
        "pp": [500.0, 480.5, 450.0, 600.0, 550.5],  # Some exact, some fuzzy matches
        "accuracy": [99.0, 98.5, 97.0, 99.5, 98.0],
        "ruleset_id": [0, 0, 0, 0, 0],
        "data": [
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "HR"}]}',
            '{"mods": []}',
            '{"mods": [{"acronym": "DT"}, {"acronym": "HR"}]}',
            '{"mods": [{"acronym": "HD"}]}',
        ],
        "mods_key": ["DT", "HR", "", "DT,HR", "HD"],
        "speed_mod": ["DT", None, None, "DT", None],
    }


@pytest.fixture
def temp_parquet_file(tmp_path):
    """Create a temporary Parquet file from data."""

    def _create(data, filename="test.parquet"):
        table = pa.Table.from_pydict(data)
        output_path = tmp_path / filename
        pq.write_table(table, output_path)
        return output_path

    return _create


@pytest.fixture
def temp_warehouse_dir(tmp_path):
    """Create a temporary warehouse directory."""
    return tmp_path / "warehouse"


@pytest.fixture
def temp_parquet_dir(tmp_path):
    """Create a temporary parquet directory with sample files."""
    return tmp_path / "parquet_input"


# =============================================================================
# Table Creation Tests
# =============================================================================


def test_create_mart_score_id_resolution(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Should create mart_score_id_resolution table with correct columns."""
    from duckdb_pipeline import DuckDBPipeline

    # Create parquet files
    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()

    # Load both tables
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))

    # Create ID resolution table
    pipeline.create_mart_score_id_resolution()

    # Verify table exists
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM mart_score_id_resolution")
    assert result is not None


def test_mart_score_id_resolution_columns(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """mart_score_id_resolution should have all required columns."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    result = pipeline.execute("PRAGMA table_info('mart_score_id_resolution')")
    columns = [row[1] for row in result]

    expected_columns = ["legacy_id", "new_id", "match_type", "confidence"]
    for col in expected_columns:
        assert col in columns, f"Column {col} not found in mart_score_id_resolution"


def test_mart_score_id_resolution_data_types(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Columns should have correct data types."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Check data types
    result = pipeline.execute(
        "SELECT legacy_id, new_id, match_type, confidence FROM mart_score_id_resolution LIMIT 1"
    )
    assert len(result) > 0

    row = result[0]
    legacy_id, new_id, match_type, confidence = row

    assert isinstance(legacy_id, int)
    assert isinstance(new_id, int)
    assert isinstance(match_type, str)
    assert isinstance(confidence, float)


# =============================================================================
# Match Type Tests
# =============================================================================


def test_mart_score_id_resolution_exact_matches(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Should classify exact pp matches as 'exact'."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Check that rows with exact pp match have match_type='exact'
    result = pipeline.execute(
        """
        SELECT match_type FROM mart_score_id_resolution 
        WHERE legacy_id = 1001  -- pp=500.0 matches new score pp=500.0
        """
    )
    assert len(result) == 1
    assert result[0][0] == "exact"


def test_mart_score_id_resolution_fuzzy_matches(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Should classify non-exact pp matches as 'fuzzy'."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Check that rows with different pp have match_type='fuzzy'
    result = pipeline.execute(
        """
        SELECT match_type FROM mart_score_id_resolution 
        WHERE legacy_id = 1002  -- pp=480.0 vs new score pp=480.5 (fuzzy)
        """
    )
    assert len(result) == 1
    assert result[0][0] == "fuzzy"


def test_mart_score_id_resolution_confidence_calculation(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Confidence should be calculated as 1.0 / (1.0 + abs(pp_diff))."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Check exact match confidence = 1.0
    result = pipeline.execute(
        """
        SELECT confidence FROM mart_score_id_resolution 
        WHERE legacy_id = 1001  -- exact match, pp_diff = 0
        """
    )
    assert len(result) == 1
    assert result[0][0] == 1.0

    # Check fuzzy match confidence < 1.0
    result = pipeline.execute(
        """
        SELECT confidence FROM mart_score_id_resolution 
        WHERE legacy_id = 1002  -- fuzzy match, pp_diff = 0.5
        """
    )
    assert len(result) == 1
    # confidence = 1.0 / (1.0 + 0.5) = 0.666...
    assert 0.6 < result[0][0] < 0.7


# =============================================================================
# Join Logic Tests
# =============================================================================


def test_mart_score_id_resolution_join_on_user_beatmap_mods(
    temp_warehouse_dir, temp_parquet_file
):
    """Should match on user_id, beatmap_id, mods_key."""
    from duckdb_pipeline import DuckDBPipeline

    # Create data where user/beatmap/mods match but scores differ
    legacy_data = {
        "score_id": [1001],
        "user_id": [101],
        "beatmap_id": [201],
        "mods_key": ["DT"],
        "pp": [500.0],
    }
    new_data = {
        "id": [2001],
        "user_id": [101],
        "beatmap_id": [201],
        "score": [1000000],
        "pp": [500.0],
        "accuracy": [99.0],
        "ruleset_id": [0],
        "data": ['{"mods": [{"acronym": "DT"}]}'],
        "mods_key": ["DT"],
        "speed_mod": ["DT"],
    }

    high_scores_path = temp_parquet_file(legacy_data, "osu_scores_high.parquet")
    new_scores_path = temp_parquet_file(new_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    result = pipeline.execute("SELECT * FROM mart_score_id_resolution")
    assert len(result) == 1
    assert result[0][0] == 1001  # legacy_id
    assert result[0][1] == 2001  # new_id


# =============================================================================
# Index Tests
# =============================================================================


def test_mart_score_id_resolution_indexes(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Should create indexes for legacy_id and new_id columns."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Verify indexes exist
    result = pipeline.execute("SELECT index_name FROM duckdb_indexes()")
    index_names = [row[0] for row in result]

    assert "idx_mart_score_id_resolution_legacy_id" in index_names
    assert "idx_mart_score_id_resolution_new_id" in index_names


# =============================================================================
# Statistics Logging Tests
# =============================================================================


def test_mart_score_id_resolution_statistics(
    temp_warehouse_dir,
    temp_parquet_file,
    legacy_scores_high_data,
    new_scores_data,
    caplog,
):
    """Should log statistics after table creation."""
    import logging

    from duckdb_pipeline import DuckDBPipeline

    caplog.set_level(logging.INFO)

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Check that stats are logged
    assert "mart_score_id_resolution" in caplog.text
    assert "exact" in caplog.text.lower() or "fuzzy" in caplog.text.lower()


# =============================================================================
# Edge Case Tests
# =============================================================================


def test_mart_score_id_resolution_no_unmatched_ids(
    temp_warehouse_dir, temp_parquet_file
):
    """Should not include unmatched legacy IDs in the table."""
    from duckdb_pipeline import DuckDBPipeline

    # Create data where one legacy score has no match
    legacy_data = {
        "score_id": [1001, 1002],
        "user_id": [101, 999],  # 999 has no matching new score
        "beatmap_id": [201, 999],
        "mods_key": ["DT", ""],
        "pp": [500.0, 450.0],
    }
    new_data = {
        "id": [2001],
        "user_id": [101],
        "beatmap_id": [201],
        "score": [1000000],
        "pp": [500.0],
        "accuracy": [99.0],
        "ruleset_id": [0],
        "data": ['{"mods": [{"acronym": "DT"}]}'],
        "mods_key": ["DT"],
        "speed_mod": ["DT"],
    }

    high_scores_path = temp_parquet_file(legacy_data, "osu_scores_high.parquet")
    new_scores_path = temp_parquet_file(new_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))
    pipeline.create_mart_score_id_resolution()

    # Should only have 1 row (the matched one)
    result = pipeline.execute("SELECT COUNT(*) FROM mart_score_id_resolution")
    assert result[0][0] == 1


# =============================================================================
# Integration Test
# =============================================================================


def test_full_id_resolution_pipeline(
    temp_warehouse_dir, temp_parquet_file, legacy_scores_high_data, new_scores_data
):
    """Full ID resolution pipeline: load raw tables → create resolution table → verify."""
    from duckdb_pipeline import DuckDBPipeline

    high_scores_path = temp_parquet_file(
        legacy_scores_high_data, "osu_scores_high.parquet"
    )
    new_scores_path = temp_parquet_file(new_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()

    # Load raw tables
    pipeline.load_parquet_to_raw(
        table_name="osu_scores_high", parquet_path=str(high_scores_path)
    )
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(new_scores_path))

    # Create ID resolution table
    pipeline.create_mart_score_id_resolution()

    # Verify table exists and has data
    result = pipeline.execute("SELECT COUNT(*) FROM mart_score_id_resolution")
    assert result[0][0] > 0

    # Verify lookup works
    result = pipeline.execute(
        "SELECT new_id FROM mart_score_id_resolution WHERE legacy_id = 1001"
    )
    assert len(result) == 1
    assert result[0][0] == 2001

    # Verify match types
    result = pipeline.execute(
        """
        SELECT match_type, COUNT(*) 
        FROM mart_score_id_resolution 
        GROUP BY match_type
        """
    )
    match_types = {row[0]: row[1] for row in result}
    assert "exact" in match_types or "fuzzy" in match_types

    # Verify indexes exist
    result = pipeline.execute("SELECT index_name FROM duckdb_indexes()")
    index_names = [row[0] for row in result]
    assert "idx_mart_score_id_resolution_legacy_id" in index_names
    assert "idx_mart_score_id_resolution_new_id" in index_names
