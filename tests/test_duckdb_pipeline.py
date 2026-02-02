"""
Tests for DuckDB pipeline.
TDD approach - write failing tests first (RED phase).

This module tests the DuckDB pipeline for:
- Database creation
- Parquet loading with CTAS
- Staging tables (stg_scores)
- Mart tables (mart_best_scores, mart_user_topk, mart_beatmap_user_sets)
- Index creation for performance
"""

import json
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_scores_data():
    """Sample scores data for testing."""
    return {
        "id": [1, 2, 3, 4, 5, 6],
        "user_id": [101, 101, 102, 102, 103, 103],
        "beatmap_id": [201, 201, 202, 202, 203, 203],
        "score": [1000000, 950000, 980000, 920000, 990000, 970000],
        "pp": [500.0, 480.0, 450.0, 420.0, 600.0, 550.0],
        "playmode": [0, 0, 0, 0, 0, 0],
        "data": [
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "HR"}]}',
            '{"mods": []}',
            None,
            '{"mods": [{"acronym": "DT"}, {"acronym": "HR"}]}',
            '{"mods": [{"acronym": "DT"}]}',
        ],
        "mods_key": ["DT", "HR", "", "", "DT,HR", "DT"],
        "speed_mod": ["DT", None, None, None, "DT", "DT"],
    }


@pytest.fixture
def sample_scores_with_different_playmodes():
    """Sample scores with different playmodes for testing filtering."""
    return {
        "id": [1, 2, 3, 4],
        "user_id": [101, 101, 102, 102],
        "beatmap_id": [201, 201, 202, 202],
        "score": [1000000, 950000, 980000, 920000],
        "pp": [500.0, 480.0, 450.0, 420.0],
        "playmode": [0, 1, 0, 2],  # Mixed playmodes
        "data": [
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "HR"}]}',
            '{"mods": []}',
            None,
        ],
        "mods_key": ["DT", "HR", "", ""],
        "speed_mod": ["DT", None, None, None],
    }


@pytest.fixture
def duplicate_scores_data():
    """Scores with duplicates for testing deduplication."""
    return {
        "id": [1, 2, 3, 4, 5],
        "user_id": [101, 101, 101, 102, 102],
        "beatmap_id": [201, 201, 201, 202, 202],
        "score": [1000000, 950000, 980000, 990000, 920000],
        "pp": [500.0, 480.0, 520.0, 600.0, 550.0],  # Different PP values
        "playmode": [0, 0, 0, 0, 0],
        "data": [
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "HR"}]}',
            '{"mods": [{"acronym": "HR"}]}',
        ],
        "mods_key": ["DT", "DT", "DT", "HR", "HR"],
        "speed_mod": ["DT", "DT", "DT", None, None],
    }


@pytest.fixture
def large_user_dataset():
    """Large dataset for testing top-k functionality."""
    n = 250  # More than 100 to test top 100 limit
    return {
        "id": list(range(1, n + 1)),
        "user_id": [101] * n,  # Same user
        "beatmap_id": list(range(300, 300 + n)),
        "score": [1000000 - i * 1000 for i in range(n)],
        "pp": [500.0 - i * 1.5 for i in range(n)],  # Decreasing PP
        "playmode": [0] * n,
        "data": ['{"mods": [{"acronym": "DT"}]}'] * n,
        "mods_key": ["DT"] * n,
        "speed_mod": ["DT"] * n,
    }


@pytest.fixture
def temp_parquet_file(tmp_path):
    """Create a temporary Parquet file from data."""

    def _create(data, filename="test_scores.parquet"):
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
# Database Creation Tests
# =============================================================================


def test_duckdb_pipeline_class_imports():
    """Should be able to import DuckDBPipeline and create_pipeline."""
    from pipelines.duckdb_pipeline import DuckDBPipeline, create_pipeline

    assert DuckDBPipeline is not None
    assert create_pipeline is not None


def test_database_created(temp_warehouse_dir):
    """Should create DuckDB database file at data/warehouse/YYYY-MM/osu.duckdb."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )

    # Initialize should create the database
    pipeline.initialize()

    expected_db_path = temp_warehouse_dir / "osu.duckdb"
    assert expected_db_path.exists(), f"Database file not created at {expected_db_path}"


def test_database_connection(temp_warehouse_dir):
    """Should be able to connect to database and execute queries."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )

    pipeline.initialize()

    # Should be able to execute a simple query
    result = pipeline.execute("SELECT 1 as test")
    assert result is not None


def test_database_preserve_insertion_order(temp_warehouse_dir):
    """Should use preserve_insertion_order=false for performance."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir),
        database_name="osu",
        preserve_insertion_order=False,
    )

    pipeline.initialize()

    # Verify the setting is applied
    result = pipeline.execute(
        "SELECT current_setting('preserve_insertion_order') as setting"
    )
    assert result is not None


# =============================================================================
# Parquet Loading Tests
# =============================================================================


def test_load_parquet_to_raw(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """Should load Parquet file to raw table using CTAS."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    # Create sample parquet file
    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()

    # Load parquet to raw table
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))

    # Verify table was created
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM raw_scores")
    assert result is not None
    assert result[0][0] == 6  # 6 rows


def test_load_parquet_schema_validation(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should validate Parquet schema matches expected DuckDB schema."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()

    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))

    # Verify schema - check columns exist
    result = pipeline.execute("PRAGMA table_info('raw_scores')")
    columns = [row[1] for row in result]

    expected_columns = [
        "id",
        "user_id",
        "beatmap_id",
        "score",
        "pp",
        "playmode",
        "data",
        "mods_key",
        "speed_mod",
    ]
    for col in expected_columns:
        assert col in columns, f"Column {col} not found in raw_scores"


def test_load_parquet_glob_pattern(temp_warehouse_dir, temp_parquet_dir):
    """Should support glob patterns for loading multiple parquet files."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    temp_parquet_dir.mkdir(parents=True, exist_ok=True)

    # Create multiple parquet files
    data1 = {
        "id": [1, 2],
        "user_id": [101, 102],
        "beatmap_id": [201, 202],
        "score": [1000000, 950000],
        "pp": [500.0, 480.0],
        "playmode": [0, 0],
        "data": [None, None],
        "mods_key": ["", ""],
        "speed_mod": [None, None],
    }
    data2 = {
        "id": [3, 4],
        "user_id": [103, 104],
        "beatmap_id": [203, 204],
        "score": [980000, 920000],
        "pp": [450.0, 420.0],
        "playmode": [0, 0],
        "data": [None, None],
        "mods_key": ["", ""],
        "speed_mod": [None, None],
    }

    table1 = pa.Table.from_pydict(data1)
    table2 = pa.Table.from_pydict(data2)
    pq.write_table(table1, temp_parquet_dir / "part-000001.parquet")
    pq.write_table(table2, temp_parquet_dir / "part-000002.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()

    # Load using glob pattern
    pipeline.load_parquet_to_raw(
        table_name="scores", parquet_path=str(temp_parquet_dir / "*.parquet")
    )

    # Verify all rows loaded
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM raw_scores")
    assert result[0][0] == 4  # 2 + 2 rows


# =============================================================================
# Staging Table Tests
# =============================================================================


def test_create_stg_scores(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """Should create stg_scores with mod normalization."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()

    # Verify stg_scores exists
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM stg_scores")
    assert result is not None


def test_stg_scores_playmode_filter(
    temp_warehouse_dir, temp_parquet_file, sample_scores_with_different_playmodes
):
    """Should filter stg_scores to playmode=0 (osu! Standard)."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(
        sample_scores_with_different_playmodes, "scores.parquet"
    )

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()

    # Should only have playmode=0 records (2 out of 4)
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM stg_scores")
    assert result[0][0] == 2

    # Verify all have playmode=0
    result = pipeline.execute("SELECT DISTINCT playmode FROM stg_scores")
    playmodes = [row[0] for row in result]
    assert all(pm == 0 for pm in playmodes)


def test_stg_scores_columns(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """stg_scores should have correct columns."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()

    result = pipeline.execute("PRAGMA table_info('stg_scores')")
    columns = [row[1] for row in result]

    expected_columns = [
        "id",
        "user_id",
        "beatmap_id",
        "score",
        "data",
        "mods_key",
        "speed_mod",
    ]
    for col in expected_columns:
        assert col in columns, f"Column {col} not found in stg_scores"


# =============================================================================
# Mart Table Tests - mart_best_scores
# =============================================================================


def test_create_mart_best_scores(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should create mart_best_scores with deduplication."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()

    # Verify mart_best_scores exists
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM mart_best_scores")
    assert result is not None


def test_mart_best_scores_deduplication(
    temp_warehouse_dir, temp_parquet_file, duplicate_scores_data
):
    """Should keep only best score per (user_id, beatmap_id, mods_key) based on pp."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(duplicate_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()

    # User 101, beatmap 201, mods DT should have only 1 row with pp=520 (highest)
    result = pipeline.execute("""
        SELECT user_id, beatmap_id, mods_key, pp 
        FROM mart_best_scores 
        WHERE user_id = 101 AND beatmap_id = 201 AND mods_key = 'DT'
    """)
    assert len(result) == 1
    assert result[0][3] == 520.0  # Highest PP

    # User 102, beatmap 202, mods HR should have only 1 row with pp=600 (highest)
    result = pipeline.execute("""
        SELECT user_id, beatmap_id, mods_key, pp 
        FROM mart_best_scores 
        WHERE user_id = 102 AND beatmap_id = 202 AND mods_key = 'HR'
    """)
    assert len(result) == 1
    assert result[0][3] == 600.0  # Highest PP


def test_mart_best_scores_no_rn_column(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """mart_best_scores should not have the rn (row_number) column."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()

    result = pipeline.execute("PRAGMA table_info('mart_best_scores')")
    columns = [row[1] for row in result]
    assert "rn" not in columns, "rn column should not exist in mart_best_scores"


# =============================================================================
# Mart Table Tests - mart_user_topk
# =============================================================================


def test_create_mart_user_topk(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should create mart_user_topk with top 100 per user/speed_mod."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_user_topk()

    # Verify mart_user_topk exists
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM mart_user_topk")
    assert result is not None


def test_mart_user_topk_limit_100(
    temp_warehouse_dir, temp_parquet_file, large_user_dataset
):
    """Should limit to top 100 scores per user/speed_mod combination."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(large_user_dataset, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_user_topk()

    # Should have exactly 100 rows for user 101 with speed_mod='DT'
    result = pipeline.execute("""
        SELECT COUNT(*) as cnt 
        FROM mart_user_topk 
        WHERE user_id = 101 AND speed_mod = 'DT'
    """)
    assert result[0][0] == 100


def test_mart_user_topk_ordered_by_pp(
    temp_warehouse_dir, temp_parquet_file, large_user_dataset
):
    """Top 100 should be ordered by pp descending."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(large_user_dataset, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_user_topk()

    # Get top scores for user 101
    result = pipeline.execute("""
        SELECT pp 
        FROM mart_user_topk 
        WHERE user_id = 101 AND speed_mod = 'DT'
        ORDER BY pp DESC
    """)

    pp_values = [row[0] for row in result]
    # Should be in descending order
    for i in range(len(pp_values) - 1):
        assert pp_values[i] >= pp_values[i + 1], (
            "PP values should be in descending order"
        )

    # Highest PP should be close to 500
    assert pp_values[0] > 490


def test_mart_user_topk_no_rn_column(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """mart_user_topk should not have the rn (row_number) column."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_user_topk()

    result = pipeline.execute("PRAGMA table_info('mart_user_topk')")
    columns = [row[1] for row in result]
    assert "rn" not in columns, "rn column should not exist in mart_user_topk"


# =============================================================================
# Mart Table Tests - mart_beatmap_user_sets
# =============================================================================


def test_create_mart_beatmap_user_sets(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should create mart_beatmap_user_sets with precomputed stats and user arrays."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_beatmap_user_sets()

    # Verify mart_beatmap_user_sets exists
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM mart_beatmap_user_sets")
    assert result is not None


def test_mart_beatmap_user_sets_columns(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """mart_beatmap_user_sets should have all required columns."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_beatmap_user_sets()

    result = pipeline.execute("PRAGMA table_info('mart_beatmap_user_sets')")
    columns = [row[1] for row in result]

    expected_columns = [
        "beatmap_id",
        "mods_key",
        "user_ids",
        "user_count",
        "avg_pp",
        "std_pp",
        "min_pp",
        "median_pp",
        "p75_pp",
        "p90_pp",
    ]
    for col in expected_columns:
        assert col in columns, f"Column {col} not found in mart_beatmap_user_sets"


def test_mart_beatmap_user_sets_user_arrays(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should create ARRAY_AGG of user_ids."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_beatmap_user_sets()

    # Check that user_ids is an array
    result = pipeline.execute("""
        SELECT beatmap_id, mods_key, user_ids, user_count
        FROM mart_beatmap_user_sets
        WHERE beatmap_id = 201 AND mods_key = 'DT'
    """)

    assert len(result) == 1
    user_ids = result[0][2]
    user_count = result[0][3]

    # user_ids should be a list/array
    assert isinstance(user_ids, (list, tuple)), "user_ids should be an array"
    assert user_count == len(user_ids), "user_count should match array length"


def test_mart_beatmap_user_sets_percentiles(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should calculate median, p75, p90 percentiles."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_beatmap_user_sets()

    # Get stats for a beatmap
    result = pipeline.execute("""
        SELECT avg_pp, median_pp, p75_pp, p90_pp, min_pp
        FROM mart_beatmap_user_sets
        WHERE beatmap_id = 201
    """)

    assert len(result) > 0
    row = result[0]
    avg_pp = row[0]
    median_pp = row[1]
    p75_pp = row[2]
    p90_pp = row[3]
    min_pp = row[4]

    # Percentiles should be ordered: min <= median <= p75 <= p90
    assert min_pp <= median_pp, "min_pp should be <= median_pp"
    assert median_pp <= p75_pp, "median_pp should be <= p75_pp"
    assert p75_pp <= p90_pp, "p75_pp should be <= p90_pp"


def test_mart_beatmap_user_sets_grouping(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should group by beatmap_id and mods_key."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_beatmap_user_sets()

    # Each (beatmap_id, mods_key) combination should be one row
    result = pipeline.execute("""
        SELECT beatmap_id, mods_key, COUNT(*) as cnt
        FROM mart_beatmap_user_sets
        GROUP BY beatmap_id, mods_key
        HAVING COUNT(*) > 1
    """)

    assert len(result) == 0, "Each (beatmap_id, mods_key) should have exactly one row"


# =============================================================================
# Index Creation Tests
# =============================================================================


def test_create_indexes(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """Should create performance indexes."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_indexes()

    # Verify indexes exist
    result = pipeline.execute("SELECT index_name FROM duckdb_indexes()")
    index_names = [row[0] for row in result]

    assert "idx_mart_best_scores_beatmap_lookup" in index_names
    assert "idx_mart_best_scores_user_lookup" in index_names


def test_index_beatmap_lookup(
    temp_warehouse_dir, temp_parquet_file, sample_scores_data
):
    """Should create idx_mart_best_scores_beatmap_lookup with correct columns."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_indexes()

    # Get index info
    result = pipeline.execute("""
        SELECT index_name, sql 
        FROM duckdb_indexes() 
        WHERE index_name = 'idx_mart_best_scores_beatmap_lookup'
    """)

    assert len(result) == 1
    sql = result[0][1]

    # Should include beatmap_id, pp, mods_key, user_id
    assert "beatmap_id" in sql
    assert "pp" in sql
    assert "mods_key" in sql
    assert "user_id" in sql


def test_index_user_lookup(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """Should create idx_mart_best_scores_user_lookup with correct columns."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_indexes()

    # Get index info
    result = pipeline.execute("""
        SELECT index_name, sql 
        FROM duckdb_indexes() 
        WHERE index_name = 'idx_mart_best_scores_user_lookup'
    """)

    assert len(result) == 1
    sql = result[0][1]

    # Should include user_id, beatmap_id, pp
    assert "user_id" in sql
    assert "beatmap_id" in sql
    assert "pp" in sql


# =============================================================================
# Integration Tests
# =============================================================================


def test_full_pipeline(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """Full pipeline: Parquet → raw → stg → mart → indexes."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )

    # Run full pipeline
    pipeline.initialize()
    pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(parquet_path))
    pipeline.create_stg_scores()
    pipeline.create_mart_best_scores()
    pipeline.create_mart_user_topk()
    pipeline.create_mart_beatmap_user_sets()
    pipeline.create_indexes()

    # Verify all tables exist
    tables = pipeline.execute("SHOW TABLES")
    table_names = [row[0] for row in tables]

    assert "raw_scores" in table_names
    assert "stg_scores" in table_names
    assert "mart_best_scores" in table_names
    assert "mart_user_topk" in table_names
    assert "mart_beatmap_user_sets" in table_names

    # Verify indexes exist
    indexes = pipeline.execute("SELECT index_name FROM duckdb_indexes()")
    index_names = [row[0] for row in indexes]
    assert "idx_mart_best_scores_beatmap_lookup" in index_names
    assert "idx_mart_best_scores_user_lookup" in index_names


def test_create_pipeline_convenience_function(temp_warehouse_dir, temp_parquet_dir):
    """Test the create_pipeline convenience function."""
    from pipelines.duckdb_pipeline import create_pipeline

    temp_parquet_dir.mkdir(parents=True, exist_ok=True)

    # Create sample parquet files
    data = {
        "id": [1, 2, 3],
        "user_id": [101, 102, 103],
        "beatmap_id": [201, 202, 203],
        "score": [1000000, 950000, 980000],
        "pp": [500.0, 480.0, 450.0],
        "playmode": [0, 0, 0],
        "data": [None, None, None],
        "mods_key": ["", "", ""],
        "speed_mod": [None, None, None],
    }
    table = pa.Table.from_pydict(data)
    pq.write_table(table, temp_parquet_dir / "scores.parquet")

    # Run convenience function
    manifest = create_pipeline(
        parquet_dir=str(temp_parquet_dir), warehouse_dir=str(temp_warehouse_dir)
    )

    # Should return manifest
    assert manifest is not None
    assert "tables" in manifest or "database_path" in manifest


# =============================================================================
# Error Handling Tests
# =============================================================================


def test_pipeline_handles_missing_parquet(temp_warehouse_dir):
    """Should handle missing parquet files gracefully."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )
    pipeline.initialize()

    # Should raise an error for non-existent parquet
    with pytest.raises(Exception):
        pipeline.load_parquet_to_raw(
            table_name="scores", parquet_path="/nonexistent/path/*.parquet"
        )


def test_pipeline_idempotent(temp_warehouse_dir, temp_parquet_file, sample_scores_data):
    """Pipeline should be idempotent - running twice should work."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    parquet_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    pipeline = DuckDBPipeline(
        warehouse_dir=str(temp_warehouse_dir), database_name="osu"
    )

    # Run twice
    for _ in range(2):
        pipeline.initialize()
        pipeline.load_parquet_to_raw(
            table_name="scores", parquet_path=str(parquet_path)
        )
        pipeline.create_stg_scores()
        pipeline.create_mart_best_scores()

    # Should still work
    result = pipeline.execute("SELECT COUNT(*) as cnt FROM mart_best_scores")
    assert result is not None
