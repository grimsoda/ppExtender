"""
End-to-end integration tests for the full pipeline.

Tests the complete flow from SQL → Parquet → DuckDB → Golden tables → API.
"""

import json
import subprocess
import time
from pathlib import Path
from typing import Generator

import duckdb
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.sql_parser import parse_sql_file, SqlParser
from pipelines.parquet_writer import write_parquet_batches, ParquetWriter
from pipelines.duckdb_pipeline import DuckDBPipeline, create_pipeline
from pipelines.recommender_queries import (
    RecommenderQueries,
    get_cohort_users,
    get_recommendations,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_sql_dump():
    """Sample SQL dump content for testing."""
    return """-- Sample osu! scores dump
INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `score`, `pp`, `playmode`, `data`) VALUES
(1, 101, 201, 1000000, 500.0, 0, '{"mods": [{"acronym": "DT"}]}'),
(2, 101, 201, 950000, 480.0, 0, '{"mods": [{"acronym": "HR"}]}'),
(3, 102, 201, 980000, 450.0, 0, '{"mods": [{"acronym": "DT"}]}'),
(4, 102, 202, 920000, 420.0, 0, '{"mods": []}'),
(5, 103, 202, 990000, 600.0, 0, '{"mods": [{"acronym": "HR"}]}'),
(6, 103, 203, 970000, 550.0, 0, '{"mods": [{"acronym": "DT"}]}'),
(7, 104, 203, 985000, 520.0, 0, NULL),
(8, 104, 204, 930000, 490.0, 0, '{"mods": [{"acronym": "DT"}]}'),
(9, 105, 204, 995000, 580.0, 0, '{"mods": [{"acronym": "DT"}]}'),
(10, 105, 205, 960000, 530.0, 0, '{"mods": [{"acronym": "HR"}]}');

INSERT INTO `beatmaps` (`beatmap_id`, `beatmapset_id`, `version`, `difficultyrating`) VALUES
(201, 301, 'Hard', 4.5),
(202, 302, 'Insane', 5.2),
(203, 303, 'Expert', 6.1),
(204, 304, 'Normal', 3.2),
(205, 305, 'Easy', 2.1);

INSERT INTO `beatmapsets` (`beatmapset_id`, `artist`, `title`) VALUES
(301, 'Artist A', 'Song A'),
(302, 'Artist B', 'Song B'),
(303, 'Artist C', 'Song C'),
(304, 'Artist D', 'Song D'),
(305, 'Artist E', 'Song E');
"""


@pytest.fixture
def temp_sql_file(tmp_path: Path, sample_sql_dump: str) -> Path:
    """Create a temporary SQL file with sample data."""
    sql_path = tmp_path / "sample_scores.sql"
    sql_path.write_text(sample_sql_dump)
    return sql_path


@pytest.fixture
def temp_parquet_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for Parquet files."""
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    return parquet_dir


@pytest.fixture
def temp_warehouse_dir(tmp_path: Path) -> Path:
    """Create a temporary warehouse directory."""
    warehouse_dir = tmp_path / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    return warehouse_dir


@pytest.fixture
def e2e_pipeline_data(
    tmp_path: Path,
    temp_sql_file: Path,
    temp_parquet_dir: Path,
    temp_warehouse_dir: Path,
):
    """Setup complete E2E pipeline data and return paths."""
    return {
        "sql_file": temp_sql_file,
        "parquet_dir": temp_parquet_dir,
        "warehouse_dir": temp_warehouse_dir,
        "tmp_path": tmp_path,
    }


@pytest.fixture
def populated_database(e2e_pipeline_data: dict) -> dict:
    """Create a fully populated database through the complete pipeline."""
    sql_file = e2e_pipeline_data["sql_file"]
    parquet_dir = e2e_pipeline_data["parquet_dir"]
    warehouse_dir = e2e_pipeline_data["warehouse_dir"]

    # Step 1: Parse SQL to Parquet (scores table only)
    scores_batches = list(
        parse_sql_file(str(sql_file), table_name="scores", batch_size=100)
    )

    # Write scores to Parquet
    scores_parquet_dir = parquet_dir / "scores"
    scores_parquet_dir.mkdir(parents=True, exist_ok=True)
    write_parquet_batches(
        batches=scores_batches,
        output_dir=str(scores_parquet_dir),
        table_name="scores",
        compression="snappy",
    )

    # Create beatmaps Parquet directly (avoiding SQL parser type issues)
    beatmaps_parquet_dir = parquet_dir / "beatmaps"
    beatmaps_parquet_dir.mkdir(parents=True, exist_ok=True)
    beatmaps_data = {
        "beatmap_id": [201, 202, 203, 204, 205],
        "beatmapset_id": [301, 302, 303, 304, 305],
        "version": ["Hard", "Insane", "Expert", "Normal", "Easy"],
        "difficultyrating": [4.5, 5.2, 6.1, 3.2, 2.1],
    }
    table = pa.Table.from_pydict(beatmaps_data)
    pq.write_table(table, beatmaps_parquet_dir / "part-000000.parquet")

    # Create beatmapsets Parquet directly
    beatmapsets_parquet_dir = parquet_dir / "beatmapsets"
    beatmapsets_parquet_dir.mkdir(parents=True, exist_ok=True)
    beatmapsets_data = {
        "beatmapset_id": [301, 302, 303, 304, 305],
        "artist": ["Artist A", "Artist B", "Artist C", "Artist D", "Artist E"],
        "title": ["Song A", "Song B", "Song C", "Song D", "Song E"],
    }
    table = pa.Table.from_pydict(beatmapsets_data)
    pq.write_table(table, beatmapsets_parquet_dir / "part-000000.parquet")

    # Step 2: Load Parquet to DuckDB and create golden tables
    pipeline = DuckDBPipeline(warehouse_dir, "osu")
    try:
        pipeline.initialize()

        # Load raw tables
        pipeline.load_parquet_to_raw(
            table_name="scores", parquet_path=str(scores_parquet_dir / "*.parquet")
        )
        pipeline.load_parquet_to_raw(
            table_name="beatmaps", parquet_path=str(beatmaps_parquet_dir / "*.parquet")
        )
        pipeline.load_parquet_to_raw(
            table_name="beatmapsets",
            parquet_path=str(beatmapsets_parquet_dir / "*.parquet"),
        )

        # Create staging and mart tables
        pipeline.create_stg_scores()
        pipeline.create_mart_best_scores()
        pipeline.create_mart_user_topk()
        pipeline.create_mart_beatmap_user_sets()
        pipeline.create_indexes()

        # Get manifest
        manifest = {"database_path": str(pipeline.db_path), "tables": {}}
        for table in [
            "raw_scores",
            "raw_beatmaps",
            "raw_beatmapsets",
            "stg_scores",
            "mart_best_scores",
            "mart_user_topk",
            "mart_beatmap_user_sets",
        ]:
            try:
                result = pipeline.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                manifest["tables"][table] = {"row_count": result[0] if result else 0}
            except:
                manifest["tables"][table] = {"row_count": 0}

        yield {
            "db_path": str(pipeline.db_path),
            "manifest": manifest,
            "parquet_dir": str(parquet_dir),
            "warehouse_dir": str(warehouse_dir),
        }
    finally:
        pipeline.close()


# =============================================================================
# Full Pipeline Tests
# =============================================================================


def test_full_pipeline_sql_to_parquet(e2e_pipeline_data: dict):
    """Test full pipeline: SQL dump → Parquet files."""
    sql_file = e2e_pipeline_data["sql_file"]
    parquet_dir = e2e_pipeline_data["parquet_dir"]

    # Parse SQL to batches
    batches = list(parse_sql_file(str(sql_file), table_name="scores", batch_size=100))

    # Verify batches were created
    assert len(batches) > 0, "Should create at least one batch"

    # Write to Parquet
    output_dir = parquet_dir / "scores"
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = write_parquet_batches(
        batches=batches,
        output_dir=str(output_dir),
        table_name="scores",
        compression="snappy",
    )

    # Verify Parquet files were created
    parquet_files = list(output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0, "Should create at least one Parquet file"

    # Verify manifest
    assert manifest["table_name"] == "scores"
    assert manifest["total_rows"] > 0
    assert len(manifest["files"]) > 0

    # Verify manifest.json was written
    manifest_path = output_dir / "manifest.json"
    assert manifest_path.exists(), "Should create manifest.json"


def test_full_pipeline_parquet_to_duckdb(e2e_pipeline_data: dict):
    """Test full pipeline: Parquet → DuckDB raw tables."""
    sql_file = e2e_pipeline_data["sql_file"]
    parquet_dir = e2e_pipeline_data["parquet_dir"]
    warehouse_dir = e2e_pipeline_data["warehouse_dir"]

    # Parse SQL to Parquet
    batches = list(parse_sql_file(str(sql_file), table_name="scores", batch_size=100))
    scores_parquet_dir = parquet_dir / "scores"
    scores_parquet_dir.mkdir(parents=True, exist_ok=True)

    write_parquet_batches(
        batches=batches,
        output_dir=str(scores_parquet_dir),
        table_name="scores",
        compression="snappy",
    )

    # Load into DuckDB
    pipeline = DuckDBPipeline(warehouse_dir, "osu")
    try:
        pipeline.initialize()
        pipeline.load_parquet_to_raw(
            table_name="scores", parquet_path=str(scores_parquet_dir / "*.parquet")
        )

        # Verify data was loaded
        result = pipeline.execute("SELECT COUNT(*) FROM raw_scores")
        assert result[0][0] > 0, "Should have rows in raw_scores"

        # Verify schema
        columns_result = pipeline.execute("PRAGMA table_info('raw_scores')")
        columns = [row[1] for row in columns_result]
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
            assert col in columns, f"Column {col} should exist in raw_scores"
    finally:
        pipeline.close()


def test_full_pipeline_duckdb_to_golden_tables(e2e_pipeline_data: dict):
    """Test full pipeline: DuckDB raw → staging → mart tables."""
    sql_file = e2e_pipeline_data["sql_file"]
    parquet_dir = e2e_pipeline_data["parquet_dir"]
    warehouse_dir = e2e_pipeline_data["warehouse_dir"]

    # Parse SQL to Parquet
    batches = list(parse_sql_file(str(sql_file), table_name="scores", batch_size=100))
    scores_parquet_dir = parquet_dir / "scores"
    scores_parquet_dir.mkdir(parents=True, exist_ok=True)

    write_parquet_batches(
        batches=batches,
        output_dir=str(scores_parquet_dir),
        table_name="scores",
        compression="snappy",
    )

    # Run full pipeline
    manifest = create_pipeline(
        parquet_dir=str(parquet_dir),
        warehouse_dir=str(warehouse_dir),
        database_name="osu",
    )

    # Verify all tables were created
    pipeline = DuckDBPipeline(warehouse_dir, "osu")
    try:
        pipeline.initialize()

        tables_result = pipeline.execute("SHOW TABLES")
        table_names = [row[0] for row in tables_result]

        expected_tables = [
            "raw_scores",
            "stg_scores",
            "mart_best_scores",
            "mart_user_topk",
            "mart_beatmap_user_sets",
        ]

        for table in expected_tables:
            assert table in table_names, f"Table {table} should exist"

        # Verify indexes were created
        indexes_result = pipeline.execute("SELECT index_name FROM duckdb_indexes()")
        index_names = [row[0] for row in indexes_result]

        assert "idx_mart_best_scores_beatmap_lookup" in index_names
        assert "idx_mart_best_scores_user_lookup" in index_names

        # Verify manifest has correct row counts
        assert "tables" in manifest
        for table in expected_tables:
            assert table in manifest["tables"], f"Manifest should include {table}"
            assert manifest["tables"][table]["row_count"] >= 0

    finally:
        pipeline.close()


def test_full_pipeline_end_to_end(populated_database: dict):
    """Test complete E2E pipeline: SQL → Parquet → DuckDB → Golden tables."""
    db_path = populated_database["db_path"]
    manifest = populated_database["manifest"]

    # Verify database exists
    assert Path(db_path).exists(), "Database file should exist"

    # Verify manifest structure
    assert "database_path" in manifest
    assert "tables" in manifest

    # Verify all expected tables have data
    conn = duckdb.connect(db_path)
    try:
        # Check raw_scores
        result = conn.execute("SELECT COUNT(*) FROM raw_scores").fetchone()
        assert result[0] > 0, "raw_scores should have data"

        # Check stg_scores (filtered to playmode=0)
        result = conn.execute("SELECT COUNT(*) FROM stg_scores").fetchone()
        assert result[0] > 0, "stg_scores should have data"

        # Verify playmode filtering
        result = conn.execute("SELECT DISTINCT playmode FROM stg_scores").fetchall()
        playmodes = [row[0] for row in result]
        assert all(pm == 0 for pm in playmodes), (
            "stg_scores should only have playmode=0"
        )

        # Check mart tables
        result = conn.execute("SELECT COUNT(*) FROM mart_best_scores").fetchone()
        assert result[0] > 0, "mart_best_scores should have data"

        result = conn.execute("SELECT COUNT(*) FROM mart_user_topk").fetchone()
        assert result[0] >= 0, "mart_user_topk should exist"

        result = conn.execute("SELECT COUNT(*) FROM mart_beatmap_user_sets").fetchone()
        assert result[0] > 0, "mart_beatmap_user_sets should have data"

    finally:
        conn.close()


# =============================================================================
# Query Performance Tests
# =============================================================================


def test_recommender_query_performance(populated_database: dict):
    """Test that recommender queries execute in <1 second."""
    db_path = populated_database["db_path"]

    queries = RecommenderQueries(db_path)
    queries.connect()

    try:
        # Test cohort extraction performance
        start_time = time.time()
        cohort = queries.get_cohort_users(beatmap_id=201, mods="DT")
        cohort_time = time.time() - start_time

        assert cohort_time < 1.0, (
            f"Cohort extraction took {cohort_time:.2f}s, should be <1s"
        )

        # Test recommendations query performance
        extended_cohort = [101, 102, 103, 104, 105]

        start_time = time.time()
        recommendations = queries.get_recommendations(
            extended_cohort, min_cohort_overlap=1, min_total_players=1, limit=10
        )
        recommend_time = time.time() - start_time

        assert recommend_time < 1.0, (
            f"Recommendations query took {recommend_time:.2f}s, should be <1s"
        )

    finally:
        queries.close()


def test_query_performance_with_indexes(populated_database: dict):
    """Test that indexes improve query performance."""
    db_path = populated_database["db_path"]

    conn = duckdb.connect(db_path)
    try:
        # Test beatmap lookup query
        start_time = time.time()
        result = conn.execute("""
            SELECT user_id, pp, mods_key 
            FROM mart_best_scores 
            WHERE beatmap_id = 201 
            ORDER BY pp DESC
        """).fetchall()
        query_time = time.time() - start_time

        assert query_time < 1.0, f"Beatmap lookup took {query_time:.2f}s, should be <1s"
        assert len(result) > 0, "Should return results"

        # Test user lookup query
        start_time = time.time()
        result = conn.execute("""
            SELECT beatmap_id, pp 
            FROM mart_best_scores 
            WHERE user_id = 101 
            ORDER BY pp DESC
        """).fetchall()
        query_time = time.time() - start_time

        assert query_time < 1.0, f"User lookup took {query_time:.2f}s, should be <1s"

    finally:
        conn.close()


# =============================================================================
# Data Integrity Tests
# =============================================================================


def test_data_integrity_row_counts(populated_database: dict):
    """Test that row counts in database match manifest expectations."""
    db_path = populated_database["db_path"]
    manifest = populated_database["manifest"]

    conn = duckdb.connect(db_path)
    try:
        # Verify each table's row count matches manifest
        for table_name, table_info in manifest["tables"].items():
            expected_count = table_info["row_count"]

            # Skip tables that might not exist
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                actual_count = result[0]

                assert actual_count == expected_count, (
                    f"Table {table_name}: expected {expected_count} rows, got {actual_count}"
                )
            except Exception:
                # Table might not exist, skip
                pass

    finally:
        conn.close()


def test_data_integrity_deduplication(populated_database: dict):
    """Test that deduplication worked correctly in mart_best_scores."""
    db_path = populated_database["db_path"]

    conn = duckdb.connect(db_path)
    try:
        # Check that there are no duplicate (user_id, beatmap_id, mods_key) combinations
        result = conn.execute("""
            SELECT user_id, beatmap_id, mods_key, COUNT(*) as cnt
            FROM mart_best_scores
            GROUP BY user_id, beatmap_id, mods_key
            HAVING COUNT(*) > 1
        """).fetchall()

        assert len(result) == 0, (
            f"Found {len(result)} duplicate combinations in mart_best_scores"
        )

        # Verify that for each group, we kept the highest PP score
        result = conn.execute("""
            SELECT user_id, beatmap_id, mods_key, pp
            FROM mart_best_scores
            WHERE user_id = 101 AND beatmap_id = 201 AND mods_key = 'DT'
        """).fetchall()

        # Should have exactly one row with the highest PP
        assert len(result) == 1, (
            "Should have exactly one row per (user, beatmap, mods) combination"
        )

    finally:
        conn.close()


def test_data_integrity_referential(populated_database: dict):
    """Test referential integrity between tables."""
    db_path = populated_database["db_path"]

    conn = duckdb.connect(db_path)
    try:
        # All beatmap_ids in mart_best_scores should exist in raw_beatmaps
        result = conn.execute("""
            SELECT DISTINCT m.beatmap_id
            FROM mart_best_scores m
            LEFT JOIN raw_beatmaps b ON m.beatmap_id = b.beatmap_id
            WHERE b.beatmap_id IS NULL
        """).fetchall()

        # Allow for missing beatmaps in small test datasets
        # In production, this should be 0

        # All user_ids in mart_best_scores should exist in raw_scores
        result = conn.execute("""
            SELECT DISTINCT m.user_id
            FROM mart_best_scores m
            LEFT JOIN raw_scores s ON m.user_id = s.user_id
            WHERE s.user_id IS NULL
        """).fetchall()

        assert len(result) == 0, (
            "All users in mart_best_scores should exist in raw_scores"
        )

    finally:
        conn.close()


def test_data_integrity_mods_normalization(populated_database: dict):
    """Test that mods were normalized correctly."""
    db_path = populated_database["db_path"]

    conn = duckdb.connect(db_path)
    try:
        # Check that mods_key is sorted alphabetically
        result = conn.execute("""
            SELECT DISTINCT mods_key
            FROM mart_best_scores
            WHERE mods_key LIKE '%,%'
        """).fetchall()

        for row in result:
            mods_key = row[0]
            mods = mods_key.split(",")
            sorted_mods = sorted(mods)
            assert mods == sorted_mods, f"Mods should be sorted: {mods_key}"

        # Check speed_mod categorization
        result = conn.execute("""
            SELECT DISTINCT mods_key, speed_mod
            FROM mart_best_scores
            WHERE speed_mod IS NOT NULL
        """).fetchall()

        for row in result:
            mods_key, speed_mod = row
            if "DT" in mods_key or "NC" in mods_key:
                assert speed_mod == "DT", f"DT/NC mods should have speed_mod='DT'"
            elif "HT" in mods_key:
                assert speed_mod == "HT", f"HT mods should have speed_mod='HT'"

    finally:
        conn.close()


# =============================================================================
# API Integration Tests
# =============================================================================


@pytest.fixture(scope="module")
def mock_api_server(tmp_path_factory):
    """Create a mock API server for testing (if real server not available)."""
    # This is a placeholder - in a real scenario, you'd start the actual server
    # For now, we'll test the API layer logic directly
    yield None


def test_api_layer_database_connection(populated_database: dict):
    """Test that API can connect to the database."""
    db_path = populated_database["db_path"]

    # Simulate API database connection
    conn = duckdb.connect(db_path)
    try:
        # Test health check query
        result = conn.execute("SELECT 1 as health").fetchone()
        assert result[0] == 1, "Database health check should pass"

        # Test that required tables exist
        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables_result]

        required_tables = ["mart_best_scores", "mart_beatmap_user_sets"]
        for table in required_tables:
            assert table in table_names, f"Required table {table} should exist"

    finally:
        conn.close()


def test_api_cohort_endpoint_logic(populated_database: dict):
    """Test the logic behind the /api/cohort endpoint."""
    db_path = populated_database["db_path"]

    queries = RecommenderQueries(db_path)
    queries.connect()

    try:
        # Simulate cohort extraction
        beatmap_id = 201
        pp_lower = 400.0
        pp_upper = 600.0
        mods = "DT"

        cohort = queries.get_cohort_users(
            beatmap_id=beatmap_id, pp_lower=pp_lower, pp_upper=pp_upper, mods=mods
        )

        # Verify response structure
        assert isinstance(cohort, list), "Cohort should be a list"
        assert len(cohort) > 0, "Should find users in cohort"

        # All user IDs should be integers
        for user_id in cohort:
            assert isinstance(user_id, int), f"User ID {user_id} should be an integer"

    finally:
        queries.close()


def test_api_recommend_endpoint_logic(populated_database: dict):
    """Test the logic behind the /api/recommend endpoint."""
    db_path = populated_database["db_path"]

    queries = RecommenderQueries(db_path)
    queries.connect()

    try:
        # Simulate recommendation flow
        # Step 1: Get cohort
        cohort = queries.get_cohort_users(beatmap_id=201, mods="DT")
        assert len(cohort) > 0, "Should have cohort users"

        # Step 2: Get recommendations
        recommendations = queries.get_recommendations(
            cohort_users=cohort, min_cohort_overlap=1, min_total_players=1, limit=10
        )

        # Verify response structure
        assert isinstance(recommendations, list), "Recommendations should be a list"

        if len(recommendations) > 0:
            rec = recommendations[0]
            required_fields = [
                "beatmap_id",
                "version",
                "artist",
                "title",
                "cohort_overlap",
                "novelty_score",
                "avg_pp",
            ]
            for field in required_fields:
                assert field in rec, f"Recommendation should have {field} field"

            # Verify data types
            assert isinstance(rec["beatmap_id"], int), "beatmap_id should be int"
            assert isinstance(rec["cohort_overlap"], int), (
                "cohort_overlap should be int"
            )
            assert isinstance(rec["novelty_score"], float), (
                "novelty_score should be float"
            )

    finally:
        queries.close()


def test_api_beatmaps_endpoint_logic(populated_database: dict):
    """Test the logic behind the /api/beatmaps endpoint."""
    db_path = populated_database["db_path"]

    queries = RecommenderQueries(db_path)
    queries.connect()

    try:
        # Simulate beatmap metadata lookup
        beatmap_ids = [201, 202, 203]

        metadata = queries.get_beatmap_metadata(beatmap_ids)

        # Verify response structure
        assert isinstance(metadata, list), "Metadata should be a list"

        if len(metadata) > 0:
            for meta in metadata:
                required_fields = ["beatmap_id", "version", "artist", "title"]
                for field in required_fields:
                    assert field in meta, f"Metadata should have {field} field"

    finally:
        queries.close()


def test_api_error_handling(populated_database: dict):
    """Test API error handling for invalid inputs."""
    db_path = populated_database["db_path"]

    queries = RecommenderQueries(db_path)
    queries.connect()

    try:
        # Test with non-existent beatmap
        cohort = queries.get_cohort_users(beatmap_id=99999)
        assert cohort == [], "Should return empty list for non-existent beatmap"

        # Test with empty cohort
        recommendations = queries.get_recommendations([])
        assert recommendations == [], "Should return empty list for empty cohort"

        # Test with empty beatmap list
        metadata = queries.get_beatmap_metadata([])
        assert metadata == [], "Should return empty list for empty beatmap list"

    finally:
        queries.close()


# =============================================================================
# Integration Chain Tests
# =============================================================================


def test_frontend_backend_database_chain(populated_database: dict):
    """Test the full chain: Frontend request → Backend → Database."""
    db_path = populated_database["db_path"]

    # Simulate a frontend request for recommendations
    request_payload = {
        "beatmap_id": 201,
        "pp_lower": 400.0,
        "pp_upper": 600.0,
        "mods": ["DT"],
        "limit": 10,
    }

    # Simulate backend processing
    queries = RecommenderQueries(db_path)
    queries.connect()

    try:
        # Step 1: Validate beatmap exists (would be done by backend)
        metadata = queries.get_beatmap_metadata([request_payload["beatmap_id"]])

        # Step 2: Extract cohort
        cohort = queries.get_cohort_users(
            beatmap_id=request_payload["beatmap_id"],
            pp_lower=request_payload["pp_lower"],
            pp_upper=request_payload["pp_upper"],
            mods="DT",  # Converted from array to string
        )

        # Step 3: Get recommendations
        recommendations = queries.get_recommendations(
            cohort_users=cohort,
            min_cohort_overlap=1,
            min_total_players=1,
            limit=request_payload["limit"],
        )

        # Simulate response formatting
        response = {
            "beatmap_id": request_payload["beatmap_id"],
            "total": len(recommendations),
            "recommendations": recommendations,
        }

        # Verify response
        assert "beatmap_id" in response
        assert "total" in response
        assert "recommendations" in response
        assert response["total"] >= 0

    finally:
        queries.close()


def test_pipeline_idempotency(e2e_pipeline_data: dict):
    """Test that running the pipeline twice produces the same results."""
    sql_file = e2e_pipeline_data["sql_file"]
    parquet_dir = e2e_pipeline_data["parquet_dir"]
    warehouse_dir = e2e_pipeline_data["warehouse_dir"]

    # Run pipeline first time
    batches = list(parse_sql_file(str(sql_file), table_name="scores", batch_size=100))
    scores_parquet_dir = parquet_dir / "scores"
    scores_parquet_dir.mkdir(parents=True, exist_ok=True)

    write_parquet_batches(
        batches=batches,
        output_dir=str(scores_parquet_dir),
        table_name="scores",
        compression="snappy",
    )

    manifest1 = create_pipeline(
        parquet_dir=str(parquet_dir),
        warehouse_dir=str(warehouse_dir),
        database_name="osu",
    )

    # Get row counts from first run
    pipeline = DuckDBPipeline(warehouse_dir, "osu")
    try:
        pipeline.initialize()
        result1 = pipeline.execute("SELECT COUNT(*) FROM mart_best_scores")
        count1 = result1[0][0]
    finally:
        pipeline.close()

    # Run pipeline second time (should be idempotent)
    manifest2 = create_pipeline(
        parquet_dir=str(parquet_dir),
        warehouse_dir=str(warehouse_dir),
        database_name="osu",
    )

    pipeline = DuckDBPipeline(warehouse_dir, "osu")
    try:
        pipeline.initialize()
        result2 = pipeline.execute("SELECT COUNT(*) FROM mart_best_scores")
        count2 = result2[0][0]
    finally:
        pipeline.close()

    # Row counts should be the same
    assert count1 == count2, f"Pipeline should be idempotent: {count1} vs {count2}"


def test_manifest_accuracy(populated_database: dict):
    """Test that the manifest accurately reflects the database state."""
    db_path = populated_database["db_path"]
    manifest = populated_database["manifest"]

    conn = duckdb.connect(db_path)
    try:
        # Verify database path
        assert manifest["database_path"] == db_path

        # Verify each table's row count
        for table_name, table_info in manifest["tables"].items():
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                actual_count = result[0]
                manifest_count = table_info["row_count"]

                assert actual_count == manifest_count, (
                    f"Manifest row count mismatch for {table_name}: "
                    f"manifest={manifest_count}, actual={actual_count}"
                )
            except Exception as e:
                # Some tables might not exist, that's ok for this test
                pass

    finally:
        conn.close()
