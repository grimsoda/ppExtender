"""Tests for recommender queries."""

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from pipelines.recommender_queries import (
    RecommenderQueries,
    get_cohort_users,
    get_recommendations,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_scores_data():
    """Sample scores data for testing."""
    return {
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "user_id": [101, 101, 102, 102, 103, 103, 104, 104, 105, 105],
        "beatmap_id": [201, 202, 201, 203, 202, 204, 203, 205, 204, 206],
        "score": [
            1000000,
            950000,
            980000,
            920000,
            990000,
            970000,
            985000,
            930000,
            995000,
            960000,
        ],
        "pp": [500.0, 480.0, 450.0, 420.0, 600.0, 550.0, 520.0, 490.0, 580.0, 530.0],
        "playmode": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "data": [
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "HR"}]}',
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": []}',
            '{"mods": [{"acronym": "HR"}]}',
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": []}',
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "DT"}]}',
            '{"mods": [{"acronym": "HR"}]}',
        ],
        "mods_key": ["DT", "HR", "DT", "", "HR", "DT", "", "DT", "DT", "HR"],
        "speed_mod": ["DT", None, "DT", None, None, "DT", None, "DT", "DT", None],
    }


@pytest.fixture
def sample_beatmaps_data():
    """Sample beatmaps data for testing."""
    return {
        "beatmap_id": [201, 202, 203, 204, 205, 206],
        "beatmapset_id": [301, 302, 303, 304, 305, 306],
        "version": ["Hard", "Insane", "Expert", "Normal", "Easy", "Extra"],
        "difficultyrating": [4.5, 5.2, 6.1, 3.2, 2.1, 6.8],
    }


@pytest.fixture
def sample_beatmapsets_data():
    """Sample beatmapsets data for testing."""
    return {
        "beatmapset_id": [301, 302, 303, 304, 305, 306],
        "artist": [
            "Artist A",
            "Artist B",
            "Artist C",
            "Artist D",
            "Artist E",
            "Artist F",
        ],
        "title": ["Song A", "Song B", "Song C", "Song D", "Song E", "Song F"],
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
def populated_warehouse(
    tmp_path,
    temp_parquet_file,
    sample_scores_data,
    sample_beatmaps_data,
    sample_beatmapsets_data,
):
    """Create a populated warehouse with all necessary tables."""
    from pipelines.duckdb_pipeline import DuckDBPipeline

    warehouse_dir = tmp_path / "warehouse"
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Create parquet files
    scores_path = temp_parquet_file(sample_scores_data, "scores.parquet")

    # Create beatmaps parquet
    beatmaps_table = pa.Table.from_pydict(sample_beatmaps_data)
    beatmaps_path = parquet_dir / "beatmaps.parquet"
    pq.write_table(beatmaps_table, beatmaps_path)

    # Create beatmapsets parquet
    beatmapsets_table = pa.Table.from_pydict(sample_beatmapsets_data)
    beatmapsets_path = parquet_dir / "beatmapsets.parquet"
    pq.write_table(beatmapsets_table, beatmapsets_path)

    # Run pipeline
    pipeline = DuckDBPipeline(warehouse_dir, "osu")
    try:
        pipeline.initialize()
        pipeline.load_parquet_to_raw(table_name="scores", parquet_path=str(scores_path))
        pipeline.load_parquet_to_raw(
            table_name="beatmaps", parquet_path=str(beatmaps_path)
        )
        pipeline.load_parquet_to_raw(
            table_name="beatmapsets", parquet_path=str(beatmapsets_path)
        )
        pipeline.create_stg_scores()
        pipeline.create_mart_best_scores()
        pipeline.create_mart_user_topk()
        pipeline.create_mart_beatmap_user_sets()
        pipeline.create_indexes()

        yield str(warehouse_dir / "osu.duckdb")
    finally:
        pipeline.close()


# =============================================================================
# Class Interface Tests
# =============================================================================


def test_recommender_queries_class_imports():
    """Should be able to import RecommenderQueries class."""
    assert RecommenderQueries is not None


def test_recommender_queries_init(populated_warehouse):
    """Should initialize with database path."""
    queries = RecommenderQueries(populated_warehouse)
    assert str(queries.db_path) == populated_warehouse


def test_recommender_queries_connect(populated_warehouse):
    """Should connect to database."""
    queries = RecommenderQueries(populated_warehouse)
    conn = queries.connect()
    assert conn is not None


def test_get_cohort_users_basic(populated_warehouse):
    """Should get cohort users for a beatmap."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    # Beatmap 201 has users 101, 102, 103 with DT mod
    cohort = queries.get_cohort_users(beatmap_id=201, mods="DT")

    assert len(cohort) == 2  # Users 101 and 102
    assert 101 in cohort
    assert 102 in cohort


def test_get_cohort_users_with_pp_range(populated_warehouse):
    """Should filter cohort users by PP range."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    # Get users who played beatmap 201 with PP between 400 and 490
    cohort = queries.get_cohort_users(
        beatmap_id=201, pp_lower=400.0, pp_upper=490.0, mods="DT"
    )

    # User 102 has pp=450, User 101 has pp=500
    assert len(cohort) == 1
    assert 102 in cohort


def test_get_cohort_users_no_filters(populated_warehouse):
    """Should get all cohort users without filters."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    # Get all users who played beatmap 201
    cohort = queries.get_cohort_users(beatmap_id=201)

    # Beatmap 201 has users 101 (DT), 102 (DT)
    assert len(cohort) == 2


def test_create_cohort_cache(populated_warehouse):
    """Should create cohort cache temp table."""
    queries = RecommenderQueries(populated_warehouse)
    conn = queries.connect()

    user_ids = [101, 102, 103]
    queries.create_cohort_cache(user_ids)

    # Verify temp table exists
    result = conn.execute("SELECT COUNT(*) FROM mart_cohort_cache").fetchone()
    assert result[0] == 3

    # Verify correct user IDs
    result = conn.execute(
        "SELECT user_id FROM mart_cohort_cache ORDER BY user_id"
    ).fetchall()
    ids = [row[0] for row in result]
    assert ids == [101, 102, 103]


def test_create_cohort_cache_empty(populated_warehouse):
    """Should handle empty cohort list."""
    queries = RecommenderQueries(populated_warehouse)
    conn = queries.connect()

    queries.create_cohort_cache([])

    # Verify temp table exists but is empty
    result = conn.execute("SELECT COUNT(*) FROM mart_cohort_cache").fetchone()
    assert result[0] == 0


def test_get_recommendations_basic(populated_warehouse):
    """Should get recommendations based on cohort users."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    # Use larger cohort to find overlapping beatmaps
    # Users 101, 102, 103 have played various beatmaps
    cohort = [101, 102, 103, 104, 105]
    recommendations = queries.get_recommendations(
        cohort, min_cohort_overlap=1, min_total_players=1
    )

    # Should return recommendations
    assert isinstance(recommendations, list)
    assert len(recommendations) > 0

    # Check structure of first recommendation
    rec = recommendations[0]
    assert "beatmap_id" in rec
    assert "version" in rec
    assert "artist" in rec
    assert "title" in rec
    assert "cohort_overlap" in rec
    assert "novelty_score" in rec


def test_get_recommendations_novelty_score(populated_warehouse):
    """Should calculate novelty score correctly."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    cohort = [101, 102]
    recommendations = queries.get_recommendations(cohort, min_cohort_overlap=1)

    for rec in recommendations:
        # Novelty score should be between 0 and 1
        assert 0.0 <= rec["novelty_score"] <= 1.0

        # Verify calculation: 1 - (cohort_overlap / total_players)
        expected = 1.0 - (rec["cohort_overlap"] / rec["total_players"])
        assert abs(rec["novelty_score"] - expected) < 0.001


def test_get_recommendations_min_overlap(populated_warehouse):
    """Should filter by minimum cohort overlap."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    cohort = [101, 102, 103, 104, 105]

    # Get recommendations with min_overlap=2
    recs_overlap_2 = queries.get_recommendations(cohort, min_cohort_overlap=2)

    # All recommendations should have cohort_overlap >= 2
    for rec in recs_overlap_2:
        assert rec["cohort_overlap"] >= 2


def test_get_recommendations_min_players(populated_warehouse):
    """Should filter by minimum total players."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    cohort = [101, 102, 103, 104, 105]

    # Get recommendations with min_total_players=3
    recs = queries.get_recommendations(cohort, min_total_players=3)

    # All recommendations should have total_players >= 3
    for rec in recs:
        assert rec["total_players"] >= 3


def test_get_recommendations_limit(populated_warehouse):
    """Should respect limit parameter."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    cohort = [101, 102, 103, 104, 105]

    # Get only 2 recommendations
    recs = queries.get_recommendations(cohort, limit=2)

    assert len(recs) <= 2


def test_get_recommendations_empty_cohort(populated_warehouse):
    """Should handle empty cohort."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    recommendations = queries.get_recommendations([])

    assert recommendations == []


def test_get_beatmap_metadata(populated_warehouse):
    """Should get metadata for beatmaps."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    metadata = queries.get_beatmap_metadata([201, 202])

    assert len(metadata) == 2

    # Check structure
    for meta in metadata:
        assert "beatmap_id" in meta
        assert "version" in meta
        assert "artist" in meta
        assert "title" in meta
        assert "difficultyrating" in meta


def test_get_beatmap_metadata_empty(populated_warehouse):
    """Should handle empty beatmap list."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    metadata = queries.get_beatmap_metadata([])

    assert metadata == []


# =============================================================================
# Convenience Function Tests
# =============================================================================


def test_get_cohort_users_convenience_function(populated_warehouse):
    """Should get cohort users using convenience function."""
    conn = duckdb.connect(populated_warehouse)

    cohort = get_cohort_users(conn, beatmap_id=201, mods="DT")

    assert len(cohort) == 2
    assert 101 in cohort
    assert 102 in cohort

    conn.close()


def test_get_recommendations_convenience_function(populated_warehouse):
    """Should get recommendations using convenience function."""
    conn = duckdb.connect(populated_warehouse)

    cohort = [101, 102, 103, 104, 105]
    recommendations = get_recommendations(
        conn, cohort, min_overlap=1, min_total_players=1
    )

    assert isinstance(recommendations, list)
    assert len(recommendations) > 0

    conn.close()


# =============================================================================
# Integration Tests
# =============================================================================


def test_full_recommendation_flow(populated_warehouse):
    """Test full flow: get cohort -> get recommendations."""
    queries = RecommenderQueries(populated_warehouse)
    queries.connect()

    # Step 1: Get cohort from seed beatmap
    cohort = queries.get_cohort_users(beatmap_id=201, mods="DT")
    assert len(cohort) > 0

    # Step 2: Get recommendations based on cohort
    # Use larger cohort to find overlapping beatmaps
    extended_cohort = [101, 102, 103, 104, 105]
    recommendations = queries.get_recommendations(
        extended_cohort, min_cohort_overlap=1, min_total_players=1
    )
    assert len(recommendations) > 0

    # Verify recommendations are ordered by cohort_overlap desc
    for i in range(len(recommendations) - 1):
        assert (
            recommendations[i]["cohort_overlap"]
            >= recommendations[i + 1]["cohort_overlap"]
        )


def test_recommendations_use_precomputed_table(populated_warehouse):
    """Should use mart_beatmap_user_sets for recommendations."""
    queries = RecommenderQueries(populated_warehouse)
    conn = queries.connect()

    # Verify mart_beatmap_user_sets exists and has data
    result = conn.execute("SELECT COUNT(*) FROM mart_beatmap_user_sets").fetchone()
    assert result[0] > 0

    # Get recommendations
    cohort = [101, 102, 103]
    recommendations = queries.get_recommendations(cohort)

    # Should return results
    assert isinstance(recommendations, list)


def test_temp_table_cleanup(populated_warehouse):
    """Should clean up temp tables after query."""
    queries = RecommenderQueries(populated_warehouse)
    conn = queries.connect()

    cohort = [101, 102]
    queries.get_recommendations(cohort)

    # Temp table should be dropped
    result = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = 'mart_cohort_cache'
    """).fetchone()

    assert result[0] == 0


# =============================================================================
# Performance Tests
# =============================================================================


def test_query_performance_explain(populated_warehouse):
    """Should have efficient query plan."""
    queries = RecommenderQueries(populated_warehouse)
    conn = queries.connect()

    # Create cohort cache
    queries.create_cohort_cache([101, 102, 103])

    # Get query plan
    query = """
        WITH candidate_beatmaps AS (
            SELECT
                beatmap_id,
                mods_key,
                user_count,
                (SELECT COUNT(*) FROM mart_cohort_cache c
                 WHERE c.user_id = ANY(bus.user_ids)) as cohort_overlap
            FROM mart_beatmap_user_sets bus
            WHERE user_count >= 1
        )
        SELECT cb.beatmap_id, cb.cohort_overlap
        FROM candidate_beatmaps cb
        WHERE cb.cohort_overlap >= 1
        LIMIT 10
    """

    plan = conn.execute(f"EXPLAIN {query}").fetchall()

    # Should have a query plan
    assert len(plan) > 0

    # Cleanup
    conn.execute("DROP TABLE IF EXISTS mart_cohort_cache")
