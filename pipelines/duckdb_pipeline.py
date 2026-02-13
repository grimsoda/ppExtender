"""DuckDB pipeline for creating silver and gold tables from Parquet bronze layer."""

from pathlib import Path
from typing import Optional, Union
import duckdb


class DuckDBPipeline:
    """
    Pipeline for loading Parquet files into DuckDB and creating mart tables.
    """

    def __init__(
        self,
        warehouse_dir: Union[str, Path],
        database_name: str = "osu",
        preserve_insertion_order: bool = False,
    ):
        self.warehouse_dir = Path(warehouse_dir)
        self.database_name = database_name
        self.db_path = self.warehouse_dir / f"{database_name}.duckdb"
        self.conn = None
        self.preserve_insertion_order = preserve_insertion_order

        # Create warehouse directory
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB with performance settings."""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path))
            self.conn.execute(
                f"SET preserve_insertion_order = {str(self.preserve_insertion_order).lower()}"
            )
        return self.conn

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def initialize(self) -> None:
        """Initialize the database connection."""
        self.connect()

    def execute(self, query: str):
        """Execute a SQL query and return results."""
        conn = self.connect()
        result = conn.execute(query).fetchall()
        return result

    def _resolve_parquet_path(self, parquet_dir: Path, table_name: str) -> str:
        import glob as glob_module

        subdir_pattern = parquet_dir / table_name / "*.parquet"
        root_pattern = parquet_dir / "*.parquet"

        if glob_module.glob(str(subdir_pattern)):
            return str(subdir_pattern)
        return str(root_pattern)

    def load_parquet_to_raw(
        self, table_name: str, parquet_path: Union[str, Path], **options
    ) -> None:
        """Load Parquet files to raw table using CTAS."""
        conn = self.connect()

        # Drop existing table if exists
        conn.execute(f"DROP TABLE IF EXISTS raw_{table_name}")

        # Load parquet using CTAS
        query = f"""
            CREATE TABLE raw_{table_name} AS 
            SELECT * FROM read_parquet('{parquet_path}')
        """
        conn.execute(query)

    def create_stg_scores(self) -> None:
        """Create stg_scores with ruleset_id filtering (0 = osu!standard)."""
        conn = self.connect()

        # Drop existing table if exists
        conn.execute("DROP TABLE IF EXISTS stg_scores")

        # Create stg_scores with ruleset_id=0 filter (osu!standard)
        # Extract mods from data JSON and calculate speed_mod
        query = """
            CREATE TABLE stg_scores AS
            SELECT
                id,
                user_id,
                beatmap_id,
                total_score as score,
                pp,
                accuracy,
                ruleset_id,
                rank,
                max_combo,
                data,
                -- Extract mods from JSON data field
                CASE
                    WHEN json_extract(data, '$.mods') IS NOT NULL
                    THEN json_extract(data, '$.mods')
                    ELSE 'NM'
                END as mods_key,
                -- Calculate speed mod based on mods
                CASE
                    WHEN json_extract(data, '$.mods') LIKE '%DT%' OR json_extract(data, '$.mods') LIKE '%NC%' THEN 'DT'
                    WHEN json_extract(data, '$.mods') LIKE '%HT%' THEN 'HT'
                    ELSE 'NM'
                END as speed_mod
            FROM raw_scores
            WHERE ruleset_id = 0
        """
        conn.execute(query)

    def create_mart_best_scores(self) -> None:
        """Create mart_best_scores with deduplication using iterative batch processing."""
        conn = self.connect()

        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET threads = 2")

        conn.execute("DROP TABLE IF EXISTS mart_best_scores")
        conn.execute("DROP TABLE IF EXISTS temp_best_scores")

        # Create empty result table
        conn.execute("""
            CREATE TABLE mart_best_scores (
                id UBIGINT,
                user_id UINTEGER,
                beatmap_id UINTEGER,
                score UBIGINT,
                pp FLOAT,
                accuracy FLOAT,
                rank VARCHAR,
                max_combo UINTEGER,
                data VARCHAR,
                mods_key VARCHAR,
                speed_mod VARCHAR
            )
        """)

        # Get distinct user_id ranges
        conn.execute("""
            CREATE TEMPORARY TABLE user_ranges AS
            SELECT user_id, NTILE(10) OVER (ORDER BY user_id) as batch_num
            FROM (SELECT DISTINCT user_id FROM stg_scores)
        """)

        # Process each batch
        for batch in range(1, 11):
            conn.execute(f"""
                INSERT INTO mart_best_scores
                SELECT
                    id,
                    user_id,
                    beatmap_id,
                    score,
                    pp,
                    accuracy,
                    rank,
                    max_combo,
                    data,
                    mods_key,
                    speed_mod
                FROM stg_scores
                WHERE user_id IN (SELECT user_id FROM user_ranges WHERE batch_num = {batch})
                QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, beatmap_id, mods_key ORDER BY pp DESC) = 1
            """)

        conn.execute("DROP TABLE user_ranges")

    def create_mart_user_topk(self, top_k: int = 500) -> None:
        """Create mart_user_topk with top_k per user/speed_mod using batch processing.

        Args:
            top_k: Number of top scores to include per user per speed_mod (default: 500)
        """
        conn = self.connect()

        conn.execute("SET preserve_insertion_order = false")
        conn.execute("SET threads = 2")

        conn.execute("DROP TABLE IF EXISTS mart_user_topk")

        # Create empty result table
        conn.execute("""
            CREATE TABLE mart_user_topk (
                id UBIGINT,
                user_id UINTEGER,
                beatmap_id UINTEGER,
                score UBIGINT,
                pp FLOAT,
                accuracy FLOAT,
                rank VARCHAR,
                max_combo UINTEGER,
                data VARCHAR,
                mods_key VARCHAR,
                speed_mod VARCHAR
            )
        """)

        # Get distinct user_id ranges
        conn.execute("""
            CREATE TEMPORARY TABLE user_ranges_topk AS
            SELECT user_id, NTILE(10) OVER (ORDER BY user_id) as batch_num
            FROM (SELECT DISTINCT user_id FROM mart_best_scores)
        """)

        # Process each batch
        for batch in range(1, 11):
            conn.execute(f"""
                INSERT INTO mart_user_topk
                SELECT
                    id,
                    user_id,
                    beatmap_id,
                    score,
                    pp,
                    accuracy,
                    rank,
                    max_combo,
                    data,
                    mods_key,
                    speed_mod
                FROM mart_best_scores
                WHERE user_id IN (SELECT user_id FROM user_ranges_topk WHERE batch_num = {batch})
                QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, speed_mod ORDER BY pp DESC) <= {top_k}
            """)

        conn.execute("DROP TABLE user_ranges_topk")

    def create_mart_beatmap_user_sets(self) -> None:
        """
        Create mart_beatmap_user_sets with precomputed stats.
        CRITICAL for sub-second query performance.
        """
        conn = self.connect()

        # Drop existing table if exists
        conn.execute("DROP TABLE IF EXISTS mart_beatmap_user_sets")

        # Create mart_beatmap_user_sets with ARRAY_AGG and percentiles
        query = """
            CREATE TABLE mart_beatmap_user_sets AS
            SELECT 
                beatmap_id,
                mods_key,
                ARRAY_AGG(user_id) as user_ids,
                COUNT(*) as user_count,
                AVG(pp) as avg_pp,
                STDDEV(pp) as std_pp,
                MIN(pp) as min_pp,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pp) as median_pp,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pp) as p75_pp,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY pp) as p90_pp
            FROM mart_best_scores
            GROUP BY beatmap_id, mods_key
        """
        conn.execute(query)

    def create_indexes(self) -> None:
        """
        Create indexes for query performance.
        CRITICAL for sub-second query performance.
        """
        conn = self.connect()

        # Drop existing indexes if they exist
        conn.execute("DROP INDEX IF EXISTS idx_mart_best_scores_beatmap_lookup")
        conn.execute("DROP INDEX IF EXISTS idx_mart_best_scores_user_lookup")

        # Create index for cohort extraction (beatmap_id is the filter)
        conn.execute("""
            CREATE INDEX idx_mart_best_scores_beatmap_lookup 
            ON mart_best_scores(beatmap_id, pp, mods_key, user_id)
        """)

        # Create index for cohort score retrieval (user_id is the filter)
        conn.execute("""
            CREATE INDEX idx_mart_best_scores_user_lookup 
            ON mart_best_scores(user_id, beatmap_id, pp)
        """)

    def create_api_views(self) -> None:
        conn = self.connect()

        conn.execute("""
            CREATE OR REPLACE VIEW user_beatmap_playcount AS
            SELECT
                mbs.user_id,
                mbs.beatmap_id,
                mbs.score,
                mbs.pp,
                mbs.mods_key as mods,
                mbs.accuracy,
                mbs.rank,
                COALESCE(ubp.playcount, 1) as playcount
            FROM mart_best_scores mbs
            LEFT JOIN raw_osu_user_beatmap_playcount ubp
                ON mbs.user_id = ubp.user_id
                AND mbs.beatmap_id = ubp.beatmap_id
        """)

        conn.execute("""
            CREATE OR REPLACE VIEW beatmaps AS
            SELECT 
                b.beatmap_id,
                b.beatmapset_id,
                bs.title,
                bs.artist,
                b.version,
                bs.creator,
                b.difficultyrating as difficulty_rating,
                COALESCE(b.bpm, bs.bpm) as bpm,
                b.total_length,
                b.hit_length,
                b.playmode as mode,
                b.approved as status,
                b.countTotal,
                b.countNormal,
                b.countSlider,
                b.countSpinner,
                b.diff_drain,
                b.diff_size,
                b.diff_overall,
                b.diff_approach,
                b.max_combo,
                b.playcount,
                b.passcount
            FROM raw_osu_beatmaps b
            LEFT JOIN raw_osu_beatmapsets bs ON b.beatmapset_id = bs.beatmapset_id
        """)
        conn.execute("""
            CREATE OR REPLACE VIEW users AS
            SELECT 
                user_id,
                CAST(user_id as VARCHAR) as username,
                rank_score as pp_raw,
                accuracy,
                playcount,
                level
            FROM raw_osu_user_stats
        """)

    def run_full_pipeline(
        self, parquet_dir: Union[str, Path], tables: Optional[list] = None
    ) -> dict:
        """Run the full pipeline from Parquet to gold tables."""
        parquet_dir = Path(parquet_dir)

        scores_path = self._resolve_parquet_path(parquet_dir, "scores")
        self.load_parquet_to_raw(table_name="scores", parquet_path=scores_path)

        # Create staging table
        self.create_stg_scores()

        # Create mart tables
        self.create_mart_best_scores()
        self.create_mart_user_topk()
        self.create_mart_beatmap_user_sets()

        self.create_indexes()
        self.create_api_views()

        # Get table counts for manifest
        conn = self.connect()
        manifest = {"database_path": str(self.db_path), "tables": {}}

        for table in [
            "raw_scores",
            "stg_scores",
            "mart_best_scores",
            "mart_user_topk",
            "mart_beatmap_user_sets",
        ]:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                row_count = result[0] if result else 0
                manifest["tables"][table] = {"row_count": row_count}
            except:
                manifest["tables"][table] = {"row_count": 0}

        return manifest


def create_pipeline(
    parquet_dir: Union[str, Path],
    warehouse_dir: Union[str, Path],
    database_name: str = "osu",
) -> dict:
    """
    Convenience function to create full pipeline.

    Returns:
        Manifest dictionary with table info
    """
    pipeline = DuckDBPipeline(warehouse_dir, database_name)
    try:
        manifest = pipeline.run_full_pipeline(parquet_dir)
        return manifest
    finally:
        pipeline.close()
