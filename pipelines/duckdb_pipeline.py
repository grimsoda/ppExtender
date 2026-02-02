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
        """Create stg_scores with playmode filtering."""
        conn = self.connect()

        # Drop existing table if exists
        conn.execute("DROP TABLE IF EXISTS stg_scores")

        # Create stg_scores with playmode=0 filter
        query = """
            CREATE TABLE stg_scores AS
            SELECT 
                id,
                user_id,
                beatmap_id,
                score,
                pp,
                playmode,
                data,
                mods_key,
                speed_mod
            FROM raw_scores
            WHERE playmode = 0
        """
        conn.execute(query)

    def create_mart_best_scores(self) -> None:
        """Create mart_best_scores with deduplication."""
        conn = self.connect()

        # Drop existing table if exists
        conn.execute("DROP TABLE IF EXISTS mart_best_scores")

        # Create mart_best_scores with ROW_NUMBER deduplication
        query = """
            CREATE TABLE mart_best_scores AS
            SELECT 
                id,
                user_id,
                beatmap_id,
                score,
                pp,
                data,
                mods_key,
                speed_mod
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY user_id, beatmap_id, mods_key ORDER BY pp DESC) as rn
                FROM stg_scores
            ) ranked
            WHERE rn = 1
        """
        conn.execute(query)

    def create_mart_user_topk(self) -> None:
        """Create mart_user_topk with top 100 per user/speed_mod."""
        conn = self.connect()

        # Drop existing table if exists
        conn.execute("DROP TABLE IF EXISTS mart_user_topk")

        # Create mart_user_topk with top 100 per user/speed_mod
        query = """
            CREATE TABLE mart_user_topk AS
            SELECT 
                id,
                user_id,
                beatmap_id,
                score,
                pp,
                data,
                mods_key,
                speed_mod
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY user_id, speed_mod ORDER BY pp DESC) as rn
                FROM mart_best_scores
            ) ranked
            WHERE rn <= 100
        """
        conn.execute(query)

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

        # Create indexes
        self.create_indexes()

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
