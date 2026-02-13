"""Recommender query logic for osu! beatmap recommendations."""

from pathlib import Path
from typing import List, Dict, Optional, Union
import duckdb


class RecommenderQueries:
    """
    SQL-based recommender queries using precomputed tables.
    Optimized for sub-second query performance.
    """

    def __init__(self, db_path: Union[str, Path]):
        self.db_path = Path(db_path)
        self.conn = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to DuckDB."""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path))
        return self.conn

    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_cohort_users(
        self,
        beatmap_id: int,
        pp_lower: Optional[float] = None,
        pp_upper: Optional[float] = None,
        mods: Optional[str] = None,
    ) -> List[int]:
        """
        Get cohort users who played the seed beatmap.

        Args:
            beatmap_id: Seed beatmap ID
            pp_lower: Minimum PP (optional)
            pp_upper: Maximum PP (optional)
            mods: Mods key (optional, e.g., 'DT', 'HR,DT')

        Returns:
            List of user IDs
        """
        conn = self.connect()

        conditions = ["beatmap_id = ?"]
        params: list = [beatmap_id]

        if pp_lower is not None:
            conditions.append("pp >= ?")
            params.append(pp_lower)

        if pp_upper is not None:
            conditions.append("pp <= ?")
            params.append(pp_upper)

        if mods is not None:
            conditions.append("mods_key = ?")
            params.append(mods)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT DISTINCT user_id
            FROM mart_best_scores
            WHERE {where_clause}
        """

        result = conn.execute(query, params).fetchall()
        return [row[0] for row in result]

    def create_cohort_cache(self, user_ids: List[int]) -> None:
        """
        Create temp table with cohort users.
        CRITICAL for performance - avoids large IN clause.
        """
        conn = self.connect()

        # Drop existing temp table if exists
        conn.execute("DROP TABLE IF EXISTS mart_cohort_cache")

        if not user_ids:
            # Create empty temp table
            conn.execute("""
                CREATE TEMPORARY TABLE mart_cohort_cache (
                    user_id INTEGER PRIMARY KEY
                )
            """)
            return

        # Create temp table and insert values
        # Using batch insert for better performance with large cohorts
        conn.execute("""
            CREATE TEMPORARY TABLE mart_cohort_cache (
                user_id INTEGER PRIMARY KEY
            )
        """)

        # Insert user_ids in batches to avoid parameter limits
        batch_size = 1000
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i : i + batch_size]
            placeholders = ",".join(["?"] * len(batch))
            conn.execute(f"""
                INSERT INTO mart_cohort_cache (user_id)
                VALUES {",".join([f"({uid})" for uid in batch])}
            """)

    def get_recommendations(
        self,
        cohort_users: List[int],
        min_cohort_overlap: int = 3,
        min_total_players: int = 5,
        limit: int = 100,
    ) -> List[Dict]:
        """
        Get beatmap recommendations based on cohort users.
        Uses precomputed mart_beatmap_user_sets for <1s performance.

        Args:
            cohort_users: List of user IDs in cohort
            min_cohort_overlap: Minimum cohort users who played beatmap
            min_total_players: Minimum total players for beatmap
            limit: Maximum recommendations to return

        Returns:
            List of recommendation dicts with beatmap info and stats
        """
        conn = self.connect()

        # Step 1: Create cohort cache
        self.create_cohort_cache(cohort_users)

        try:
            # Step 2: Query using precomputed table with ARRAY overlap
            query = """
                WITH candidate_beatmaps AS (
                    SELECT
                        beatmap_id,
                        mods_key,
                        user_count,
                        avg_pp,
                        std_pp,
                        min_pp,
                        median_pp,
                        p75_pp,
                        p90_pp,
                        -- Calculate overlap using ARRAY overlap (O(n) vs O(n*m))
                        (SELECT COUNT(*) FROM mart_cohort_cache c
                         WHERE c.user_id = ANY(bus.user_ids)) as cohort_overlap
                    FROM mart_beatmap_user_sets bus
                    WHERE user_count >= ?
                )
                SELECT
                    cb.beatmap_id,
                    b.version,
                    bs.artist,
                    bs.title,
                    b.difficultyrating,
                    cb.user_count as total_players,
                    cb.cohort_overlap,
                    cb.avg_pp,
                    cb.std_pp,
                    cb.min_pp,
                    cb.median_pp,
                    cb.p75_pp,
                    cb.p90_pp,
                    -- Novelty score: inverse of cohort overlap
                    (1.0 - (cb.cohort_overlap::FLOAT / cb.user_count)) as novelty_score
                FROM candidate_beatmaps cb
                JOIN raw_beatmaps b ON cb.beatmap_id = b.beatmap_id
                JOIN raw_beatmapsets bs ON b.beatmapset_id = bs.beatmapset_id
                WHERE cb.cohort_overlap >= ?
                ORDER BY
                    cb.cohort_overlap DESC,
                    cb.avg_pp DESC
                LIMIT ?
            """

            result = conn.execute(
                query, [min_total_players, min_cohort_overlap, limit]
            ).fetchall()

            # Convert to list of dicts
            columns = [
                "beatmap_id",
                "version",
                "artist",
                "title",
                "difficultyrating",
                "total_players",
                "cohort_overlap",
                "avg_pp",
                "std_pp",
                "min_pp",
                "median_pp",
                "p75_pp",
                "p90_pp",
                "novelty_score",
            ]
            return [dict(zip(columns, row)) for row in result]

        finally:
            # Cleanup
            conn.execute("DROP TABLE IF EXISTS mart_cohort_cache")

    def get_beatmap_metadata(self, beatmap_ids: List[int]) -> List[Dict]:
        """Get metadata for multiple beatmaps."""
        conn = self.connect()

        if not beatmap_ids:
            return []

        # Create temp table for beatmap IDs
        conn.execute("DROP TABLE IF EXISTS temp_beatmap_ids")
        conn.execute("""
            CREATE TEMPORARY TABLE temp_beatmap_ids (
                beatmap_id INTEGER PRIMARY KEY
            )
        """)

        # Insert beatmap IDs in batches
        batch_size = 1000
        for i in range(0, len(beatmap_ids), batch_size):
            batch = beatmap_ids[i : i + batch_size]
            conn.execute(f"""
                INSERT INTO temp_beatmap_ids (beatmap_id)
                VALUES {",".join([f"({bid})" for bid in batch])}
            """)

        try:
            query = """
                SELECT
                    b.beatmap_id,
                    b.version,
                    bs.artist,
                    bs.title,
                    b.difficultyrating
                FROM temp_beatmap_ids t
                JOIN raw_beatmaps b ON t.beatmap_id = b.beatmap_id
                JOIN raw_beatmapsets bs ON b.beatmapset_id = bs.beatmapset_id
            """

            result = conn.execute(query).fetchall()

            columns = ["beatmap_id", "version", "artist", "title", "difficultyrating"]
            return [dict(zip(columns, row)) for row in result]

        finally:
            conn.execute("DROP TABLE IF EXISTS temp_beatmap_ids")


# Convenience functions
def get_cohort_users(
    conn: duckdb.DuckDBPyConnection,
    beatmap_id: int,
    pp_lower: Optional[float] = None,
    pp_upper: Optional[float] = None,
    mods: Optional[str] = None,
) -> List[int]:
    """Get cohort users from seed beatmap."""
    conditions = ["beatmap_id = ?"]
    params: list = [beatmap_id]

    if pp_lower is not None:
        conditions.append("pp >= ?")
        params.append(pp_lower)

    if pp_upper is not None:
        conditions.append("pp <= ?")
        params.append(pp_upper)

    if mods is not None:
        conditions.append("mods_key = ?")
        params.append(mods)

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT DISTINCT user_id
        FROM mart_best_scores
        WHERE {where_clause}
    """

    result = conn.execute(query, params).fetchall()
    return [row[0] for row in result]


def get_recommendations(
    conn: duckdb.DuckDBPyConnection,
    cohort_users: List[int],
    min_overlap: int = 3,
    min_total_players: int = 5,
    limit: int = 100,
) -> List[Dict]:
    """Get recommendations using precomputed tables."""
    # Create cohort cache
    conn.execute("DROP TABLE IF EXISTS mart_cohort_cache")

    if not cohort_users:
        conn.execute("""
            CREATE TEMPORARY TABLE mart_cohort_cache (
                user_id INTEGER PRIMARY KEY
            )
        """)
        return []

    conn.execute("""
        CREATE TEMPORARY TABLE mart_cohort_cache (
            user_id INTEGER PRIMARY KEY
        )
    """)

    # Insert user_ids in batches
    batch_size = 1000
    for i in range(0, len(cohort_users), batch_size):
        batch = cohort_users[i : i + batch_size]
        conn.execute(f"""
            INSERT INTO mart_cohort_cache (user_id)
            VALUES {",".join([f"({uid})" for uid in batch])}
        """)

    try:
        query = """
            WITH candidate_beatmaps AS (
                SELECT
                    beatmap_id,
                    mods_key,
                    user_count,
                    avg_pp,
                    std_pp,
                    min_pp,
                    median_pp,
                    p75_pp,
                    p90_pp,
                    (SELECT COUNT(*) FROM mart_cohort_cache c
                     WHERE c.user_id = ANY(bus.user_ids)) as cohort_overlap
                FROM mart_beatmap_user_sets bus
                WHERE user_count >= ?
            )
            SELECT
                cb.beatmap_id,
                b.version,
                bs.artist,
                bs.title,
                b.difficultyrating,
                cb.user_count as total_players,
                cb.cohort_overlap,
                cb.avg_pp,
                cb.std_pp,
                cb.min_pp,
                cb.median_pp,
                cb.p75_pp,
                cb.p90_pp,
                (1.0 - (cb.cohort_overlap::FLOAT / cb.user_count)) as novelty_score
            FROM candidate_beatmaps cb
            JOIN raw_beatmaps b ON cb.beatmap_id = b.beatmap_id
            JOIN raw_beatmapsets bs ON b.beatmapset_id = bs.beatmapset_id
            WHERE cb.cohort_overlap >= ?
            ORDER BY
                cb.cohort_overlap DESC,
                cb.avg_pp DESC
            LIMIT ?
        """

        result = conn.execute(query, [min_total_players, min_overlap, limit]).fetchall()

        columns = [
            "beatmap_id",
            "version",
            "artist",
            "title",
            "difficultyrating",
            "total_players",
            "cohort_overlap",
            "avg_pp",
            "std_pp",
            "min_pp",
            "median_pp",
            "p75_pp",
            "p90_pp",
            "novelty_score",
        ]
        return [dict(zip(columns, row)) for row in result]

    finally:
        conn.execute("DROP TABLE IF EXISTS mart_cohort_cache")
