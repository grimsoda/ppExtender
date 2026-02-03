#!/usr/bin/env python3
"""
Full ETL Pipeline Orchestrator

Runs the complete pipeline: SQL → Parquet → DuckDB → Gold Tables

Usage:
    python pipelines/run_pipeline.py --full
    python pipelines/run_pipeline.py --phase bronze
    python pipelines/run_pipeline.py --phase silver
    python pipelines/run_pipeline.py --phase gold
    python pipelines/run_pipeline.py --full --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

from sql_parser import parse_sql_file
from parquet_writer import write_parquet_batches
from duckdb_pipeline import DuckDBPipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# Pipeline configuration
DATA_VERSION = "2026-02"
WAREHOUSE_DIR = f"data/warehouse/{DATA_VERSION}"
INGEST_DIR = f"data/ingest/{DATA_VERSION}"
SQL_DIR = f"{INGEST_DIR}/sql"
PARQUET_DIR = f"{INGEST_DIR}/bronze_parquet"

TABLES = [
    "scores",
    "osu_scores_high",
    "osu_beatmaps",
    "osu_beatmapsets",
    "osu_user_stats",
    "osu_beatmap_difficulty",
    "osu_beatmap_difficulty_attribs",
    "osu_user_beatmap_playcount",
    "osu_beatmap_failtimes",
    "osu_counts",
    "osu_difficulty_attribs",
    "osu_beatmap_performance_blacklist",
    "sample_users",
]


class PipelineRunner:
    """Orchestrates the full ETL pipeline."""

    def __init__(self):
        self.start_time = None
        self.phase_times = {}
        self.stats = {
            "tables_processed": 0,
            "total_rows": 0,
        }

    def run_full_pipeline(self, dry_run: bool = False) -> bool:
        """Run complete pipeline."""
        self.start_time = time.time()

        try:
            # Phase 1: Bronze (SQL → Parquet)
            if not self._run_bronze(dry_run):
                return False

            # Phase 2: Silver (Parquet → DuckDB raw)
            if not self._run_silver(dry_run):
                return False

            # Phase 3: Gold (Raw → Staging → Mart)
            if not self._run_gold(dry_run):
                return False

            self._print_summary()
            return True

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _run_bronze(self, dry_run: bool) -> bool:
        """Run bronze phase: SQL → Parquet."""
        logger.info("=" * 70)
        logger.info("BRONZE PHASE: SQL → Parquet")
        logger.info("=" * 70)
        start = time.time()

        for table in TABLES:
            sql_path = Path(f"{SQL_DIR}/{table}.sql")
            output_dir = Path(f"{PARQUET_DIR}/{table}")

            if not sql_path.exists():
                logger.warning(f"  ⚠ Skipping {table} - SQL file not found: {sql_path}")
                continue

            logger.info(f"Processing {table}...")

            if dry_run:
                logger.info(f"  [DRY RUN] Would parse {sql_path} → {output_dir}")
                continue

            try:
                # Parse SQL
                logger.info(f"  Parsing {sql_path}...")
                batches = parse_sql_file(str(sql_path), table)

                # Write Parquet
                logger.info(f"  Writing Parquet to {output_dir}...")
                manifest = write_parquet_batches(
                    batches=batches,
                    output_dir=output_dir,
                    table_name=table,
                    batch_rows=100000,
                    compression="snappy",
                )

                rows = manifest.get("total_rows", 0)
                self.stats["total_rows"] += rows
                self.stats["tables_processed"] += 1
                logger.info(f"  ✓ Wrote {rows:,} rows")

            except Exception as e:
                logger.error(f"  ✗ Failed to process {table}: {e}")
                return False

        self.phase_times["bronze"] = time.time() - start
        logger.info(f"✓ Bronze phase completed in {self.phase_times['bronze']:.1f}s")
        return True

    def _run_silver(self, dry_run: bool) -> bool:
        """Run silver phase: Parquet → DuckDB raw."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("SILVER PHASE: Parquet → DuckDB")
        logger.info("=" * 70)
        start = time.time()

        pipeline = DuckDBPipeline(warehouse_dir=WAREHOUSE_DIR, database_name="osu")

        for table in TABLES:
            parquet_path = Path(f"{PARQUET_DIR}/{table}")
            parquet_glob = f"{parquet_path}/*.parquet"

            if not parquet_path.exists():
                logger.warning(
                    f"  ⚠ Skipping {table} - Parquet not found: {parquet_path}"
                )
                continue

            logger.info(f"Loading {table}...")

            if dry_run:
                logger.info(f"  [DRY RUN] Would load {parquet_glob}")
                continue

            try:
                pipeline.load_parquet_to_raw(table, parquet_glob)
                logger.info(f"  ✓ Loaded {table}")
            except Exception as e:
                logger.error(f"  ✗ Failed to load {table}: {e}")
                return False

        pipeline.close()
        self.phase_times["silver"] = time.time() - start
        logger.info(f"✓ Silver phase completed in {self.phase_times['silver']:.1f}s")
        return True

    def _run_gold(self, dry_run: bool) -> bool:
        """Run gold phase: Create staging and mart tables."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("GOLD PHASE: Creating Mart Tables")
        logger.info("=" * 70)
        start = time.time()

        pipeline = DuckDBPipeline(warehouse_dir=WAREHOUSE_DIR, database_name="osu")

        try:
            # Create staging
            logger.info("Creating stg_scores...")
            if dry_run:
                logger.info("  [DRY RUN] Would create stg_scores")
            else:
                pipeline.create_stg_scores()
                logger.info("  ✓ Created stg_scores")

            # Create mart tables
            logger.info("Creating mart_best_scores...")
            if dry_run:
                logger.info("  [DRY RUN] Would create mart_best_scores")
            else:
                pipeline.create_mart_best_scores()
                logger.info("  ✓ Created mart_best_scores")

            logger.info("Creating mart_user_topk...")
            if dry_run:
                logger.info("  [DRY RUN] Would create mart_user_topk")
            else:
                pipeline.create_mart_user_topk()
                logger.info("  ✓ Created mart_user_topk")

            logger.info("Creating mart_beatmap_user_sets...")
            if dry_run:
                logger.info("  [DRY RUN] Would create mart_beatmap_user_sets")
            else:
                pipeline.create_mart_beatmap_user_sets()
                logger.info("  ✓ Created mart_beatmap_user_sets")

            # Create indexes
            logger.info("Creating performance indexes...")
            if dry_run:
                logger.info("  [DRY RUN] Would create indexes")
            else:
                pipeline.create_indexes()
                logger.info("  ✓ Created indexes")

        except Exception as e:
            logger.error(f"  ✗ Gold phase failed: {e}")
            import traceback

            traceback.print_exc()
            return False
        finally:
            pipeline.close()

        self.phase_times["gold"] = time.time() - start
        logger.info(f"✓ Gold phase completed in {self.phase_times['gold']:.1f}s")
        return True

    def _print_summary(self):
        """Print execution summary."""
        total = time.time() - self.start_time

        print("\n" + "=" * 70)
        print("PIPELINE COMPLETE")
        print("=" * 70)
        print(f"Tables processed: {self.stats['tables_processed']}/{len(TABLES)}")
        print(f"Total rows:       {self.stats['total_rows']:,}")
        print("")
        print(f"Bronze phase:     {self.phase_times.get('bronze', 0):.1f}s")
        print(f"Silver phase:     {self.phase_times.get('silver', 0):.1f}s")
        print(f"Gold phase:       {self.phase_times.get('gold', 0):.1f}s")
        print("")
        print(f"Total time:       {total:.1f}s")
        print("=" * 70)
        print("")
        print("Next steps:")
        print("  1. Start backend:  cd server && npm run dev")
        print("  2. Start frontend: cd app && npm run dev")
        print("  3. Open browser:   http://localhost:5173")


def main():
    parser = argparse.ArgumentParser(
        description="Run ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipelines/run_pipeline.py --full
  python pipelines/run_pipeline.py --phase bronze
  python pipelines/run_pipeline.py --full --dry-run
        """,
    )
    parser.add_argument("--full", action="store_true", help="Run full pipeline")
    parser.add_argument(
        "--phase", choices=["bronze", "silver", "gold"], help="Run specific phase only"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )

    args = parser.parse_args()

    runner = PipelineRunner()

    if args.full:
        success = runner.run_full_pipeline(dry_run=args.dry_run)
    elif args.phase == "bronze":
        success = runner._run_bronze(dry_run=args.dry_run)
    elif args.phase == "silver":
        success = runner._run_silver(dry_run=args.dry_run)
    elif args.phase == "gold":
        success = runner._run_gold(dry_run=args.dry_run)
    else:
        parser.print_help()
        sys.exit(1)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
