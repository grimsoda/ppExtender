#!/usr/bin/env python3
"""
SQL to CSV Parallel Converter

Converts MySQL dump files to CSV chunks for parallel LOAD DATA INFILE.
Phase 1 of the parallel import pipeline.

Usage:
    python sql_to_csv_parallel.py --table scores --input-dir data/ingest/2026-02/sql/
    python sql_to_csv_parallel.py --all --input-dir data/ingest/2026-02/sql/
    python sql_to_csv_parallel.py --dry-run --all
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent / "lib"))

from sql_parser import StreamingSQLParser, CSVChunkWriter
from parallel_utils import ChunkPlanner, format_bytes, format_duration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# Tables in dependency order (small to large)
DEFAULT_TABLES = [
    "osu_counts",
    "osu_difficulty_attribs",
    "osu_beatmap_performance_blacklist",
    "sample_users",
    "osu_user_stats",
    "osu_beatmapsets",
    "osu_beatmaps",
    "osu_beatmap_failtimes",
    "osu_beatmap_difficulty",
    "osu_user_beatmap_playcount",
    "osu_beatmap_difficulty_attribs",
    "osu_scores_high",
    "scores",
]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert MySQL SQL dumps to CSV chunks for parallel import",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --table scores --input-dir data/ingest/2026-02/sql/
  %(prog)s --all --input-dir data/ingest/2026-02/sql/ --output-dir data/csv_chunks/
  %(prog)s --dry-run --all
  %(prog)s --table scores --chunk-size 500000
        """,
    )

    parser.add_argument("--table", help="Convert specific table only")

    parser.add_argument("--all", action="store_true", help="Convert all tables")

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("/run/media/work/OS/ppExtender/data/ingest/2026-02/sql"),
        help="Directory containing SQL files (default: data/ingest/2026-02/sql)",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/run/media/work/OS/ppExtender/data/csv_chunks"),
        help="Output directory for CSV chunks (default: data/csv_chunks)",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Rows per chunk (default: auto based on file size)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without converting",
    )

    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Skip tables that already have CSV chunks",
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    return parser.parse_args()


def convert_table(
    sql_file: Path,
    output_dir: Path,
    table_name: str,
    chunk_size: Optional[int] = None,
    dry_run: bool = False,
) -> dict:
    """
    Convert a single SQL file to CSV chunks.

    Returns dict with conversion statistics.
    """
    start_time = time.time()

    # Determine chunk size
    if chunk_size is None:
        planner = ChunkPlanner()
        plan = planner.plan_chunks(sql_file, table_name)
        chunk_size = plan["chunk_size"]

    logger.info(f"Converting {table_name} with chunk size {chunk_size:,} rows")

    if dry_run:
        file_size = sql_file.stat().st_size
        planner = ChunkPlanner()
        plan = planner.plan_chunks(sql_file, table_name)
        logger.info(f"  File size: {format_bytes(file_size)}")
        logger.info(f"  Estimated chunks: {plan['num_chunks']}")
        logger.info(f"  Estimated rows: {plan['estimated_rows']:,}")
        return {
            "table": table_name,
            "chunks_created": plan["num_chunks"],
            "rows_converted": plan["estimated_rows"],
            "duration": 0,
        }

    # Create output directory for this table
    table_output_dir = output_dir / table_name
    table_output_dir.mkdir(parents=True, exist_ok=True)

    # Parse SQL and write CSV chunks
    parser = StreamingSQLParser(table_name)
    writer = CSVChunkWriter(
        output_dir=table_output_dir, table_name=table_name, chunk_size=chunk_size
    )

    total_rows = 0
    columns = None

    try:
        with writer:
            for cols, row in parser.parse_file(str(sql_file)):
                if columns is None:
                    columns = cols
                    writer.columns = columns

                writer.write_row(row)
                total_rows += 1

                if total_rows % 1_000_000 == 0:
                    logger.info(f"  Processed {total_rows:,} rows...")

        duration = time.time() - start_time
        chunk_files = writer.get_chunk_files()

        logger.info(
            f"  Completed: {total_rows:,} rows in {len(chunk_files)} chunks "
            f"({format_duration(duration)})"
        )

        return {
            "table": table_name,
            "chunks_created": len(chunk_files),
            "rows_converted": total_rows,
            "duration": duration,
            "output_dir": str(table_output_dir),
        }

    except Exception as e:
        logger.error(f"  Failed to convert {table_name}: {e}")
        raise


def should_skip_table(table_name: str, output_dir: Path) -> bool:
    """Check if table already has CSV chunks."""
    table_output_dir = output_dir / table_name
    if not table_output_dir.exists():
        return False

    chunk_files = list(table_output_dir.glob(f"{table_name}_chunk_*.csv"))
    return len(chunk_files) > 0


def main():
    """Main entry point."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate arguments
    if not args.table and not args.all:
        logger.error("Must specify --table or --all")
        sys.exit(1)

    # Determine tables to process
    if args.all:
        tables = DEFAULT_TABLES
    else:
        tables = [args.table]

    # Validate input directory
    if not args.input_dir.exists():
        logger.error(f"Input directory not found: {args.input_dir}")
        sys.exit(1)

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Show dry-run summary
    if args.dry_run:
        logger.info("=" * 60)
        logger.info("DRY RUN - No files will be created")
        logger.info("=" * 60)

    # Process tables
    results = []
    total_start = time.time()

    for table in tables:
        sql_file = args.input_dir / f"{table}.sql"

        if not sql_file.exists():
            logger.warning(f"SQL file not found: {sql_file}")
            continue

        if args.keep_existing and should_skip_table(table, args.output_dir):
            logger.info(f"Skipping {table} - chunks already exist")
            continue

        logger.info(f"\nProcessing table: {table}")
        logger.info(f"  Input: {sql_file}")
        logger.info(f"  Output: {args.output_dir / table}")

        try:
            result = convert_table(
                sql_file=sql_file,
                output_dir=args.output_dir,
                table_name=table,
                chunk_size=args.chunk_size,
                dry_run=args.dry_run,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process {table}: {e}")
            results.append(
                {
                    "table": table,
                    "error": str(e),
                    "chunks_created": 0,
                    "rows_converted": 0,
                }
            )

    # Summary
    total_duration = time.time() - total_start
    total_chunks = sum(r.get("chunks_created", 0) for r in results)
    total_rows = sum(r.get("rows_converted", 0) for r in results)

    logger.info("\n" + "=" * 60)
    logger.info("CONVERSION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Tables processed: {len(results)}")
    logger.info(f"Total chunks: {total_chunks}")
    logger.info(f"Total rows: {total_rows:,}")
    logger.info(f"Total time: {format_duration(total_duration)}")

    if not args.dry_run and total_duration > 0:
        logger.info(f"Average speed: {total_rows / total_duration:,.0f} rows/sec")

    # List output directories
    if not args.dry_run:
        logger.info(f"\nCSV chunks saved to: {args.output_dir}")
        for table in tables:
            table_dir = args.output_dir / table
            if table_dir.exists():
                chunks = list(table_dir.glob(f"{table}_chunk_*.csv"))
                if chunks:
                    logger.info(f"  {table}: {len(chunks)} chunk(s)")


if __name__ == "__main__":
    main()
