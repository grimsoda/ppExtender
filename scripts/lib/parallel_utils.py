"""
Parallel processing utilities for chunk loading system.

Provides process pool management, progress tracking, and task distribution.
"""

import os
import time
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict, Callable, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ChunkTask:
    """Represents a single chunk processing task."""

    chunk_id: int
    chunk_path: Path
    table_name: str
    status: str = "pending"  # pending, running, completed, failed
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    rows_processed: int = 0

    @property
    def duration(self) -> float:
        """Calculate task duration in seconds."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time


@dataclass
class ImportStats:
    """Statistics for a parallel import operation."""

    table_name: str
    total_chunks: int = 0
    completed_chunks: int = 0
    failed_chunks: int = 0
    total_rows: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    chunk_stats: List[ChunkTask] = field(default_factory=list)

    @property
    def duration(self) -> float:
        """Calculate total duration in seconds."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_chunks == 0:
            return 0.0
        return (self.completed_chunks / self.total_chunks) * 100

    @property
    def rows_per_second(self) -> float:
        """Calculate average rows per second."""
        if self.duration == 0:
            return 0.0
        return self.total_rows / self.duration

    def log_summary(self):
        """Log a summary of the import statistics."""
        logger.info(f"Import Summary for {self.table_name}:")
        logger.info(f"  Total chunks: {self.total_chunks}")
        logger.info(f"  Completed: {self.completed_chunks}")
        logger.info(f"  Failed: {self.failed_chunks}")
        logger.info(f"  Success rate: {self.success_rate:.1f}%")
        logger.info(f"  Total rows: {self.total_rows:,}")
        logger.info(f"  Duration: {self.duration:.1f}s")
        logger.info(f"  Rows/sec: {self.rows_per_second:,.0f}")


class ParallelProcessor:
    """
    Manages parallel processing of chunk files.

    Uses process pools for CPU-bound tasks and provides
    progress tracking and error handling.
    """

    def __init__(self, max_workers: Optional[int] = None, progress_interval: int = 10):
        self.max_workers = max_workers or max(1, mp.cpu_count() - 1)
        self.progress_interval = progress_interval
        self.stats: Dict[str, ImportStats] = {}

    def process_chunks(
        self,
        table_name: str,
        chunk_paths: List[Path],
        process_func: Callable[[Path], Any],
        error_callback: Optional[Callable[[ChunkTask, Exception], None]] = None,
    ) -> ImportStats:
        """
        Process multiple chunks in parallel.

        Args:
            table_name: Name of the table being processed
            chunk_paths: List of chunk file paths
            process_func: Function to process each chunk
            error_callback: Optional callback for handling errors

        Returns:
            ImportStats with results
        """
        stats = ImportStats(
            table_name=table_name, total_chunks=len(chunk_paths), start_time=time.time()
        )
        self.stats[table_name] = stats

        # Create tasks
        tasks = [
            ChunkTask(chunk_id=i, chunk_path=path, table_name=table_name)
            for i, path in enumerate(chunk_paths)
        ]

        logger.info(
            f"Processing {len(tasks)} chunks for {table_name} "
            f"with {self.max_workers} workers"
        )

        # Process in parallel
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._process_single_chunk, task, process_func): task
                for task in tasks
            }

            # Collect results as they complete
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    task.status = "completed"
                    task.end_time = time.time()
                    if isinstance(result, dict):
                        task.rows_processed = result.get("rows", 0)
                    stats.completed_chunks += 1
                    stats.total_rows += task.rows_processed

                except Exception as e:
                    task.status = "failed"
                    task.end_time = time.time()
                    task.error_message = str(e)
                    stats.failed_chunks += 1
                    logger.error(f"Chunk {task.chunk_id} failed: {e}")

                    if error_callback:
                        error_callback(task, e)

                stats.chunk_stats.append(task)

                # Log progress
                if (
                    stats.completed_chunks + stats.failed_chunks
                ) % self.progress_interval == 0:
                    self._log_progress(stats)

        stats.end_time = time.time()
        return stats

    def _process_single_chunk(
        self, task: ChunkTask, process_func: Callable[[Path], Any]
    ) -> Any:
        """Process a single chunk with timing."""
        task.start_time = time.time()
        task.status = "running"
        return process_func(task.chunk_path)

    def _log_progress(self, stats: ImportStats):
        """Log current progress."""
        completed = stats.completed_chunks + stats.failed_chunks
        percent = (
            (completed / stats.total_chunks) * 100 if stats.total_chunks > 0 else 0
        )
        logger.info(
            f"Progress: {completed}/{stats.total_chunks} chunks "
            f"({percent:.1f}%) - {stats.rows_per_second:,.0f} rows/sec"
        )


class ChunkPlanner:
    """
    Plans chunking strategy based on file size and system resources.
    """

    # Chunk size guidelines based on file size
    CHUNK_SIZES = {
        "small": 100_000,  # < 100MB
        "medium": 500_000,  # 100MB - 1GB
        "large": 1_000_000,  # 1GB - 5GB
        "xlarge": 2_000_000,  # > 5GB
    }

    # Parallel workers based on table size
    PARALLEL_WORKERS = {
        "small": 1,
        "medium": 2,
        "large": 4,
        "xlarge": 8,
    }

    def __init__(self):
        self.system_cpus = mp.cpu_count()
        self.available_memory = self._get_available_memory()

    def _get_available_memory(self) -> int:
        """Get available system memory in MB."""
        try:
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        return int(line.split()[1]) // 1024  # Convert to MB
        except:
            pass
        return 4096  # Default to 4GB if can't determine

    def categorize_file(self, file_size_bytes: int) -> str:
        """Categorize file size into size class."""
        size_mb = file_size_bytes / (1024 * 1024)

        if size_mb < 100:
            return "small"
        elif size_mb < 1024:
            return "medium"
        elif size_mb < 5120:
            return "large"
        else:
            return "xlarge"

    def plan_chunks(
        self, file_path: Path, table_name: str, estimated_rows: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a chunking plan for a table.

        Returns dict with:
        - chunk_size: rows per chunk
        - num_chunks: estimated number of chunks
        - parallel_workers: recommended workers
        - category: size category
        """
        file_size = file_path.stat().st_size
        category = self.categorize_file(file_size)

        chunk_size = self.CHUNK_SIZES[category]
        parallel_workers = min(
            self.PARALLEL_WORKERS[category],
            self.system_cpus - 1,  # Leave one CPU free
        )

        # Estimate number of chunks
        if estimated_rows:
            num_chunks = max(1, (estimated_rows + chunk_size - 1) // chunk_size)
        else:
            # Rough estimate: 100 bytes per row average
            estimated_rows = file_size // 100
            num_chunks = max(1, (estimated_rows + chunk_size - 1) // chunk_size)

        return {
            "table_name": table_name,
            "file_path": str(file_path),
            "file_size_mb": file_size / (1024 * 1024),
            "category": category,
            "chunk_size": chunk_size,
            "num_chunks": num_chunks,
            "parallel_workers": parallel_workers,
            "estimated_rows": estimated_rows,
        }

    def plan_all_tables(self, sql_dir: Path, tables: List[str]) -> List[Dict[str, Any]]:
        """Create chunking plans for all tables."""
        plans = []

        for table in tables:
            sql_file = sql_dir / f"{table}.sql"
            if sql_file.exists():
                plan = self.plan_chunks(sql_file, table)
                plans.append(plan)
            else:
                logger.warning(f"SQL file not found: {sql_file}")

        return plans


def retry_with_backoff(
    func: Callable,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Any:
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries (seconds)
        backoff_factor: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry

    Returns:
        Result of func()

    Raises:
        Last exception if all retries fail
    """
    delay = initial_delay
    last_exception: Exception = Exception("No attempts made")

    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(
                    f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.error(f"All {max_retries + 1} attempts failed")

    raise last_exception


def format_bytes(bytes_val: int) -> str:
    """Format bytes as human-readable string."""
    bytes_float = float(bytes_val)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_float < 1024.0:
            return f"{bytes_float:.1f} {unit}"
        bytes_float /= 1024.0
    return f"{bytes_float:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration as human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"
