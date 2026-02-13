"""Parquet writer for converting Arrow RecordBatches to Parquet files."""

import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterator, Optional, Union, List
import pyarrow as pa
from pyarrow import parquet as pq


class ParquetWriter:
    """
    Write Arrow RecordBatches to Parquet files with sharding.

    Creates files like:
        part-000000.parquet
        part-000001.parquet
        ...
    """

    def __init__(
        self,
        output_dir: Union[str, Path],
        table_name: str,
        batch_rows: int = 100000,
        row_group_rows: int = 500000,
        file_rows: int = 2000000,
        compression: str = "snappy",
        schema: Optional[pa.Schema] = None,
    ):
        self.output_dir = Path(output_dir)
        self.table_name = table_name
        self.batch_rows = batch_rows
        self.row_group_rows = row_group_rows
        self.file_rows = file_rows
        self.compression = compression
        self.schema = schema

        self.current_file_index = 0
        self.current_row_count = 0
        self.total_row_count = 0
        self.file_info = []
        self.current_writer = None
        self.current_file_path = None
        self._buffered_batches: List[pa.RecordBatch] = []
        self._buffered_rows = 0

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_batch(self, batch: pa.RecordBatch) -> None:
        """Write a RecordBatch to Parquet."""
        if batch.num_rows == 0:
            return

        # Extract schema from first batch if not provided
        if self.schema is None:
            self.schema = batch.schema

        # Add to buffer
        self._buffered_batches.append(batch)
        self._buffered_rows += batch.num_rows
        self.total_row_count += batch.num_rows

        # Process buffer if it exceeds batch_rows or we need to start a new file
        while self._buffered_rows > 0:
            # Check if we need to start a new file
            if self.current_writer is None:
                self._start_new_file()

            # Check if adding all buffered rows would exceed file_rows
            if self.current_row_count + self._buffered_rows > self.file_rows:
                # Fill up current file to file_rows limit
                rows_needed = self.file_rows - self.current_row_count
                if rows_needed > 0 and self._buffered_rows > 0:
                    self._write_rows_to_current_file(rows_needed)
                # Close current file and start new one
                self._close_current_file()
                continue
            else:
                # Write all buffered rows to current file
                self._write_all_buffered_rows()
                break

    def _write_rows_to_current_file(self, num_rows: int) -> None:
        """Write specified number of rows from buffer to current file."""
        if num_rows <= 0 or not self._buffered_batches:
            return

        rows_to_write = []
        rows_remaining = num_rows

        while rows_remaining > 0 and self._buffered_batches:
            batch = self._buffered_batches[0]
            if batch.num_rows <= rows_remaining:
                rows_to_write.append(batch)
                rows_remaining -= batch.num_rows
                self._buffered_rows -= batch.num_rows
                self._buffered_batches.pop(0)
            else:
                # Split the batch
                first_part = batch.slice(0, rows_remaining)
                second_part = batch.slice(rows_remaining)
                rows_to_write.append(first_part)
                self._buffered_batches[0] = second_part
                self._buffered_rows -= rows_remaining
                rows_remaining = 0

        if rows_to_write:
            table = pa.Table.from_batches(rows_to_write)
            self._write_table_to_file(table)

    def _write_all_buffered_rows(self) -> None:
        """Write all buffered rows to current file."""
        if not self._buffered_batches:
            return

        table = pa.Table.from_batches(self._buffered_batches)
        self._write_table_to_file(table)
        self._buffered_rows = 0
        self._buffered_batches = []

    def _write_table_to_file(self, table: pa.Table) -> None:
        """Write a table to the current Parquet file."""
        if self.current_writer is None:
            self._start_new_file()

        # Write table in chunks respecting row_group_rows
        total_rows = table.num_rows
        rows_written = 0

        while rows_written < total_rows:
            chunk_size = min(self.row_group_rows, total_rows - rows_written)
            chunk = table.slice(rows_written, chunk_size)
            self.current_writer.write_table(chunk)
            rows_written += chunk_size
            self.current_row_count += chunk_size

    def _start_new_file(self) -> None:
        """Start writing to a new Parquet file."""
        # Close current writer if exists
        if self.current_writer is not None:
            self._close_current_file()

        # Create new file path: part-{index:06d}.parquet
        self.current_file_path = (
            self.output_dir / f"part-{self.current_file_index:06d}.parquet"
        )
        self.current_file_index += 1
        self.current_row_count = 0

        # Determine compression
        if self.compression == "snappy":
            compression_codec = "snappy"
        else:
            compression_codec = "none"

        # Create new ParquetWriter
        self.current_writer = pq.ParquetWriter(
            self.current_file_path,
            schema=self.schema,
            compression=compression_codec,
            use_dictionary=True,
            write_statistics=True,
        )

    def _close_current_file(self) -> None:
        """Close current file and record its info."""
        if self.current_writer is None:
            return

        # Close writer
        self.current_writer.close()
        self.current_writer = None

        # Calculate file size and hash
        file_size = self.current_file_path.stat().st_size
        file_hash = self._compute_hash(self.current_file_path)

        # Add to file_info list
        self.file_info.append(
            {
                "file": self.current_file_path.name,
                "rows": self.current_row_count,
                "size_bytes": file_size,
                "hash": file_hash,
            }
        )

    def finalize(self) -> dict:
        """
        Finalize writing and generate manifest.

        Returns:
            Manifest dictionary
        """
        # Write any remaining buffered rows
        if self._buffered_batches:
            if self.current_writer is None:
                self._start_new_file()
            self._write_all_buffered_rows()

        # Close current file
        if self.current_writer is not None:
            self._close_current_file()

        # Generate manifest
        manifest = self._generate_manifest()

        # Write manifest.json
        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return manifest

    def _generate_manifest(self) -> dict:
        """Generate manifest dictionary."""
        # Build schema info
        schema_info = {"fields": []}

        # Get schema from first file if available
        if self.file_info:
            first_file = self.output_dir / self.file_info[0]["file"]
            try:
                parquet_file = pq.ParquetFile(first_file)
                arrow_schema = parquet_file.schema_arrow
                for field in arrow_schema:
                    schema_info["fields"].append(
                        {
                            "name": field.name,
                            "type": str(field.type),
                        }
                    )
            except Exception:
                pass
        elif self.schema:
            for field in self.schema:
                schema_info["fields"].append(
                    {
                        "name": field.name,
                        "type": str(field.type),
                    }
                )

        manifest = {
            "table_name": self.table_name,
            "version": "1.0",
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "files": self.file_info,
            "total_rows": self.total_row_count,
            "schema": schema_info,
        }

        return manifest

    def _compute_hash(self, file_path: Path) -> str:
        """Compute SHA256 hash of file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return f"sha256:{sha256.hexdigest()}"


def write_parquet_batches(
    batches: Iterator[pa.RecordBatch],
    output_dir: Union[str, Path],
    table_name: str,
    batch_rows: int = 100000,
    row_group_rows: int = 500000,
    file_rows: int = 2000000,
    compression: str = "snappy",
    schema: Optional[pa.Schema] = None,
) -> dict:
    """
    Convenience function to write multiple batches to Parquet.

    Args:
        batches: Iterator of RecordBatch objects
        output_dir: Output directory path
        table_name: Name of the table
        batch_rows: Rows per internal buffer
        row_group_rows: Rows per Parquet row group
        file_rows: Max rows per file
        compression: Compression codec ('snappy' or 'none')
        schema: Optional Arrow schema

    Returns:
        Manifest dictionary
    """
    writer = ParquetWriter(
        output_dir=output_dir,
        table_name=table_name,
        batch_rows=batch_rows,
        row_group_rows=row_group_rows,
        file_rows=file_rows,
        compression=compression,
        schema=schema,
    )

    for batch in batches:
        writer.write_batch(batch)

    return writer.finalize()
