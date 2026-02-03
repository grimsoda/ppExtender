"""
Shared SQL parsing utilities for parallel chunk loading system.

This module provides memory-efficient SQL parsing for converting
MySQL dump files to CSV format.
"""

import csv
import io
import logging
import re
from typing import Iterator, Optional, Any, Tuple, List
from pathlib import Path

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for performance
INSERT_PATTERN = re.compile(
    r'INSERT\s+INTO\s+[`\'"]?([^`\'"\s]+)[`\'"]?\s*'
    r"(?:\(([^)]+)\))?\s*"
    r"VALUES\s+",
    re.IGNORECASE,
)

VALUES_PATTERN = re.compile(r"VALUES\s+(.+?)(?:;|$)", re.IGNORECASE | re.DOTALL)

TUPLE_PATTERN = re.compile(r"\(([^)]+(?:\([^)]*\)[^)]*)*)\)", re.DOTALL)


class SQLValueParser:
    """Parse SQL values into Python types."""

    @staticmethod
    def parse(value_str: str) -> Any:
        """Parse a single SQL value into Python type."""
        value_str = value_str.strip()

        if not value_str or value_str.upper() == "NULL":
            return None

        # Handle quoted strings
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            quote_char = value_str[0]
            content = value_str[1:-1]
            # Handle escaped quotes (doubled quotes in SQL)
            content = content.replace(quote_char + quote_char, quote_char)
            # Handle backslash escapes
            content = (
                content.replace("\\", "\\").replace("\\'", "'").replace('\\"', '"')
            )
            return content

        # Try integer
        try:
            return int(value_str)
        except ValueError:
            pass

        # Try float
        try:
            return float(value_str)
        except ValueError:
            pass

        return value_str

    @staticmethod
    def to_csv_value(value: Any) -> str:
        """Convert a Python value to CSV string representation."""
        if value is None:
            return "\\N"  # MySQL NULL representation
        if isinstance(value, str):
            # Escape special characters for CSV
            return value.replace("\\", "\\\\").replace('"', '\\"')
        return str(value)


class StreamingSQLParser:
    """
    Memory-efficient streaming parser for MySQL INSERT statements.

    Reads SQL files line by line without loading entire file into memory.
    Yields rows as they are parsed for efficient processing.
    """

    def __init__(self, table_name: Optional[str] = None):
        self.table_name = table_name
        self.columns: List[str] = []
        self._buffer = ""
        self._in_insert = False
        self._current_columns: Optional[List[str]] = None

    def parse_file(self, file_path: str) -> Iterator[Tuple[List[str], List[Any]]]:
        """
        Parse a SQL file and yield (columns, row) tuples.

        Args:
            file_path: Path to SQL dump file

        Yields:
            Tuple of (column_names, row_values)
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")

        logger.info(f"Parsing SQL file: {file_path}")

        with open(file_path, "r", encoding="utf-8", buffering=1024 * 1024 * 8) as f:
            for line in f:
                yield from self._parse_line(line)

        # Process any remaining buffer
        if self._buffer.strip():
            yield from self._parse_buffer(self._buffer, force=True)

    def _parse_line(self, line: str) -> Iterator[Tuple[List[str], List[Any]]]:
        """Parse a single line, yielding any complete rows found."""
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("--") or line.startswith("/*"):
            return

        # Check for INSERT statement
        if "INSERT" in line.upper():
            # Check if this is for our target table
            match = INSERT_PATTERN.search(line)
            if match:
                parsed_table = match.group(1)
                if (
                    self.table_name is None
                    or parsed_table.lower() == self.table_name.lower()
                ):
                    self._in_insert = True
                    # Extract column names if present
                    if match.group(2):
                        self._current_columns = [
                            c.strip().strip("`\"'") for c in match.group(2).split(",")
                        ]
                    else:
                        self._current_columns = None

                    # Start buffering from VALUES clause
                    values_match = VALUES_PATTERN.search(line)
                    if values_match:
                        self._buffer = values_match.group(1)
                    else:
                        self._buffer = line[line.upper().find("VALUES") + 6 :]
                    return

        if not self._in_insert:
            return

        # Check for end of INSERT statement
        if ";" in line:
            parts = line.split(";", 1)
            self._buffer += " " + parts[0]
            yield from self._parse_buffer(self._buffer)
            self._buffer = ""
            self._in_insert = False
            if len(parts) > 1 and "INSERT" in parts[1].upper():
                # Another INSERT starts in this line
                self._buffer = parts[1]
        else:
            self._buffer += " " + line

    def _parse_buffer(
        self, buffer: str, force: bool = False
    ) -> Iterator[Tuple[List[str], List[Any]]]:
        """Parse the buffer and extract complete tuples."""
        # Find all complete tuples in the buffer
        tuples = TUPLE_PATTERN.findall(buffer)

        for tuple_str in tuples:
            fields = self._split_tuple(tuple_str)
            row = [SQLValueParser.parse(f) for f in fields]
            columns = self._current_columns or [f"col_{i}" for i in range(len(row))]
            yield (columns, row)

    def _split_tuple(self, tuple_str: str) -> List[str]:
        """
        Split a tuple string into individual field values.

        Handles quoted strings, nested parentheses, and commas within values.
        """
        fields = []
        current_field = ""
        in_string = False
        string_char = None
        paren_depth = 0

        for char in tuple_str:
            if char in ("'", '"') and not in_string:
                in_string = True
                string_char = char
                current_field += char
            elif char == string_char and in_string:
                # Check for escaped quote (doubled quote)
                if len(current_field) > 0 and current_field[-1] == string_char:
                    # This is an escaped quote, keep it
                    current_field += char
                elif len(current_field) > 0 and current_field[-1] == "\\":
                    # Backslash-escaped quote
                    current_field += char
                else:
                    # End of string
                    current_field += char
                    in_string = False
                    string_char = None
            elif char == "(" and not in_string:
                paren_depth += 1
                current_field += char
            elif char == ")" and not in_string:
                paren_depth -= 1
                current_field += char
            elif char == "," and not in_string and paren_depth == 0:
                fields.append(current_field.strip())
                current_field = ""
            else:
                current_field += char

        if current_field.strip():
            fields.append(current_field.strip())

        return fields


class CSVChunkWriter:
    """
    Write rows to CSV files in chunks.

    Automatically splits output into multiple files based on row count.
    """

    def __init__(
        self,
        output_dir: Path,
        table_name: str,
        chunk_size: int = 1_000_000,
        columns: Optional[List[str]] = None,
    ):
        self.output_dir = Path(output_dir)
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.columns = columns
        self.current_chunk = 0
        self.current_row_count = 0
        self.total_rows = 0
        self._current_file: Optional[io.TextIOWrapper] = None
        self._csv_writer: Optional[Any] = None

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_chunk_path(self, chunk_num: int) -> Path:
        """Get the file path for a specific chunk."""
        return self.output_dir / f"{self.table_name}_chunk_{chunk_num:04d}.csv"

    def _start_new_chunk(self):
        """Start writing to a new chunk file."""
        if self._current_file:
            self._current_file.close()

        chunk_path = self._get_chunk_path(self.current_chunk)
        logger.info(f"Starting new chunk: {chunk_path}")

        self._current_file = open(chunk_path, "w", newline="", encoding="utf-8")
        self._csv_writer = csv.writer(
            self._current_file,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar="\\",
        )

        # Write header if columns are known
        if self.columns:
            self._csv_writer.writerow(self.columns)

    def write_row(self, row: List[Any]):
        """Write a single row to the current chunk."""
        if self._current_file is None or self.current_row_count >= self.chunk_size:
            self._start_new_chunk()

        if self._csv_writer is None:
            raise RuntimeError("CSV writer not initialized")

        # Convert values to CSV-safe format
        csv_row = [SQLValueParser.to_csv_value(v) for v in row]
        self._csv_writer.writerow(csv_row)

        self.current_row_count += 1
        self.total_rows += 1

    def write_rows(self, rows: List[List[Any]]):
        """Write multiple rows efficiently."""
        for row in rows:
            self.write_row(row)

    def close(self):
        """Close the current chunk file."""
        if self._current_file:
            self._current_file.close()
            self._current_file = None

    def get_chunk_files(self) -> List[Path]:
        """Get list of all chunk files created."""
        return sorted(self.output_dir.glob(f"{self.table_name}_chunk_*.csv"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def extract_create_table(sql_file: str, table_name: str) -> Optional[str]:
    """
    Extract CREATE TABLE statement from SQL dump file.

    Args:
        sql_file: Path to SQL dump file
        table_name: Name of table to extract

    Returns:
        CREATE TABLE statement as string, or None if not found
    """
    pattern = re.compile(
        rf'CREATE\s+TABLE\s+[`\'"]?{re.escape(table_name)}[`\'"]?\s*\(.*?\)\s*ENGINE\s*=.*?;',
        re.IGNORECASE | re.DOTALL,
    )

    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()
        match = pattern.search(content)
        if match:
            return match.group(0)

    return None


def estimate_row_count(sql_file: str, table_name: str) -> int:
    """
    Estimate the number of rows in a SQL dump file.

    This is a rough estimate based on file size and average row size.
    """
    path = Path(sql_file)
    if not path.exists():
        return 0

    file_size = path.stat().st_size

    # Sample first few INSERT statements to estimate average row size
    sample_size = min(file_size, 1024 * 1024)  # 1MB sample
    sample_rows = 0
    sample_bytes = 0

    parser = StreamingSQLParser(table_name)
    with open(sql_file, "r", encoding="utf-8") as f:
        for line in f:
            for _, row in parser._parse_line(line):
                sample_rows += 1
                sample_bytes += len(str(row))
                if sample_bytes >= sample_size:
                    break
            if sample_bytes >= sample_size:
                break

    if sample_rows == 0:
        return 0

    avg_row_size = sample_bytes / sample_rows
    estimated_rows = int(file_size / avg_row_size)

    return estimated_rows


def get_table_names_from_sql(sql_file: str) -> List[str]:
    """Extract all table names from a SQL dump file."""
    tables = set()

    with open(sql_file, "r", encoding="utf-8") as f:
        for line in f:
            match = INSERT_PATTERN.search(line)
            if match:
                tables.add(match.group(1))

    return sorted(list(tables))
