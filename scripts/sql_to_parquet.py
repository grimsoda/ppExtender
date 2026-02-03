#!/usr/bin/env python3
"""
Direct SQL to Parquet Converter

Parses MySQL SQL dump files and writes directly to Parquet format.
Optimized for large files with streaming processing.
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Iterator, Optional, List, Any, Dict
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SQLParser:
    """Streaming SQL parser for MySQL dumps."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.columns: List[str] = []
        self.schema: Optional[pa.Schema] = None

    def extract_schema(self, line: str) -> bool:
        """Extract column names from CREATE TABLE statement."""
        if "CREATE TABLE" not in line.upper():
            return False

        # Match column definitions
        pattern = rf"CREATE TABLE.*?`{self.table_name}`.*?\((.*?)\)"
        match = re.search(pattern, line, re.DOTALL | re.IGNORECASE)
        if not match:
            return False

        content = match.group(1)
        # Extract column names (lines starting with `column_name`)
        col_pattern = r"`(\w+)`\s+(\w+)"
        columns = []
        for col_match in re.finditer(col_pattern, content):
            col_name = col_match.group(1)
            col_type = col_match.group(2).lower()
            columns.append((col_name, col_type))

        if columns:
            self.columns = [c[0] for c in columns]
            # Add derived columns
            self.columns.extend(["mods_key", "speed_mod"])
            logger.info(f"  Detected {len(columns)} columns: {self.columns[:5]}...")
            return True
        return False

    def parse_values(self, values_section: str) -> Iterator[List[Any]]:
        """Parse VALUES section and yield rows."""
        # Pattern to match tuples: (val1, val2, ...)
        # Handles nested parentheses in JSON
        depth = 0
        current_tuple = []
        current_value = []
        in_string = False
        string_char = None

        i = 0
        while i < len(values_section):
            char = values_section[i]

            if char in ("'", '"'):
                if not in_string:
                    in_string = True
                    string_char = char
                    current_value.append(char)
                elif char == string_char:
                    # Check for escaped quote
                    if i > 0 and values_section[i - 1] == "\\":
                        current_value.append(char)
                    else:
                        current_value.append(char)
                        in_string = False
                        string_char = None
                else:
                    current_value.append(char)
            elif char == "(" and not in_string:
                if depth == 0:
                    # Start of tuple
                    current_tuple = []
                    current_value = []
                else:
                    current_value.append(char)
                depth += 1
            elif char == ")" and not in_string:
                depth -= 1
                if depth == 0:
                    # End of tuple
                    if current_value:
                        current_tuple.append("".join(current_value))
                    yield self.parse_row(current_tuple)
                    current_tuple = []
                    current_value = []
                else:
                    current_value.append(char)
            elif char == "," and not in_string and depth == 1:
                # Field separator (only at depth 1)
                if current_value:
                    current_tuple.append("".join(current_value))
                current_value = []
            else:
                current_value.append(char)

            i += 1

    def parse_row(self, fields: List[str]) -> List[Any]:
        """Parse a single row's fields."""
        parsed = []
        for field in fields:
            field = field.strip()

            if not field or field.upper() == "NULL":
                parsed.append(None)
            elif field.startswith("'") and field.endswith("'"):
                # String
                parsed.append(field[1:-1].replace("''", "'").replace("\\'", "'"))
            elif field.startswith('"') and field.endswith('"'):
                # Double-quoted string
                parsed.append(field[1:-1].replace('""', '"').replace('\\"', '"'))
            else:
                # Try numeric
                try:
                    if "." in field or "e" in field.lower():
                        parsed.append(float(field))
                    else:
                        parsed.append(int(field))
                except ValueError:
                    parsed.append(field)

        # Extract mods from data column (assuming it's the 'data' column)
        if "data" in self.columns:
            data_idx = self.columns.index("data")
            if data_idx < len(parsed):
                data_val = parsed[data_idx]
                mods_key, speed_mod = self.extract_mods(data_val)
                parsed.extend([mods_key, speed_mod])

        return parsed

    def extract_mods(self, data_str: Optional[str]) -> tuple:
        """Extract mod info from data JSON."""
        if not data_str:
            return ("", None)

        try:
            data = json.loads(data_str)
            mods = data.get("mods", [])
            acronyms = sorted([m["acronym"] for m in mods if "acronym" in m])
            mods_key = ",".join(acronyms)

            speed_mod = None
            if "DT" in acronyms or "NC" in acronyms:
                speed_mod = "DT"
            elif "HT" in acronyms:
                speed_mod = "HT"

            return (mods_key, speed_mod)
        except (json.JSONDecodeError, TypeError):
            return ("", None)


def convert_sql_to_parquet(
    sql_file: Path, output_dir: Path, table_name: str, chunk_size: int = 100000
) -> Dict[str, Any]:
    """Convert SQL dump to Parquet files."""

    logger.info(f"Converting {table_name}...")
    logger.info(f"  Input: {sql_file}")
    logger.info(f"  Chunk size: {chunk_size:,} rows")

    output_dir.mkdir(parents=True, exist_ok=True)

    parser = SQLParser(table_name)
    batch_rows = []
    total_rows = 0
    chunk_num = 0
    start_time = time.time()

    # Detect schema from first part of file
    with open(sql_file, "r", encoding="utf-8", errors="replace") as f:
        header = f.read(50000)  # Read first 50KB
        for line in header.split("\n"):
            if parser.extract_schema(line):
                break

    if not parser.columns:
        logger.warning(f"  Could not detect schema for {table_name}")
        # Use generic column names
        parser.columns = [f"col_{i}" for i in range(20)]

    # Determine Arrow schema
    arrow_fields = []
    for col in parser.columns:
        if col in ["mods_key", "speed_mod"]:
            arrow_fields.append(pa.field(col, pa.string()))
        elif col == "data":
            arrow_fields.append(pa.field(col, pa.string()))
        else:
            arrow_fields.append(pa.field(col, pa.string()))  # Default to string

    schema = pa.schema(arrow_fields)

    # Parse file
    with open(sql_file, "r", encoding="utf-8", errors="replace") as f:
        for line_num, line in enumerate(f, 1):
            # Look for INSERT statements
            if "INSERT INTO" not in line.upper():
                continue

            if table_name.upper() not in line.upper():
                continue

            # Extract VALUES section
            match = re.search(r"VALUES\s+(.+);?$", line, re.DOTALL)
            if not match:
                continue

            values_section = match.group(1)

            # Parse each row
            for row in parser.parse_values(values_section):
                batch_rows.append(row)
                total_rows += 1

                # Write chunk when full
                if len(batch_rows) >= chunk_size:
                    write_parquet_chunk(
                        batch_rows, schema, output_dir, table_name, chunk_num
                    )
                    chunk_num += 1
                    batch_rows = []

                    if total_rows % 100000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_rows / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"  Progress: {total_rows:,} rows ({elapsed:.1f}s, {rate:,.0f}/sec)"
                        )

    # Write final chunk
    if batch_rows:
        write_parquet_chunk(batch_rows, schema, output_dir, table_name, chunk_num)
        chunk_num += 1

    elapsed = time.time() - start_time
    rate = total_rows / elapsed if elapsed > 0 else 0

    logger.info(
        f"  Completed: {total_rows:,} rows in {chunk_num} chunks ({elapsed:.1f}s, {rate:,.0f}/sec)"
    )

    return {
        "table": table_name,
        "rows": total_rows,
        "chunks": chunk_num,
        "time": elapsed,
        "rate": rate,
    }


def write_parquet_chunk(
    rows: List[List[Any]],
    schema: pa.Schema,
    output_dir: Path,
    table_name: str,
    chunk_num: int,
):
    """Write a batch of rows to a Parquet file."""

    # Convert rows to columnar format
    columns = {field.name: [] for field in schema}

    for row in rows:
        for i, field in enumerate(schema):
            if i < len(row):
                columns[field.name].append(row[i])
            else:
                columns[field.name].append(None)

    # Create PyArrow table
    arrays = []
    for field in schema:
        arr = pa.array(columns[field.name], type=field.type)
        arrays.append(arr)

    table = pa.Table.from_arrays(arrays, names=[f.name for f in schema])

    # Write to Parquet
    output_file = output_dir / f"{table_name}_chunk_{chunk_num:04d}.parquet"
    pq.write_table(table, output_file, compression="snappy", row_group_size=10000)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Convert SQL dumps to Parquet")
    parser.add_argument("--table", required=True, help="Table name to convert")
    parser.add_argument(
        "--input-dir",
        default="data/ingest/2026-02/sql",
        help="Input directory containing SQL files",
    )
    parser.add_argument(
        "--output-dir",
        default="data/parquet",
        help="Output directory for Parquet files",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=100000, help="Rows per Parquet file"
    )

    args = parser.parse_args()

    sql_file = Path(args.input_dir) / f"{args.table}.sql"
    output_dir = Path(args.output_dir) / args.table

    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        sys.exit(1)

    result = convert_sql_to_parquet(sql_file, output_dir, args.table, args.chunk_size)

    print(f"\nConversion complete:")
    print(f"  Table: {result['table']}")
    print(f"  Rows: {result['rows']:,}")
    print(f"  Chunks: {result['chunks']}")
    print(f"  Time: {result['time']:.1f}s")
    print(f"  Rate: {result['rate']:,.0f} rows/sec")


if __name__ == "__main__":
    main()
