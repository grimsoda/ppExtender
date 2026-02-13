"""Fast SQL parser using regex for bulk INSERT statements."""

import json
import logging
import re
import time
from pathlib import Path
from typing import Iterator, Optional, Any
import pyarrow as pa

logger = logging.getLogger(__name__)

# Pre-compile regex patterns for performance
VALUES_PATTERN = re.compile(
    r'INSERT\s+INTO\s+[`\'"]?\w+[`\'"]?\s+(?:\([^)]+\)\s+)?VALUES\s+(.+?)(?:;|$)',
    re.IGNORECASE | re.DOTALL,
)

TUPLE_PATTERN = re.compile(r"\(([^)]+(?:\([^)]*\)[^)]*)*)\)", re.DOTALL)


def parse_value_fast(value_str: str) -> Any:
    """Parse a single SQL value quickly."""
    value_str = value_str.strip()

    if not value_str or value_str.upper() == "NULL":
        return None

    # Handle quoted strings
    if (value_str.startswith("'") and value_str.endswith("'")) or (
        value_str.startswith('"') and value_str.endswith('"')
    ):
        quote_char = value_str[0]
        content = value_str[1:-1]
        # Handle escaped quotes
        content = content.replace(quote_char + quote_char, quote_char)
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


def parse_mods_from_data(data_str: Optional[str]) -> list:
    """Extract mods array from data JSON string."""
    if data_str is None or data_str == "NULL":
        return []
    try:
        data = json.loads(data_str)
        return data.get("mods", [])
    except (json.JSONDecodeError, TypeError):
        return []


def normalize_mods(mods: list) -> tuple:
    """Normalize mods list. Returns (mods_key, speed_mod)."""
    if not mods:
        return ("", None)

    acronyms = [mod["acronym"] for mod in mods if "acronym" in mod]
    acronyms.sort()
    mods_key = ",".join(acronyms)

    speed_mod = None
    if "DT" in acronyms or "NC" in acronyms:
        speed_mod = "DT"
    elif "HT" in acronyms:
        speed_mod = "HT"

    return (mods_key, speed_mod)


def _rows_to_batch(rows: list, columns: list) -> pa.RecordBatch:
    """Convert list of rows to PyArrow RecordBatch."""
    if not rows:
        return pa.RecordBatch.from_pydict({})

    # Transpose rows to columns
    data = {col: [] for col in columns}

    for row in rows:
        for i, col in enumerate(columns):
            if i < len(row):
                data[col].append(row[i])
            else:
                data[col].append(None)

    return pa.RecordBatch.from_pydict(data)


def parse_sql_file_fast(
    file_path: str,
    table_name: str,
    batch_size: int = 100000,
    columns: Optional[list] = None,
) -> Iterator[pa.RecordBatch]:
    """
    Fast SQL parser using regex for bulk INSERT statements.

    This is ~100x faster than character-by-character parsing for bulk inserts.
    """
    batch_rows = []
    total_rows = 0
    last_log_rows = 0
    start_time = time.time()

    # Track columns from first INSERT
    detected_columns = columns

    with open(file_path, "r", encoding="utf-8", buffering=1024 * 1024 * 8) as f:
        line_num = 0
        for line in f:
            line_num += 1

            # Skip non-INSERT lines quickly
            if "INSERT" not in line.upper() or table_name.upper() not in line.upper():
                continue

            # Extract VALUES section using regex
            match = VALUES_PATTERN.search(line)
            if not match:
                continue

            values_section = match.group(1)

            # Extract column names if present and not already detected
            if detected_columns is None:
                col_match = re.search(r"\(([^)]+)\)\s*VALUES", line, re.IGNORECASE)
                if col_match:
                    col_str = col_match.group(1)
                    detected_columns = [
                        c.strip().strip("`\"'") for c in col_str.split(",")
                    ]
                    # Add derived columns
                    detected_columns.extend(["mods_key", "speed_mod"])

            # Find all tuples in VALUES section
            tuples = TUPLE_PATTERN.findall(values_section)

            for tuple_str in tuples:
                # Split by comma, but be careful with commas inside strings and JSON
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
                        # Check for escaped quote
                        if len(current_field) > 0 and current_field[-1] == "\\":
                            current_field += char
                        else:
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

                # Parse fields
                parsed_row = [parse_value_fast(f) for f in fields]

                # Extract mods from data column (assuming it's the last column before mods_key)
                if len(parsed_row) >= 4:
                    data_idx = -3  # Assuming data is 3rd from last
                    data_val = parsed_row[data_idx]
                    mods = parse_mods_from_data(data_val)
                    mods_key, speed_mod = normalize_mods(mods)
                    parsed_row.extend([mods_key, speed_mod])

                batch_rows.append(parsed_row)
                total_rows += 1

                # Log progress
                if total_rows - last_log_rows >= 100000:
                    elapsed = time.time() - start_time
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"  Progress: {total_rows:,} rows ({elapsed:.1f}s, ~{rate:,.0f} rows/sec)"
                    )
                    last_log_rows = total_rows

                # Yield batch when full
                if len(batch_rows) >= batch_size:
                    yield _rows_to_batch(
                        batch_rows,
                        detected_columns
                        or [f"col_{i}" for i in range(len(batch_rows[0]))],
                    )
                    batch_rows = []

    # Yield remaining rows
    if batch_rows:
        yield _rows_to_batch(
            batch_rows,
            detected_columns or [f"col_{i}" for i in range(len(batch_rows[0]))],
        )

    # Final stats
    elapsed = time.time() - start_time
    rate = total_rows / elapsed if elapsed > 0 else 0
    logger.info(
        f"  Completed: {total_rows:,} rows in {elapsed:.1f}s (~{rate:,.0f} rows/sec)"
    )
