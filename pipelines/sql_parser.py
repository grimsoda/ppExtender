"""Streaming SQL parser for osu! scores data."""

import json
import re
from enum import Enum, auto
from pathlib import Path
from typing import Iterator, Optional, Any
import pyarrow as pa


class ParserState(Enum):
    SEARCH_INSERT = auto()
    READ_VALUES = auto()
    READ_ROW = auto()
    READ_FIELD = auto()


class SqlParser:
    """State machine parser for SQL INSERT statements."""

    def __init__(self, table_name: str = "scores"):
        self.table_name = table_name
        self._state = ParserState.SEARCH_INSERT
        self.columns = []
        self.current_row = []
        self.rows = []
        self.buffer = ""
        self.in_string = False
        self.string_delimiter = None
        self.escape_next = False
        self.paren_depth = 0
        self.found_table = False
        self.insert_buffer = ""
        self.row_started = False

    @property
    def state(self):
        """Return state as string for test compatibility."""
        return self._state.name if isinstance(self._state, ParserState) else self._state

    @state.setter
    def state(self, value):
        self._state = value

    def feed(self, text: str):
        """Feed text into the parser state machine."""
        text = self._remove_comments(text)

        i = 0
        while i < len(text):
            char = text[i]

            if self._state == ParserState.SEARCH_INSERT:
                self.insert_buffer += char
                upper_buffer = self.insert_buffer.upper()
                if "INSERT" in upper_buffer and "INTO" in upper_buffer:
                    pattern = (
                        rf"INSERT\s+INTO\s+[`\"']?{re.escape(self.table_name)}[`\"']?"
                    )
                    if re.search(pattern, self.insert_buffer, re.IGNORECASE):
                        self.found_table = True
                        # Extract column names if present
                        if "VALUES" in upper_buffer:
                            col_match = re.search(
                                r"\(([^)]+)\)\s*VALUES",
                                self.insert_buffer,
                                re.IGNORECASE,
                            )
                            if col_match:
                                col_str = col_match.group(1)
                                self.columns = [
                                    c.strip().strip("`\"'") for c in col_str.split(",")
                                ]
                            self._state = ParserState.READ_VALUES
                            self.insert_buffer = ""
                    elif "VALUES" in upper_buffer:
                        self.insert_buffer = ""

            elif self._state == ParserState.READ_VALUES:
                if char == "(":
                    self._state = ParserState.READ_ROW
                    self.paren_depth = 1
                    self.buffer = ""
                    self.current_row = []
                    self.row_started = True
                elif char == ";":
                    self._state = ParserState.SEARCH_INSERT
                    self.insert_buffer = ""
                    self.found_table = False
                    self.row_started = False

            elif self._state == ParserState.READ_ROW:
                if char == "'" and not self.in_string:
                    self.in_string = True
                    self.string_delimiter = "'"
                    self._state = ParserState.READ_FIELD
                    self.buffer += char
                elif char == '"' and not self.in_string:
                    self.in_string = True
                    self.string_delimiter = '"'
                    self._state = ParserState.READ_FIELD
                    self.buffer += char
                elif char == "(" and not self.in_string:
                    self.paren_depth += 1
                    if self.paren_depth == 1:
                        # Starting a new row
                        self.buffer = ""
                        self.current_row = []
                    else:
                        self.buffer += char
                    self._state = ParserState.READ_FIELD
                elif char == ")" and not self.in_string:
                    self.paren_depth -= 1
                    if self.paren_depth == 0:
                        field = self.buffer.strip()
                        if field:
                            self.current_row.append(field)
                        self.buffer = ""
                        # Stay in READ_ROW until semicolon or next VALUES
                        # But store the completed row
                        if self.current_row:
                            self.rows.append(self.current_row[:])
                            self.current_row = []
                    else:
                        self.buffer += char
                        self._state = ParserState.READ_FIELD
                elif char == "," and not self.in_string and self.paren_depth == 1:
                    field = self.buffer.strip()
                    if field:
                        self.current_row.append(field)
                    self.buffer = ""
                    # Transition to READ_FIELD for the next field
                    self._state = ParserState.READ_FIELD
                elif char == ";" and not self.in_string and self.paren_depth == 0:
                    self._state = ParserState.SEARCH_INSERT
                    self.insert_buffer = ""
                    self.found_table = False
                    self.row_started = False
                elif char.strip():
                    if self.paren_depth == 0:
                        # We're between rows, ignore non-() chars until next row starts
                        pass
                    else:
                        self._state = ParserState.READ_FIELD
                        self.buffer += char

            elif self._state == ParserState.READ_FIELD:
                if char == "'" and self.string_delimiter is None and not self.in_string:
                    # Opening single quote
                    self.in_string = True
                    self.string_delimiter = "'"
                    self.buffer += char
                elif (
                    char == '"' and self.string_delimiter is None and not self.in_string
                ):
                    # Opening double quote
                    self.in_string = True
                    self.string_delimiter = '"'
                    self.buffer += char
                elif char == "'" and self.string_delimiter == "'":
                    self.buffer += char
                    if i + 1 < len(text) and text[i + 1] == "'":
                        i += 1
                        self.buffer += text[i]
                    else:
                        self.in_string = False
                        self.string_delimiter = None
                        self._state = ParserState.READ_ROW
                elif char == '"' and self.string_delimiter == '"':
                    self.buffer += char
                    if i + 1 < len(text) and text[i + 1] == '"':
                        i += 1
                        self.buffer += text[i]
                    else:
                        self.in_string = False
                        self.string_delimiter = None
                        self._state = ParserState.READ_ROW
                elif char == "(" and not self.in_string:
                    self.paren_depth += 1
                    self.buffer += char
                elif char == ")" and not self.in_string:
                    self.paren_depth -= 1
                    if self.paren_depth == 0:
                        field = self.buffer.strip()
                        if field:
                            self.current_row.append(field)
                        self.buffer = ""
                        # Go back to READ_ROW (not READ_VALUES)
                        self._state = ParserState.READ_ROW
                        if self.current_row:
                            self.rows.append(self.current_row[:])
                            self.current_row = []
                    else:
                        self.buffer += char
                        self._state = ParserState.READ_ROW
                elif char == "," and not self.in_string and self.paren_depth == 1:
                    field = self.buffer.strip()
                    if field:
                        self.current_row.append(field)
                    self.buffer = ""
                    # Stay in READ_FIELD for the next field
                elif char == ";" and not self.in_string and self.paren_depth == 0:
                    self._state = ParserState.SEARCH_INSERT
                    self.insert_buffer = ""
                    self.found_table = False
                    self.row_started = False
                else:
                    self.buffer += char

            i += 1

    def _remove_comments(self, text: str) -> str:
        """Remove SQL comments from text."""
        lines = text.split("\n")
        result_lines = []
        for line in lines:
            comment_idx = line.find("--")
            if comment_idx >= 0:
                line = line[:comment_idx]
            result_lines.append(line)

        result = "\n".join(result_lines)
        while "/*" in result and "*/" in result:
            start = result.find("/*")
            end = result.find("*/", start) + 2
            result = result[:start] + " " + result[end:]

        return result

    def parse_line(self, line: str) -> Optional[list]:
        """Parse a single line, return completed row if found."""
        self.feed(line)
        if self.rows:
            return self.rows.pop(0)
        return None

    def parse_value(self, value_str: str) -> Any:
        """Parse a field value string into Python type."""
        value_str = value_str.strip()

        if not value_str or value_str.upper() == "NULL":
            return None

        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            quote_char = value_str[0]
            content = value_str[1:-1]
            content = content.replace(quote_char + quote_char, quote_char)
            return content

        try:
            return int(value_str)
        except ValueError:
            pass

        try:
            return float(value_str)
        except ValueError:
            pass

        return value_str

    def get_all_rows(self) -> list:
        """Get all parsed rows."""
        return self.rows


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
    """
    Normalize mods list.
    Returns: (mods_key, speed_mod)
    - mods_key: comma-separated sorted acronyms
    - speed_mod: 'DT', 'HT', or None
    """
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


def _infer_column_types(rows: list, columns: list) -> dict:
    """Infer PyArrow types for each column based on data."""
    type_map = {}

    for col_idx, col_name in enumerate(columns):
        type_map[col_name] = pa.string()

        for row in rows:
            if col_idx < len(row):
                val = row[col_idx]
                if val is not None and val != "NULL":
                    parser = SqlParser()
                    parsed = parser.parse_value(val)
                    if isinstance(parsed, bool):
                        type_map[col_name] = pa.bool_()
                    elif isinstance(parsed, int):
                        type_map[col_name] = pa.int64()
                    elif isinstance(parsed, float):
                        type_map[col_name] = pa.float64()
                    else:
                        type_map[col_name] = pa.string()
                    break

    return type_map


def _rows_to_batch(rows: list, columns: Optional[list] = None) -> pa.RecordBatch:
    """Convert list of rows to PyArrow RecordBatch."""
    if not rows:
        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("user_id", pa.int64()),
                ("beatmap_id", pa.int64()),
                ("score", pa.int64()),
                ("data", pa.string()),
                ("mods_key", pa.string()),
                ("speed_mod", pa.string()),
            ]
        )
        return pa.RecordBatch.from_pydict(
            {
                "id": [],
                "user_id": [],
                "beatmap_id": [],
                "score": [],
                "data": [],
                "mods_key": [],
                "speed_mod": [],
            },
            schema=schema,
        )

    if columns is None:
        columns = ["id", "user_id", "beatmap_id", "score", "data"]

    type_map = _infer_column_types(rows, columns)
    data_dict = {col: [] for col in columns + ["mods_key", "speed_mod"]}

    for row in rows:
        parser = SqlParser()
        parsed_values = []
        for field in row:
            parsed_values.append(parser.parse_value(field))

        while len(parsed_values) < len(columns):
            parsed_values.append(None)

        data_idx = columns.index("data") if "data" in columns else -1
        if data_idx >= 0 and data_idx < len(parsed_values):
            data_str = parsed_values[data_idx]
            mods = parse_mods_from_data(data_str)
            mods_key, speed_mod = normalize_mods(mods)
        else:
            mods_key = ""
            speed_mod = None

        for i, col in enumerate(columns):
            if i < len(parsed_values):
                data_dict[col].append(parsed_values[i])
            else:
                data_dict[col].append(None)

        data_dict["mods_key"].append(mods_key)
        data_dict["speed_mod"].append(speed_mod)

    schema_fields = []
    for col in data_dict.keys():
        if col in type_map:
            schema_fields.append((col, type_map[col]))
        elif col in ["mods_key", "speed_mod"]:
            schema_fields.append((col, pa.string()))
        else:
            schema_fields.append((col, pa.string()))

    schema = pa.schema(schema_fields)

    arrays = {}
    for col, values in data_dict.items():
        if col in type_map:
            try:
                arrays[col] = pa.array(values, type=type_map[col])
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                arrays[col] = pa.array(
                    [str(v) if v is not None else None for v in values],
                    type=pa.string(),
                )
        elif col in ["mods_key", "speed_mod"]:
            arrays[col] = pa.array(values, type=pa.string())
        else:
            arrays[col] = pa.array(values)

    return pa.RecordBatch.from_pydict(arrays, schema=schema)


def parse_sql_file(
    file_path: str,
    table_name: str,
    batch_size: int = 100000,
    columns: Optional[list] = None,
) -> Iterator[pa.RecordBatch]:
    """
    Stream parse SQL file and yield RecordBatches.

    Args:
        file_path: Path to SQL file
        table_name: Name of table to parse (e.g., 'scores')
        batch_size: Number of rows per batch
        columns: Optional column schema

    Yields:
        PyArrow RecordBatch objects
    """
    parser = SqlParser(table_name)
    batch_rows = []
    total_rows = 0
    last_log_rows = 0
    start_time = time.time()

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            row = parser.parse_line(line)
            if row:
                batch_rows.append(row)
                total_rows += 1

                # Log progress every 100,000 rows
                if total_rows - last_log_rows >= 100000:
                    elapsed = time.time() - start_time
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"  Progress: {total_rows:,} rows ({elapsed:.1f}s, ~{rate:,.0f} rows/sec)"
                    )
                    last_log_rows = total_rows

                if len(batch_rows) >= batch_size:
                    cols = columns if columns else parser.columns
                    yield _rows_to_batch(batch_rows, cols)
                    batch_rows = []

    if batch_rows:
        cols = columns if columns else parser.columns
        yield _rows_to_batch(batch_rows, cols)
