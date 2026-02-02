"""
Tests for SQL parser state machine.
TDD approach - write failing tests first (RED phase).
"""

import json
from pathlib import Path

import pytest
import pyarrow as pa


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_sql_snippet():
    """Basic SQL snippet with INSERT INTO statement."""
    return """INSERT INTO `scores` (`id`, `user_id`, `beatmap_id`, `score`, `data`) VALUES
(1, 123, 456, 1000000, '{"mods": [{"acronym": "DT"}]}'),
(2, 124, 456, 950000, NULL),
(3, 125, 457, 980000, '{"mods": [{"acronym": "HR"}, {"acronym": "DT"}]}');"""


@pytest.fixture
def sample_sql_with_escaped_quotes():
    """SQL with escaped quotes in strings."""
    return """INSERT INTO `scores` (`id`, `data`) VALUES
(1, 'It''s a test with ''quotes'''),
(2, 'Another ''escaped'' string');"""


@pytest.fixture
def sample_sql_with_malformed_json():
    """SQL with malformed JSON in data field."""
    return """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "DT"}'),  -- truncated JSON
(2, 'not valid json at all'),
(3, NULL);"""


@pytest.fixture
def sample_sql_with_various_mods():
    """SQL with various mod combinations."""
    return """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "HR"}, {"acronym": "DT"}]}'),  -- DT,HR sorted
(2, '{"mods": [{"acronym": "DT"}, {"acronym": "HR"}]}'),  -- Same as above, unsorted input
(3, '{"mods": [{"acronym": "HT"}]}'),  -- HT only
(4, '{"mods": [{"acronym": "NC"}]}'),  -- NC (treated as DT)
(5, '{"mods": [{"acronym": "HR"}, {"acronym": "HD"}]}'),  -- No speed mod
(6, '{"mods": []}'),  -- Empty mods array
(7, NULL);"""


@pytest.fixture
def sample_sql_truncated():
    """Truncated SQL statement."""
    return """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "DT"}]}'),
(2, NULL  -- missing closing paren and semicolon"""


@pytest.fixture
def temp_sql_file(tmp_path):
    """Factory fixture to create temporary SQL files."""

    def _create(content):
        f = tmp_path / "test.sql"
        f.write_text(content)
        return f

    return _create


@pytest.fixture
def sample_sql_multiple_inserts():
    """SQL with multiple INSERT statements."""
    return """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "DT"}]}');

INSERT INTO `scores` (`id`, `data`) VALUES
(2, '{"mods": [{"acronym": "HR"}]}');"""


@pytest.fixture
def sample_sql_with_numbers():
    """SQL with various number formats."""
    return """INSERT INTO `scores` (`id`, `score`, `accuracy`) VALUES
(1, 1000000, 98.5),
(2, 950000, NULL),
(3, -50000, 100.0);"""


# =============================================================================
# State Machine Tests
# =============================================================================


def test_parser_initial_state():
    """Parser should start in SEARCH_INSERT state."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    assert parser.state == "SEARCH_INSERT"


def test_parser_finds_insert_statement():
    """Parser should transition to READ_VALUES on INSERT INTO."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    parser.feed("INSERT INTO `scores` (`id`, `data`) VALUES")

    assert parser.state == "READ_VALUES"


def test_parser_transitions_to_read_row():
    """Parser should transition to READ_ROW after VALUES."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    parser.feed("INSERT INTO `scores` (`id`, `data`) VALUES (1, 'test')")

    assert parser.state == "READ_ROW"


def test_parser_transitions_to_read_field():
    """Parser should transition to READ_FIELD when parsing row values."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    parser.feed("INSERT INTO `scores` (`id`, `data`) VALUES (1,")

    assert parser.state == "READ_FIELD"


def test_parser_returns_to_search_insert():
    """Parser should return to SEARCH_INSERT after semicolon."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    parser.feed("INSERT INTO `scores` (`id`, `data`) VALUES (1, 'test');")

    assert parser.state == "SEARCH_INSERT"


# =============================================================================
# Token Parsing Tests
# =============================================================================


def test_parse_null():
    """Parser should recognize NULL values."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value("NULL")

    assert result is None


def test_parse_integer():
    """Parser should parse integer values."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value("123")

    assert result == 123
    assert isinstance(result, int)


def test_parse_negative_integer():
    """Parser should parse negative integer values."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value("-456")

    assert result == -456
    assert isinstance(result, int)


def test_parse_float():
    """Parser should parse float values."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value("98.5")

    assert result == 98.5
    assert isinstance(result, float)


def test_parse_quoted_string():
    """Parser should parse quoted strings."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value("'hello world'")

    assert result == "hello world"


def test_parse_escaped_quotes():
    """Parser should handle escaped quotes in strings."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value("'It''s a test with ''quotes'''")

    assert result == "It's a test with 'quotes'"


def test_parse_json_string():
    """Parser should parse JSON strings and return as string."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    result = parser.parse_value('\'{"mods": [{"acronym": "DT"}]}\'')

    assert result == '{"mods": [{"acronym": "DT"}]}'


# =============================================================================
# JSON Parsing Tests
# =============================================================================


def test_parse_data_json_with_mods():
    """Parser should extract mods from data JSON."""
    from pipelines.sql_parser import parse_mods_from_data

    data = '{"mods": [{"acronym": "DT"}, {"acronym": "HR"}]}'
    mods = parse_mods_from_data(data)

    assert mods == [{"acronym": "DT"}, {"acronym": "HR"}]


def test_parse_data_json_empty_mods():
    """Parser should handle empty mods array."""
    from pipelines.sql_parser import parse_mods_from_data

    data = '{"mods": []}'
    mods = parse_mods_from_data(data)

    assert mods == []


def test_parse_data_json_no_mods_field():
    """Parser should handle JSON without mods field."""
    from pipelines.sql_parser import parse_mods_from_data

    data = '{"other": "value"}'
    mods = parse_mods_from_data(data)

    assert mods == []


def test_parse_data_json_null():
    """Parser should handle NULL data."""
    from pipelines.sql_parser import parse_mods_from_data

    mods = parse_mods_from_data(None)

    assert mods == []


def test_parse_data_json_malformed():
    """Parser should handle malformed JSON gracefully."""
    from pipelines.sql_parser import parse_mods_from_data

    data = '{"mods": [{"acronym": "DT"}'  # truncated
    mods = parse_mods_from_data(data)

    assert mods == []


def test_parse_data_json_invalid():
    """Parser should handle completely invalid JSON."""
    from pipelines.sql_parser import parse_mods_from_data

    data = "not json at all"
    mods = parse_mods_from_data(data)

    assert mods == []


# =============================================================================
# Mod Normalization Tests
# =============================================================================


def test_mods_sorted_alphabetically():
    """Mod acronyms should be sorted alphabetically."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "DT"}, {"acronym": "HR"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "DT,HR"  # DT comes before HR alphabetically


def test_mods_sorted_unsorted_input():
    """Mod acronyms should be sorted even if input is unsorted."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "HR"}, {"acronym": "DT"}, {"acronym": "HD"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "DT,HD,HR"


def test_speed_mod_dt():
    """DT should be categorized as speed_mod='DT'."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "DT"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "DT"
    assert speed_mod == "DT"


def test_speed_mod_nc():
    """NC should be treated as DT for speed_mod."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "NC"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "NC"
    assert speed_mod == "DT"


def test_speed_mod_ht():
    """HT should be categorized as speed_mod='HT'."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "HT"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "HT"
    assert speed_mod == "HT"


def test_speed_mod_none():
    """No speed mods should result in speed_mod=None."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "HR"}, {"acronym": "HD"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "HD,HR"
    assert speed_mod is None


def test_speed_mod_empty():
    """Empty mods should result in empty mods_key and None speed_mod."""
    from pipelines.sql_parser import normalize_mods

    mods = []
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == ""
    assert speed_mod is None


def test_speed_mod_dt_priority():
    """DT takes priority over HT when both present."""
    from pipelines.sql_parser import normalize_mods

    mods = [{"acronym": "HT"}, {"acronym": "DT"}]
    mods_key, speed_mod = normalize_mods(mods)

    assert mods_key == "DT,HT"
    assert speed_mod == "DT"


# =============================================================================
# Error Handling Tests
# =============================================================================


def test_error_malformed_json_in_data(temp_sql_file):
    """Parser should handle malformed JSON gracefully without crashing."""
    from pipelines.sql_parser import parse_sql_file

    sql_content = """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "DT"}'),  -- truncated JSON
(2, 'not valid json');"""

    sql_file = temp_sql_file(sql_content)

    # Should not raise exception
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    # Should still yield some data
    assert len(batches) > 0


def test_error_truncated_statement(temp_sql_file):
    """Parser should handle truncated statements gracefully."""
    from pipelines.sql_parser import parse_sql_file

    sql_content = """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "DT"}]}'),
(2, NULL  -- missing closing paren and semicolon"""

    sql_file = temp_sql_file(sql_content)

    # Should not raise exception
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    # May yield partial data or empty
    assert isinstance(batches, list)


def test_error_missing_table(temp_sql_file):
    """Parser should handle missing table gracefully."""
    from pipelines.sql_parser import parse_sql_file

    sql_content = """INSERT INTO `other_table` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "DT"}]}');"""

    sql_file = temp_sql_file(sql_content)

    # Should not raise exception, but yield no data for 'scores' table
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert batches == []


def test_error_empty_file(temp_sql_file):
    """Parser should handle empty files gracefully."""
    from pipelines.sql_parser import parse_sql_file

    sql_file = temp_sql_file("")

    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert batches == []


def test_error_invalid_sql(temp_sql_file):
    """Parser should handle invalid SQL gracefully."""
    from pipelines.sql_parser import parse_sql_file

    sql_content = "This is not SQL at all"

    sql_file = temp_sql_file(sql_content)

    # Should not raise exception
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert batches == []


# =============================================================================
# Integration Tests
# =============================================================================


def test_full_parse_sql_file(temp_sql_file, sample_sql_snippet):
    """Parser should yield RecordBatches from SQL file."""
    from pipelines.sql_parser import parse_sql_file

    sql_file = temp_sql_file(sample_sql_snippet)
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert len(batches) > 0

    # Check first batch is a RecordBatch
    batch = batches[0]
    assert isinstance(batch, pa.RecordBatch)

    # Check expected columns exist
    assert "id" in batch.schema.names
    assert "user_id" in batch.schema.names
    assert "beatmap_id" in batch.schema.names
    assert "score" in batch.schema.names
    assert "mods_key" in batch.schema.names
    assert "speed_mod" in batch.schema.names


def test_parse_sql_file_with_mods_normalization(temp_sql_file):
    """Parser should normalize mods correctly in full parse."""
    from pipelines.sql_parser import parse_sql_file

    sql_content = """INSERT INTO `scores` (`id`, `data`) VALUES
(1, '{"mods": [{"acronym": "HR"}, {"acronym": "DT"}]}'),
(2, '{"mods": [{"acronym": "HT"}]}'),
(3, NULL);"""

    sql_file = temp_sql_file(sql_content)
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert len(batches) > 0

    batch = batches[0]

    # Check mods are sorted (DT comes before HR)
    mods_keys = batch.column("mods_key").to_pylist()
    assert "DT,HR" in mods_keys
    assert "HT" in mods_keys
    assert "" in mods_keys  # NULL should result in empty string

    # Check speed_mod categorization
    speed_mods = batch.column("speed_mod").to_pylist()
    assert "DT" in speed_mods
    assert "HT" in speed_mods
    assert None in speed_mods


def test_parse_sql_file_multiple_inserts(temp_sql_file, sample_sql_multiple_inserts):
    """Parser should handle multiple INSERT statements."""
    from pipelines.sql_parser import parse_sql_file

    sql_file = temp_sql_file(sample_sql_multiple_inserts)
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert len(batches) > 0

    # Should have data from both INSERT statements
    total_rows = sum(batch.num_rows for batch in batches)
    assert total_rows == 2


def test_parse_sql_file_batch_size(temp_sql_file, sample_sql_snippet):
    """Parser should respect batch_size parameter."""
    from pipelines.sql_parser import parse_sql_file

    sql_file = temp_sql_file(sample_sql_snippet)
    batches = list(parse_sql_file(str(sql_file), table_name="scores", batch_size=2))

    # Should have multiple batches with at most 2 rows each
    for batch in batches:
        assert batch.num_rows <= 2

    # Total rows should be 3
    total_rows = sum(batch.num_rows for batch in batches)
    assert total_rows == 3


def test_parse_sql_file_with_escaped_quotes(
    temp_sql_file, sample_sql_with_escaped_quotes
):
    """Parser should handle escaped quotes in full parse."""
    from pipelines.sql_parser import parse_sql_file

    sql_file = temp_sql_file(sample_sql_with_escaped_quotes)
    batches = list(parse_sql_file(str(sql_file), table_name="scores"))

    assert len(batches) > 0

    batch = batches[0]
    # Check data column has unescaped strings
    data_values = batch.column("data").to_pylist()
    assert "It's a test with 'quotes'" in data_values


# =============================================================================
# State Machine Edge Cases
# =============================================================================


def test_parser_handles_whitespace():
    """Parser should handle various whitespace patterns."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    # Various whitespace patterns
    parser.feed("  INSERT   INTO   `scores`  (`id`)  VALUES  (1)  ;  ")

    assert parser.state == "SEARCH_INSERT"


def test_parser_handles_newlines():
    """Parser should handle newlines in SQL."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    parser.feed("""INSERT INTO `scores`
(`id`, `data`)
VALUES
(1, 'test');""")

    assert parser.state == "SEARCH_INSERT"


def test_parser_handles_comments():
    """Parser should handle SQL comments."""
    from pipelines.sql_parser import SqlParser

    parser = SqlParser()
    parser.feed("""-- This is a comment
INSERT INTO `scores` (`id`, `data`) VALUES
/* Multi-line
   comment */
(1, 'test');""")

    # Should still parse correctly
    assert parser.state == "SEARCH_INSERT"
