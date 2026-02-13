"""
Tests for Parquet writer.
TDD approach - write failing tests first (RED phase).
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_record_batch():
    """Create a sample RecordBatch for testing."""
    return pa.RecordBatch.from_pydict(
        {
            "id": [1, 2, 3],
            "user_id": [123, 124, 125],
            "beatmap_id": [456, 456, 457],
            "score": [1000000, 950000, 980000],
            "mods_key": ["DT", "", "DT,HR"],
            "speed_mod": ["DT", None, "DT"],
        }
    )


@pytest.fixture
def large_record_batch():
    """Create a larger batch for testing row limits."""
    n = 10000
    return pa.RecordBatch.from_pydict(
        {
            "id": list(range(n)),
            "user_id": [100 + i % 1000 for i in range(n)],
            "beatmap_id": [200 + i % 500 for i in range(n)],
            "score": [500000 + i * 100 for i in range(n)],
            "mods_key": [
                "DT" if i % 3 == 0 else "HR" if i % 3 == 1 else "" for i in range(n)
            ],
            "speed_mod": ["DT" if i % 3 == 0 else None for i in range(n)],
        }
    )


@pytest.fixture
def multi_batch_fixture():
    """Create multiple RecordBatches for testing."""
    batches = []
    for batch_idx in range(3):
        n = 1000
        batches.append(
            pa.RecordBatch.from_pydict(
                {
                    "id": [batch_idx * 1000 + i for i in range(n)],
                    "user_id": [100 + i % 100 for i in range(n)],
                    "beatmap_id": [200 + i % 50 for i in range(n)],
                    "score": [500000 + i * 10 for i in range(n)],
                    "mods_key": [
                        "DT"
                        if i % 4 == 0
                        else "HR"
                        if i % 4 == 1
                        else "HD"
                        if i % 4 == 2
                        else ""
                        for i in range(n)
                    ],
                    "speed_mod": ["DT" if i % 4 == 0 else None for i in range(n)],
                }
            )
        )
    return batches


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary output directory."""
    return tmp_path / "parquet_output"


@pytest.fixture
def schema_with_various_types():
    """Create a RecordBatch with various Arrow types."""
    return pa.RecordBatch.from_pydict(
        {
            "int_col": [1, 2, 3],
            "float_col": [1.5, 2.5, 3.5],
            "string_col": ["a", "b", "c"],
            "bool_col": [True, False, True],
            "null_col": [None, None, None],
        }
    )


# =============================================================================
# Basic Conversion Tests
# =============================================================================


def test_write_single_batch(temp_output_dir, sample_record_batch):
    """Should write a single RecordBatch to Parquet."""
    from pipelines.parquet_writer import write_parquet_batches

    result = write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    # Should create output directory
    assert temp_output_dir.exists()
    # Should return manifest
    assert result is not None
    assert isinstance(result, dict)


def test_batch_to_parquet_schema_preserved(temp_output_dir, sample_record_batch):
    """Schema should be preserved in output Parquet file."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    # Find and read the parquet file
    parquet_files = list(temp_output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    table = pq.read_table(parquet_files[0])
    schema = table.schema

    # Check all columns exist
    assert "id" in schema.names
    assert "user_id" in schema.names
    assert "beatmap_id" in schema.names
    assert "score" in schema.names
    assert "mods_key" in schema.names
    assert "speed_mod" in schema.names


def test_various_arrow_types_preserved(temp_output_dir, schema_with_various_types):
    """Various Arrow types should be preserved correctly."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[schema_with_various_types],
        output_dir=str(temp_output_dir),
        table_name="test_table",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    parquet_files = list(temp_output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    table = pq.read_table(parquet_files[0])
    schema = table.schema

    # Check column types are preserved
    assert pa.types.is_integer(schema.field("int_col").type)
    assert pa.types.is_floating(schema.field("float_col").type)
    assert pa.types.is_string(schema.field("string_col").type)
    assert pa.types.is_boolean(schema.field("bool_col").type)


# =============================================================================
# Shard/Partition Tests
# =============================================================================


def test_creates_part_files(temp_output_dir, large_record_batch):
    """Should create part-XXXXXX.parquet files."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[large_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    # Check for part-XXXXXX.parquet files
    parquet_files = list(temp_output_dir.glob("part-*.parquet"))
    assert len(parquet_files) > 0

    # Check naming convention
    for f in parquet_files:
        assert f.name.startswith("part-")
        assert f.name.endswith(".parquet")
        # Extract number part
        number_part = f.name[5:-8]  # Between 'part-' and '.parquet'
        assert number_part.isdigit()
        assert len(number_part) == 6  # Zero-padded to 6 digits


def test_respects_file_rows_limit(temp_output_dir):
    """Should create new file when row limit exceeded."""
    from pipelines.parquet_writer import write_parquet_batches

    # Create batches that exceed file_rows limit
    n = 5000
    batch1 = pa.RecordBatch.from_pydict(
        {"id": list(range(n)), "value": [i for i in range(n)]}
    )
    batch2 = pa.RecordBatch.from_pydict(
        {"id": list(range(n, n * 2)), "value": [i for i in range(n, n * 2)]}
    )

    # Set file_rows to 6000, so 10000 rows should create 2 files
    write_parquet_batches(
        batches=[batch1, batch2],
        output_dir=str(temp_output_dir),
        table_name="test",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=6000,  # Small limit to force multiple files
        compression="snappy",
    )

    parquet_files = sorted(temp_output_dir.glob("part-*.parquet"))
    assert len(parquet_files) >= 2


def test_multiple_batches_single_file(temp_output_dir):
    """Multiple small batches should be combined into single file."""
    from pipelines.parquet_writer import write_parquet_batches

    # Create small batches
    batches = []
    for i in range(5):
        batches.append(
            pa.RecordBatch.from_pydict(
                {
                    "id": [i * 10 + j for j in range(10)],
                    "value": [i * 10 + j for j in range(10)],
                }
            )
        )

    # With file_rows=1000, all 50 rows should fit in one file
    write_parquet_batches(
        batches=batches,
        output_dir=str(temp_output_dir),
        table_name="test",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=1000,
        compression="snappy",
    )

    parquet_files = list(temp_output_dir.glob("part-*.parquet"))
    assert len(parquet_files) == 1


# =============================================================================
# Compression Tests
# =============================================================================


def test_snappy_compression_applied(temp_output_dir, sample_record_batch):
    """Should apply Snappy compression."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    parquet_files = list(temp_output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # Read metadata and check compression
    metadata = pq.read_metadata(parquet_files[0])
    # Compression should be SNAPPY
    assert metadata.row_group(0).column(0).compression == "SNAPPY"


def test_compression_none_option(temp_output_dir, sample_record_batch):
    """Should support no compression option."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="none",
    )

    parquet_files = list(temp_output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # File should exist and be readable
    table = pq.read_table(parquet_files[0])
    assert table.num_rows == 3


# =============================================================================
# Row Group Tests
# =============================================================================


def test_row_group_size_configurable(temp_output_dir, large_record_batch):
    """Row group size should be configurable."""
    from pipelines.parquet_writer import write_parquet_batches

    # Set row_group_rows to 1000
    write_parquet_batches(
        batches=[large_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=1000,  # Small row group size
        file_rows=2000000,
        compression="snappy",
    )

    parquet_files = list(temp_output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # Check row groups
    metadata = pq.read_metadata(parquet_files[0])
    num_row_groups = metadata.num_row_groups

    # With 10000 rows and row_group_rows=1000, expect ~10 row groups
    assert num_row_groups >= 5  # At least several row groups


def test_row_group_size_respected(temp_output_dir):
    """Row groups should not exceed configured size."""
    from pipelines.parquet_writer import write_parquet_batches

    n = 5000
    batch = pa.RecordBatch.from_pydict(
        {"id": list(range(n)), "value": [i for i in range(n)]}
    )

    write_parquet_batches(
        batches=[batch],
        output_dir=str(temp_output_dir),
        table_name="test",
        batch_rows=100000,
        row_group_rows=1000,  # Each row group should have ~1000 rows
        file_rows=2000000,
        compression="snappy",
    )

    parquet_files = list(temp_output_dir.glob("*.parquet"))
    metadata = pq.read_metadata(parquet_files[0])

    # Check each row group doesn't exceed limit by too much
    for i in range(metadata.num_row_groups):
        rg_rows = metadata.row_group(i).num_rows
        assert rg_rows <= 1500  # Allow some buffer over 1000


# =============================================================================
# Manifest Tests
# =============================================================================


def test_manifest_generated(temp_output_dir, sample_record_batch):
    """Should generate manifest.json with row counts."""
    from pipelines.parquet_writer import write_parquet_batches

    result = write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    # Check manifest.json exists
    manifest_path = temp_output_dir / "manifest.json"
    assert manifest_path.exists()

    # Parse manifest
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Check required fields
    assert "table_name" in manifest
    assert manifest["table_name"] == "scores"
    assert "version" in manifest
    assert "created_at" in manifest
    assert "files" in manifest
    assert "total_rows" in manifest
    assert "schema" in manifest


def test_manifest_contains_hashes(temp_output_dir, sample_record_batch):
    """Manifest should contain file hashes."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    manifest_path = temp_output_dir / "manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Check each file has a hash
    for file_info in manifest["files"]:
        assert "hash" in file_info
        assert file_info["hash"].startswith("sha256:")
        # Verify hash format (sha256: followed by 64 hex chars)
        hash_value = file_info["hash"][7:]  # Remove 'sha256:' prefix
        assert len(hash_value) == 64
        assert all(c in "0123456789abcdef" for c in hash_value)


def test_manifest_file_info_complete(temp_output_dir, large_record_batch):
    """Manifest should contain complete file information."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[large_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=5000,  # Force multiple files
        compression="snappy",
    )

    manifest_path = temp_output_dir / "manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Check each file entry has all required fields
    for file_info in manifest["files"]:
        assert "file" in file_info
        assert file_info["file"].endswith(".parquet")
        assert "rows" in file_info
        assert isinstance(file_info["rows"], int)
        assert file_info["rows"] > 0
        assert "size_bytes" in file_info
        assert isinstance(file_info["size_bytes"], int)
        assert file_info["size_bytes"] > 0


def test_manifest_total_rows_correct(temp_output_dir, multi_batch_fixture):
    """Manifest total_rows should equal sum of all file rows."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=multi_batch_fixture,
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    manifest_path = temp_output_dir / "manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Calculate sum of file rows
    file_rows_sum = sum(f["rows"] for f in manifest["files"])

    # Should match total_rows
    assert manifest["total_rows"] == file_rows_sum
    # Should match original data
    assert manifest["total_rows"] == 3000  # 3 batches * 1000 rows each


def test_manifest_schema_structure(temp_output_dir, sample_record_batch):
    """Manifest should contain schema information."""
    from pipelines.parquet_writer import write_parquet_batches

    write_parquet_batches(
        batches=[sample_record_batch],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    manifest_path = temp_output_dir / "manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Check schema exists and has proper structure
    assert "schema" in manifest
    schema = manifest["schema"]
    assert isinstance(schema, dict)

    # Should have fields or columns
    assert "fields" in schema or "columns" in schema


# =============================================================================
# ParquetWriter Class Tests
# =============================================================================


def test_parquet_writer_class_exists():
    """ParquetWriter class should be importable."""
    from pipelines.parquet_writer import ParquetWriter

    writer = ParquetWriter(
        output_dir="/tmp/test",
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    assert writer is not None
    assert writer.table_name == "scores"


def test_parquet_writer_write_batch(temp_output_dir, sample_record_batch):
    """ParquetWriter should support write_batch method."""
    from pipelines.parquet_writer import ParquetWriter

    writer = ParquetWriter(
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    writer.write_batch(sample_record_batch)
    manifest = writer.finalize()

    # Should create files
    parquet_files = list(temp_output_dir.glob("*.parquet"))
    assert len(parquet_files) > 0

    # Should return manifest
    assert manifest is not None
    assert "files" in manifest


def test_parquet_writer_multiple_batches(temp_output_dir, multi_batch_fixture):
    """ParquetWriter should handle multiple batches."""
    from pipelines.parquet_writer import ParquetWriter

    writer = ParquetWriter(
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    for batch in multi_batch_fixture:
        writer.write_batch(batch)

    manifest = writer.finalize()

    # Should have correct total rows
    assert manifest["total_rows"] == 3000


# =============================================================================
# Integration Tests
# =============================================================================


def test_full_write_pipeline(temp_output_dir):
    """Full pipeline: multiple batches → Parquet shards → manifest."""
    from pipelines.parquet_writer import write_parquet_batches

    # Create multiple batches
    batches = []
    for i in range(5):
        n = 2000
        batches.append(
            pa.RecordBatch.from_pydict(
                {
                    "id": [i * 2000 + j for j in range(n)],
                    "user_id": [100 + j % 100 for j in range(n)],
                    "beatmap_id": [200 + j % 50 for j in range(n)],
                    "score": [500000 + j * 10 for j in range(n)],
                    "mods_key": ["DT" if j % 4 == 0 else "" for j in range(n)],
                    "speed_mod": ["DT" if j % 4 == 0 else None for j in range(n)],
                }
            )
        )

    # Write with small file limit to force multiple files
    manifest = write_parquet_batches(
        batches=batches,
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=1000,
        file_rows=3000,  # Small limit to create multiple files
        compression="snappy",
    )

    # Verify output
    assert temp_output_dir.exists()

    # Should have multiple parquet files
    parquet_files = sorted(temp_output_dir.glob("part-*.parquet"))
    assert len(parquet_files) >= 3  # 10000 rows / 3000 limit = ~4 files

    # Should have manifest
    manifest_path = temp_output_dir / "manifest.json"
    assert manifest_path.exists()

    # Verify manifest content
    assert manifest["table_name"] == "scores"
    assert manifest["total_rows"] == 10000
    assert len(manifest["files"]) >= 3

    # Verify each parquet file is readable and has correct schema
    total_rows = 0
    for pf in parquet_files:
        table = pq.read_table(pf)
        total_rows += table.num_rows

        # Check schema
        assert "id" in table.schema.names
        assert "user_id" in table.schema.names
        assert "beatmap_id" in table.schema.names

    assert total_rows == 10000


def test_empty_batches_handling(temp_output_dir):
    """Should handle empty batch list gracefully."""
    from pipelines.parquet_writer import write_parquet_batches

    manifest = write_parquet_batches(
        batches=[],
        output_dir=str(temp_output_dir),
        table_name="scores",
        batch_rows=100000,
        row_group_rows=500000,
        file_rows=2000000,
        compression="snappy",
    )

    # Should still create manifest
    manifest_path = temp_output_dir / "manifest.json"
    assert manifest_path.exists()

    with open(manifest_path) as f:
        manifest = json.load(f)

    assert manifest["total_rows"] == 0
    assert manifest["files"] == []
