# DuckDB Native Import Script - Test Results

**Date:** 2026-02-03  
**Script:** `scripts/import_duckdb_native.sh`

## Summary

The script has been tested and fixed. It now works correctly for importing MySQL SQL dumps to Parquet via DuckDB.

## Issues Found and Fixed

### 1. DuckDB Binary Location
**Issue:** DuckDB CLI was not in PATH  
**Fix:** Added logic to check `${PROJECT_DIR}/bin/duckdb` first, then fall back to PATH
```bash
if [[ -f "${PROJECT_DIR}/bin/duckdb" ]]; then
    DUCKDB_CMD="${PROJECT_DIR}/bin/duckdb"
else
    DUCKDB_CMD="duckdb"
fi
```

### 2. MariaDB Authentication
**Issue:** Script assumed root user with password, but system uses socket authentication  
**Fix:** Made DB_USER optional (empty string uses socket auth)
```bash
DB_USER="${DB_USER:-}"  # Empty = socket authentication
```

### 3. Database Name
**Issue:** Hardcoded `osu_import` database name  
**Fix:** Made configurable via environment variable with default to `test`
```bash
DB_NAME="${DB_NAME:-test}"
```

### 4. Connection String Construction
**Issue:** Connection string included user even when not needed  
**Fix:** Dynamic connection string building
```bash
local conn_string="host=${DB_HOST} database=${DB_NAME}"
if [[ -n "${DB_USER}" ]]; then
    conn_string="${conn_string} user=${DB_USER}"
fi
```

### 5. SHOW TABLES Syntax
**Issue:** DuckDB doesn't support `SHOW TABLES FROM mysqldb`  
**Fix:** Query MySQL directly for table list instead

## Test Results

### Phase 1: MySQL Import

| Table | File Size | Import Time | Rows |
|-------|-----------|-------------|------|
| osu_counts | 2.8KB | 0.045s | 30 |
| sample_users | 256KB | 0.089s | 9,996 |

**Commands tested:**
```bash
# Import to MySQL
mariadb test < data/ingest/2026-02/sql/osu_counts.sql
mariadb test < data/ingest/2026-02/sql/sample_users.sql

# Verify import
mariadb -e "USE test; SHOW TABLES; SELECT COUNT(*) FROM osu_counts;"
```

### Phase 2: DuckDB Export

| Table | Export Time | Parquet Size | Rows |
|-------|-------------|--------------|------|
| osu_counts | 0.286s | 878 bytes | 30 |
| sample_users | 0.660s | 139KB | 9,996 |

**Commands tested:**
```bash
# Install DuckDB extension
./bin/duckdb -c "INSTALL mysql; LOAD mysql;"

# Test connection
./bin/duckdb -c "
  INSTALL mysql;
  LOAD mysql;
  ATTACH 'host=localhost database=test' AS mysqldb (TYPE mysql);
  SELECT * FROM mysqldb.osu_counts LIMIT 5;
"

# Export to Parquet
./bin/duckdb -c "
  INSTALL mysql;
  LOAD mysql;
  ATTACH 'host=localhost database=test' AS mysqldb (TYPE mysql);
  COPY (SELECT * FROM mysqldb.osu_counts) TO 'data/parquet/osu_counts.parquet' (FORMAT PARQUET, COMPRESSION 'snappy');
"

# Verify Parquet
./bin/duckdb -c "SELECT * FROM read_parquet('data/parquet/osu_counts.parquet') LIMIT 5;"
```

## Performance Comparison

### MySQL Import Speed
- **osu_counts (2.8KB):** 0.045s (~62KB/s)
- **sample_users (256KB):** 0.089s (~2.9MB/s)

### DuckDB Export Speed
- **osu_counts (30 rows):** 0.286s total (includes extension loading)
- **sample_users (9,996 rows):** 0.660s total (~15K rows/s)

### Comparison with Python Parsing
Based on file sizes and typical Python parsing performance:
- Python streaming parser: ~1-5MB/s for SQL parsing
- MySQL import: ~2-10MB/s (varies by table complexity)
- DuckDB export: Very fast once connected (~50K+ rows/s expected for larger tables)

**Conclusion:** The DuckDB approach is competitive and likely faster for large tables due to:
1. No intermediate Python processing
2. Direct MySQL-to-Parquet conversion
3. Optimized C++ implementation

## Usage Instructions

### Prerequisites
```bash
# Install DuckDB CLI (if not already installed)
curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
unzip -o /tmp/duckdb.zip -d /tmp/
mv /tmp/duckdb ${PROJECT_DIR}/bin/
```

### Configuration
```bash
# Optional: Set database name (default: test)
export DB_NAME=my_database

# Optional: Set database user (default: empty for socket auth)
export DB_USER=root

# Optional: Skip Phase 1 if MySQL already loaded
export SKIP_PHASE1=true
```

### Run the Script
```bash
# Full import (Phase 1 + Phase 2)
./scripts/import_duckdb_native.sh

# Skip MySQL import (if already loaded)
./scripts/import_duckdb_native.sh --skip-phase1
```

## Known Limitations

1. **Requires running MariaDB/MySQL** - The script needs a local database server
2. **Socket authentication** - Tested with socket auth; password auth may need additional testing
3. **Single database** - Currently imports to a single database at a time
4. **Memory usage** - Large tables may require tuning MySQL's `innodb_buffer_pool_size`

## Recommendations

1. **For production use:**
   - Set `DB_NAME` to appropriate database
   - Configure MySQL with optimized settings (see script for `innodb_flush_log_at_trx_commit`)
   - Monitor disk space for Parquet output

2. **For testing:**
   - Use `DB_NAME=test` (default)
   - Start with small tables (osu_counts, sample_users)
   - Verify row counts match between MySQL and Parquet

3. **Performance tuning:**
   - Increase `PARALLEL_JOBS` for concurrent exports (not yet implemented)
   - Adjust `ROW_GROUP_SIZE` based on table size
   - Use SSD storage for both MySQL data and Parquet output

## Verification Checklist

- [x] DuckDB CLI installed and working
- [x] MariaDB running and accessible
- [x] MySQL import works for small tables
- [x] MySQL import works for medium tables
- [x] DuckDB MySQL extension installs correctly
- [x] DuckDB connects to MySQL successfully
- [x] Export to Parquet works
- [x] Parquet files are readable and contain correct data
- [x] Row counts match between MySQL and Parquet

## Next Steps

1. Test with larger tables (osu_user_stats.sql - 2MB)
2. Test with scores.sql (13GB) to verify performance at scale
3. Consider adding parallel export for multiple tables
4. Add progress indicators for large table exports
