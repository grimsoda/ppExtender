# Comprehensive MySQL Import Optimization Checklist

## For Monthly SQL Dump Reimports

---

## Phase 0: Pre-Import Optimizations (Do These FIRST)

### 1. MariaDB Server Configuration

Edit `/etc/my.cnf` or `/etc/mysql/mariadb.conf.d/50-server.cnf`:

```ini
[mysqld]
# ============================================================================
# CRITICAL: Memory Settings (Most Important!)
# ============================================================================
# Buffer pool should be 50-70% of RAM for import-only workloads
# For 16GB RAM system:
innodb_buffer_pool_size = 10G

# Redo log size (larger = less flushing, faster writes)
innodb_log_file_size = 2G
innodb_log_buffer_size = 256M

# ============================================================================
# CRITICAL: Disable Durability (Data loss risk on crash - acceptable for import)
# ============================================================================
# Don't flush on every commit (MAJOR speedup)
innodb_flush_log_at_trx_commit = 0

# Disable doublewrite buffer (MAJOR speedup)
innodb_doublewrite = 0

# ============================================================================
# Parallel I/O (For NVMe SSDs)
# ============================================================================
innodb_read_io_threads = 16
innodb_write_io_threads = 16
innodb_io_capacity = 4000
innodb_io_capacity_max = 8000

# ============================================================================
# Logging (Disable during import)
# ============================================================================
skip-log-bin
slow_query_log = 0
general_log = 0

# ============================================================================
# Other Optimizations
# ============================================================================
# Larger packets for big INSERTs
max_allowed_packet = 1G

# Connection limit
max_connections = 200

# Disable query cache (not helpful for bulk import)
query_cache_type = 0
query_cache_size = 0

# ============================================================================
# TEMP: For initial load only (REMOVE after import!)
# ============================================================================
# These are DANGEROUS for production but fast for import:
skip-innodb-doublewrite
```

**After editing config:**
```bash
sudo systemctl restart mariadb
```

---

## Phase 1: MySQL Import Session Optimizations

### 2. Session-Level Settings (CRITICAL!)

Run these BEFORE importing each database:

```sql
-- Disable ALL checks and logging
SET GLOBAL innodb_flush_log_at_trx_commit = 0;
SET GLOBAL innodb_doublewrite = 0;

-- Session settings (these were MISSING in current script!)
SET SESSION foreign_key_checks = 0;      -- Skip FK validation
SET SESSION unique_checks = 0;           -- Skip unique checks (BIGGEST WIN!)
SET SESSION sql_log_bin = 0;             -- Skip binary logging
SET SESSION autocommit = 0;              -- Single transaction per file

-- Buffer sizes
SET SESSION bulk_insert_buffer_size = 536870912;  -- 512MB
SET SESSION sort_buffer_size = 67108864;          -- 64MB
SET SESSION read_buffer_size = 2097152;           -- 2MB
```

### 3. Table Structure Optimizations

**Option A: Import without indexes, add later (FASTEST)**

```sql
-- Before import: Create table without indexes
CREATE TABLE scores (
    id BIGINT UNSIGNED NOT NULL,
    user_id INT UNSIGNED NOT NULL,
    -- ... other columns ...
    data JSON DEFAULT NULL
    -- NO INDEXES!
) ENGINE=InnoDB;

-- Import data here (much faster!)

-- After import: Add indexes
ALTER TABLE scores ADD PRIMARY KEY (id);
ALTER TABLE scores ADD INDEX idx_user_id (user_id);
-- etc.
```

**Option B: Disable keys during import (Good middle ground)**

Already in your SQL dumps via:
```sql
ALTER TABLE `scores` DISABLE KEYS;
-- INSERT statements...
ALTER TABLE `scores` ENABLE KEYS;
```

---

## Phase 2: Import Process Optimizations

### 4. Import Order Strategy

Import from **smallest to largest** tables:

```bash
# This allows buffer pool to warm up gradually
tables=(
    osu_counts                  # 2KB
    osu_difficulty_attribs      # 3KB
    osu_beatmap_performance_blacklist  # 2KB
    sample_users               # 256KB
    osu_user_stats             # 2MB
    osu_beatmapsets            # 42MB
    osu_beatmaps               # 52MB
    osu_beatmap_failtimes      # 134MB
    osu_beatmap_difficulty     # 470MB
    osu_user_beatmap_playcount # 1.6GB
    osu_beatmap_difficulty_attribs  # 3.9GB
    osu_scores_high            # 6.2GB
    scores                     # 13GB (largest, import last)
)
```

### 5. Parallel Import (Advanced)

For truly massive imports, import multiple small/medium tables in parallel:

```bash
# Import 3 small tables simultaneously
mysql db < table1.sql &
mysql db < table2.sql &
mysql db < table3.sql &
wait
```

### 6. Filesystem Optimizations

```bash
# Mount MySQL data directory with noatime (reduces I/O)
sudo mount -o remount,noatime /var/lib/mysql

# Or in /etc/fstab:
# /dev/nvme0n1p4 /var/lib/mysql ext4 noatime,nodiratime 0 2

# Use deadline I/O scheduler for SSDs
echo deadline | sudo tee /sys/block/nvme0n1/queue/scheduler
```

---

## Phase 3: Alternative Approaches (Skip MySQL!)

### 7. Direct SQL → Parquet (Fastest Overall)

Since you have DuckDB, consider **bypassing MySQL entirely**:

```python
# Use DuckDB's streaming CSV/Parquet capabilities
import duckdb

conn = duckdb.connect()

# Option A: If you can convert SQL to CSV first
conn.execute("""
    COPY (
        SELECT * FROM read_csv_auto('scores.csv')
    ) TO 'scores.parquet' (FORMAT PARQUET)
""")

# Option B: Use DuckDB's MySQL scanner directly
conn.execute("""
    INSTALL mysql;
    LOAD mysql;
    ATTACH 'host=localhost user=root database=osu' AS mysql (TYPE mysql);
    
    COPY (SELECT * FROM mysql.scores) 
    TO 'scores.parquet' (FORMAT PARQUET)
""")
```

### 8. Use mydumper/myloader (Industrial Strength)

```bash
# Install mydumper
sudo apt-get install mydumper

# Parallel export from source (if you have MySQL access)
mydumper -h source_host -u root -B osu -t 8 -o /backup/dir

# Parallel import
myloader -h localhost -u root -B osu_import -t 8 -d /backup/dir
```

### 9. Split Large SQL Files

If `scores.sql` is too large as a single file:

```bash
# Split into 100MB chunks
csplit -z scores.sql '/^-- Table structure for table/' '{*}' -f scores_part_

# Import chunks sequentially
for f in scores_part_*; do
    mysql db < "$f"
done
```

---

## Expected Performance with All Optimizations

| Table | Current | Optimized | Speedup |
|-------|---------|-----------|---------|
| osu_beatmap_difficulty (448MB) | 369s (1.2 MB/s) | ~75s (6 MB/s) | **5x** |
| scores (13GB) | ~2 hours (est.) | ~20-30 min | **4-6x** |
| **Total 24GB** | ~3-4 hours | **~40-60 min** | **3-5x** |

---

## Quick Reference: Optimized Import Command

```bash
#!/bin/bash
# optimized_import.sh

DB_NAME="osu_import"

# 1. Apply server config (my.cnf changes)
sudo systemctl restart mariadb

# 2. Create database with optimizations
mysql -u root << EOF
DROP DATABASE IF EXISTS ${DB_NAME};
CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4;

USE ${DB_NAME};

-- Apply session optimizations
SET GLOBAL innodb_flush_log_at_trx_commit = 0;
SET GLOBAL innodb_doublewrite = 0;
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;
SET SESSION sql_log_bin = 0;
SET SESSION autocommit = 0;
EOF

# 3. Import each file
for sql_file in *.sql; do
    echo "Importing $sql_file..."
    
    # Apply optimizations before each import
    mysql -u root ${DB_NAME} << 'SETTINGS'
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;
SET SESSION sql_log_bin = 0;
SETTINGS
    
    # Import
    mysql -u root ${DB_NAME} < "$sql_file"
    
    echo "Completed $sql_file"
done

# 4. Re-enable checks and optimize
echo "Re-enabling constraints and optimizing tables..."
mysql -u root ${DB_NAME} << EOF
SET SESSION foreign_key_checks = 1;
SET SESSION unique_checks = 1;

-- Optimize all tables
$(mysql -u root -N -e "SELECT CONCAT('OPTIMIZE TABLE ', table_name, ';') 
                        FROM information_schema.tables 
                        WHERE table_schema = '${DB_NAME}';")
EOF

echo "Import complete!"
```

---

## Summary: Top 5 Optimizations

1. **UNIQUE_CHECKS = 0** - Skips unique constraint validation (BIGGEST WIN)
2. **FOREIGN_KEY_CHECKS = 0** - Skips FK validation
3. **innodb_buffer_pool_size = 10G** - Keep tables in memory
4. **Import without indexes, add after** - Avoids index maintenance during load
5. **innodb_flush_log_at_trx_commit = 0** - Don't flush on every commit

**Current script is missing #1 and #2, which would give you ~3-4x speedup!**
