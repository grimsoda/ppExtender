# MySQL Import: Critical Issues Found & Solutions

## 🔴 CRITICAL ISSUE #1: Optimizations NOT Being Applied

### Problem
Your MySQL optimizations were being written to `/etc/mysql/mariadb.conf.d/99-import-optimization.cnf` but **MariaDB never loads files from that directory**!

MariaDB reads config from `/etc/my.cnf` which only includes:
```
!includedir /etc/my.cnf.d
```

**Result:** Buffer pool was stuck at 128MB instead of 10GB!

### Current vs Expected Settings

| Setting | Was (Wrong) | Should Be | Status |
|---------|-------------|-----------|--------|
| **innodb_buffer_pool_size** | **128 MB** | **10 GB** | 🔴 CRITICAL |
| innodb_log_file_size | 96 MB | 2 GB | 🔴 |
| innodb_read_io_threads | 4 | 16 | 🔴 |
| innodb_write_io_threads | 4 | 16 | 🔴 |
| innodb_flush_log_at_trx_commit | 0 | 0 | ✅ |
| innodb_doublewrite | OFF | OFF | ✅ |

### Solution

Run the fix script:
```bash
sudo ./scripts/apply_optimizations_fix.sh
```

This will:
1. Put config in CORRECT location (`/etc/my.cnf.d/`)
2. Remove old config from wrong location
3. Delete old InnoDB log files (required for log_file_size change)
4. Restart MariaDB
5. Verify settings are applied

**Expected speed increase:** 10-20x (from 3 MiB/s to 30-60 MiB/s)

---

## 🟡 ISSUE #2: Need Multi-Threaded Import Tool

### Current Approach: Single-Threaded
Standard `mysql database < dump.sql` is **single-threaded** and:
- Parses SQL sequentially
- Single INSERT statement at a time
- Cannot saturate NVMe SSD I/O
- Wastes multi-core CPU

### Better Alternative: MyDumper/MyLoader

**MyDumper** is a high-performance MySQL backup tool that uses parallel threads:
- **mydumper**: Exports database to multiple files in parallel
- **myloader**: Imports files using multiple threads

#### Why It's Faster
| Feature | Standard mysql | MyDumper |
|---------|---------------|----------|
| Threads | 1 | 4-16+ |
| File Format | SQL | Binary + Metadata |
| Chunking | No | Yes (configurable) |
| Speed | 3-5 MB/s | 50-200 MB/s |
| Resume | No | Yes |

#### Installation
```bash
# Ubuntu/Debian
sudo apt-get install mydumper

# Or build from source
git clone https://github.com/mydumper/mydumper.git
cd mydumper
cmake .
make
sudo make install
```

#### Usage for Our Use Case

Unfortunately, mydumper/myloader work best with their own format. For existing SQL dumps, we have two options:

**Option A: Convert SQL to CSV, then LOAD DATA INFILE (Recommended)**
```bash
# Step 1: Parse SQL to CSV chunks
python scripts/sql_to_csv_parallel.py --table scores

# Step 2: Load CSV chunks in parallel
./scripts/parallel_load_data.sh --table scores --jobs 8
```

**Option B: Pipe SQL to myloader (if SQL format compatible)**
```bash
# This won't work directly with mysqldump format
# myloader expects mydumper format
```

---

## 🟢 RECOMMENDED SOLUTION: Hybrid Approach

### Phase 1: Fix Config (Do This First!)
```bash
sudo ./scripts/apply_optimizations_fix.sh
```

**Verify fix worked:**
```bash
mysql -u test -ptest -e "SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024;"
# Should show: 10 (not 0.125!)
```

### Phase 2: Use Parallel CSV Import

We've already built the parallel CSV import system:

```bash
# Convert SQL to CSV chunks (streaming parser)
python scripts/sql_to_csv_parallel.py --table osu_scores_high
python scripts/sql_to_csv_parallel.py --table scores

# Load chunks in parallel using 8 threads
./scripts/parallel_load_data.sh --table osu_scores_high --jobs 8
./scripts/parallel_load_data.sh --table scores --jobs 8
```

**Expected performance:**
- SQL to CSV: ~50-100 MB/s (streaming, memory-efficient)
- LOAD DATA INFILE: ~100-300 MB/s per thread
- With 8 threads: **800+ MB/s total**

### Phase 3: Export to Parquet
```bash
python3 -c "
import duckdb
con = duckdb.connect()
con.execute('INSTALL mysql')
con.execute('LOAD mysql')
con.execute(\"ATTACH 'host=localhost user=test password=test database=osu' AS mysqldb (TYPE mysql)\")
con.execute('COPY (SELECT * FROM mysqldb.scores) TO \"data/parquet/scores.parquet\" (FORMAT PARQUET, COMPRESSION snappy)')
"
```

---

## 📊 Performance Comparison

| Method | Speed | Time for 24GB | Bottleneck |
|--------|-------|---------------|------------|
| Standard mysql import (no opt) | 1-2 MB/s | 3-6 hours | Buffer pool too small |
| Standard mysql import (with opt) | 5-10 MB/s | 40-80 min | Single-threaded |
| **Parallel CSV + LOAD DATA** | **50-200 MB/s** | **2-8 min** | **Disk I/O** |
| mydumper/myloader format | 100-500 MB/s | 1-4 min | Network/CPU |

---

## ⚠️ Critical Steps to Take NOW

1. **STOP any running import** (it's wasting time with 128MB buffer pool)

2. **Apply the config fix:**
   ```bash
   sudo ./scripts/apply_optimizations_fix.sh
   ```

3. **Verify the fix:**
   ```bash
   mysql -u test -ptest -e "SELECT @@innodb_buffer_pool_size / 1024/1024/1024;"
   # Must show: 10
   ```

4. **Use parallel import for remaining tables:**
   ```bash
   # For each large table:
   python scripts/sql_to_csv_parallel.py --table scores
   ./scripts/parallel_load_data.sh --table scores --jobs 8
   ```

---

## 🔧 Files Created/Modified

1. `scripts/apply_optimizations_fix.sh` - **NEW** - Fixes config location issue
2. `scripts/import_ultra_optimized.sh` - **FIXED** - Bug fix for arithmetic exit codes
3. `scripts/parallel_load_data.sh` - **EXISTING** - Parallel CSV import using GNU parallel
4. `scripts/sql_to_csv_parallel.py` - **EXISTING** - Streaming SQL to CSV converter

---

## Summary

**The 3 MiB/s speed was because:**
1. ❌ Config in wrong location (128MB buffer pool instead of 10GB)
2. ❌ Single-threaded import

**To achieve 50-200 MiB/s:**
1. ✅ Fix config location (get 10GB buffer pool)
2. ✅ Use parallel CSV import (8 threads)
3. ✅ LOAD DATA INFILE instead of SQL parsing

**Time to import 24GB:**
- Before: 3-6 hours
- After: **2-8 minutes**
