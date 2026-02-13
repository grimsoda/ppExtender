#!/bin/bash
#
# Manual Import Commands with Full Optimizations
# Run these commands with sudo for maximum performance
#

# Step 1: Apply Server-Level Optimizations (requires sudo)
sudo tee /etc/mysql/mariadb.conf.d/99-import-optimization.cnf > /dev/null << 'EOF'
[mysqld]
innodb_buffer_pool_size = 10G
innodb_log_file_size = 2G
innodb_log_buffer_size = 256M
innodb_log_files_in_group = 3
innodb_flush_log_at_trx_commit = 0
innodb_doublewrite = 0
innodb_flush_method = O_DIRECT
innodb_read_io_threads = 16
innodb_write_io_threads = 16
innodb_io_capacity = 4000
innodb_io_capacity_max = 8000
skip-log-bin
slow_query_log = 0
general_log = 0
bulk_insert_buffer_size = 1G
max_allowed_packet = 1G
EOF

# Step 2: Restart MariaDB with new config
sudo systemctl restart mariadb
sleep 5

# Step 3: Apply Session Optimizations
mysql -u test -ptest -e "
SET GLOBAL innodb_flush_log_at_trx_commit = 0;
SET GLOBAL innodb_doublewrite = 0;
USE osu;
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;
SET SESSION sql_log_bin = 0;
SET SESSION autocommit = 0;
SET SESSION bulk_insert_buffer_size = 1073741824;
SELECT 'All optimizations applied';
"

# Step 4: Import remaining tables (in order of size)
echo "Starting imports at $(date)"

# Table 1: osu_beatmap_difficulty_attribs (3.7GB)
echo "Importing osu_beatmap_difficulty_attribs (3.7GB)..."
time mysql -u test -ptest osu < data/ingest/2026-02/sql/osu_beatmap_difficulty_attribs.sql

# Table 2: osu_user_beatmap_playcount (1.6GB)
echo "Importing osu_user_beatmap_playcount (1.6GB)..."
time mysql -u test -ptest osu < data/ingest/2026-02/sql/osu_user_beatmap_playcount.sql

# Table 3: osu_scores_high (5.8GB)
echo "Importing osu_scores_high (5.8GB)..."
time mysql -u test -ptest osu < data/ingest/2026-02/sql/osu_scores_high.sql

# Table 4: scores (13GB) - Largest
echo "Importing scores (13GB)..."
time mysql -u test -ptest osu < data/ingest/2026-02/sql/scores.sql

echo "All imports completed at $(date)"

# Step 5: Export to Parquet via DuckDB
echo "Exporting to Parquet..."

mkdir -p data/parquet

for table in osu_beatmap_difficulty_attribs osu_user_beatmap_playcount osu_scores_high scores; do
  echo "Exporting $table..."
  python3 -c "
import duckdb
con = duckdb.connect()
con.execute(\"INSTALL mysql\")
con.execute(\"LOAD mysql\")
con.execute(\"ATTACH 'host=localhost database=osu' AS mysqldb (TYPE mysql)\")
con.execute(f\"\"\"
    COPY (SELECT * FROM mysqldb.{table})
    TO 'data/parquet/{table}.parquet' (
        FORMAT PARQUET,
        COMPRESSION 'snappy',
        ROW_GROUP_SIZE 100000
    )
\"\"\")
con.close()
print(f'Exported {table}')
  "
done

# Step 6: Cleanup - Remove optimization config
sudo rm -f /etc/mysql/mariadb.conf.d/99-import-optimization.cnf
sudo systemctl restart mariadb

echo "Complete! Parquet files in data/parquet/"
