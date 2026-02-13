#!/bin/bash
#
# FINAL IMPORT - All 13 tables with proper optimizations
# Run with sudo
#

set -euo pipefail

PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
PARQUET_DIR="${DATA_DIR}/parquet"
LOG_DIR="${PROJECT_DIR}/logs"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"

echo "========================================"
echo "FINAL IMPORT - All Tables"
echo "========================================"
echo ""

# Phase 0: Apply MariaDB 12.x compatible optimizations
echo "[Phase 0] Applying MariaDB 12.x optimizations..."
echo "Creating config at /etc/my.cnf.d/..."

cat > /etc/my.cnf.d/99-import-optimization.cnf <>/dev/null

# Wait for MariaDB
sleep 3
retries=0
while ! mysqladmin ping --silent 2>/dev/null; do
    sleep 1
    ((retries++))
    if [[ $retries -gt 30 ]]; then
        echo "ERROR: MariaDB failed to start"
        exit 1
    fi
done

echo "✓ MariaDB started successfully"
echo ""

# Verify optimizations
echo "Verifying buffer pool size:"
mysql -u${DB_USER} -p${DB_PASS} -e "SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024 as 'Buffer Pool (GB)';" 2>/dev/null

if [[ $(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT @@innodb_buffer_pool_size;" 2>/dev/null) -lt 10000000000 ]]; then
    echo "WARNING: Buffer pool is less than 10GB!"
    echo "Current value: $(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024;" 2>/dev/null) GB"
    echo "Config may not be loaded. Check /etc/my.cnf.d/99-import-optimization.cnf"
fi

echo ""
echo "========================================"

# Create database and user
echo "[Phase 1] Creating database and user..."
mysql -u root -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME} CHARACTER SET utf8mb4;" 2>/dev/null || true
mysql -u root -e "CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASS}';" 2>/dev/null || true
mysql -u root -e "GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';" 2>/dev/null || true
mysql -u root -e "FLUSH PRIVILEGES;" 2>/dev/null || true

echo "✓ Database and user ready"
echo ""

# Import all tables (smallest to largest)
echo "[Phase 2] Importing all tables..."
echo ""

declare -a tables=(
    "osu_counts:0.003"
    "osu_difficulty_attribs:0.001"
    "osu_beatmap_performance_blacklist:0.001"
    "sample_users:0.3"
    "osu_user_stats:2"
    "osu_beatmapsets:42"
    "osu_beatmaps:52"
    "osu_beatmap_failtimes:134"
    "osu_beatmap_difficulty:470"
    "osu_user_beatmap_playcount:1600"
    "osu_beatmap_difficulty_attribs:3700"
    "osu_scores_high:5800"
    "scores:13000"
)

mkdir -p "$LOG_DIR"

for table_info in "${tables[@]}"; do
    IFS=':' read -r table size_mb <<< "$table_info"
    sql_file="${SQL_DIR}/${table}.sql"
    
    echo "----------------------------------------"
    echo "Importing: $table (${size_mb}MB)"
    
    if [[ ! -f "$sql_file" ]]; then
        echo "  ✗ SQL file not found: $sql_file"
        continue
    fi
    
    # Apply session optimizations for large tables
    if [[ $(echo "$size_mb > 100" | bc) -eq 1 ]]; then
        echo "  Applying session optimizations..."
        mysql -u${DB_USER} -p${DB_PASS} -e "
            SET GLOBAL innodb_flush_log_at_trx_commit = 0;
            SET GLOBAL innodb_doublewrite = 0;
            USE ${DB_NAME};
            SET SESSION foreign_key_checks = 0;
            SET SESSION unique_checks = 0;
            SET SESSION sql_log_bin = 0;
            SET SESSION autocommit = 0;
            SET SESSION bulk_insert_buffer_size = 1073741824;
        " 2>/dev/null
    fi
    
    # Import
    start_time=$(date +%s)
    
    if mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} < "$sql_file" 2> "${LOG_DIR}/import_${table}_$(date +%Y%m%d_%H%M%S).log"; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        actual_size_mb=$(du -m "$sql_file" | cut -f1)
        speed=$(echo "scale=2; $actual_size_mb / $duration" | bc 2>/dev/null || echo "N/A")
        
        row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
        
        echo "  ✓ Imported in ${duration}s (${speed} MB/s) - ${row_count} rows"
    else
        echo "  ✗ Failed to import"
    fi
done

echo ""
echo "========================================"

# Export to Parquet
echo "[Phase 3] Exporting to Parquet..."

mkdir -p "$PARQUET_DIR"

tables_list=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SHOW TABLES FROM ${DB_NAME};" 2>/dev/null)

for table in $tables_list; do
    echo "Exporting $table..."
    
    output_file="${PARQUET_DIR}/${table}.parquet"
    
    if python3 -c "
import duckdb
import sys
try:
    con = duckdb.connect()
    con.execute('INSTALL mysql')
    con.execute('LOAD mysql')
    con.execute(\"ATTACH 'host=localhost user=${DB_USER} password=${DB_PASS} database=${DB_NAME}' AS mysqldb (TYPE mysql)\")
    con.execute(f\"\"\"
        COPY (SELECT * FROM mysqldb.{table})
        TO '{output_file}' (
            FORMAT PARQUET,
            COMPRESSION 'snappy',
            ROW_GROUP_SIZE 100000
        )
    \"\"\")
    con.close()
    sys.exit(0)
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
    " 2> "${LOG_DIR}/export_${table}.log"; then
        
        file_size=$(stat -c%s "$output_file" 2>/dev/null || echo "0")
        size_mb=$((file_size / 1024 / 1024))
        echo "  ✓ Exported (${size_mb}MB)"
    else
        echo "  ✗ Failed to export"
    fi
done

echo ""
echo "========================================"

# Cleanup
echo "[Phase 4] Cleanup..."
rm -f /etc/my.cnf.d/99-import-optimization.cnf
systemctl restart mariadb
echo "✓ Restored default MariaDB settings"

echo ""
echo "========================================"
echo "IMPORT COMPLETE"
echo "========================================"
echo "Parquet files: ${PARQUET_DIR}/"
echo "Logs: ${LOG_DIR}/"
