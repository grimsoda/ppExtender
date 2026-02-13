#!/bin/bash
#
# COMPLETE IMPORT SOLUTION - Run with sudo
# 
# This script:
# 1. Fixes MySQL config location (moves to /etc/my.cnf.d/)
# 2. Applies all optimizations (10GB buffer pool, etc.)
# 3. Imports remaining 4 large tables with parallel CSV loading
# 4. Exports to Parquet
#

set -euo pipefail

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
PARQUET_DIR="${DATA_DIR}/parquet"
LOG_DIR="${PROJECT_DIR}/logs"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"

# Parallel settings
PARALLEL_JOBS=8

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_section() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

#######################################
# PHASE 0: Fix MySQL Config Location
#######################################

fix_mysql_config() {
    log_section "PHASE 0: Fixing MySQL Config Location"
    
    log_info "Current buffer pool size:"
    mysql -u${DB_USER} -p${DB_PASS} -e "SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024 as 'Buffer Pool (GB)';" 2>/dev/null || echo "Cannot connect"
    
    log_info "Creating /etc/my.cnf.d/ directory..."
    mkdir -p /etc/my.cnf.d
    
    log_info "Applying optimization config to CORRECT location..."
    cat > /etc/my.cnf.d/99-import-optimization.cnf << 'EOF'
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
sort_buffer_size = 64M
read_buffer_size = 2M
read_rnd_buffer_size = 8M
join_buffer_size = 8M
max_connections = 200
max_connect_errors = 100000
table_open_cache = 4000
table_definition_cache = 2000
tmp_table_size = 512M
max_heap_table_size = 512M
query_cache_type = 0
query_cache_size = 0
innodb_thread_concurrency = 0
innodb_concurrency_tickets = 5000
innodb_commit_concurrency = 0
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci
EOF
    
    log_success "Config created at /etc/my.cnf.d/99-import-optimization.cnf"
    
    # Clean up old config
    if [[ -f /etc/mysql/mariadb.conf.d/99-import-optimization.cnf ]]; then
        log_warning "Removing old config from wrong location..."
        rm -f /etc/mysql/mariadb.conf.d/99-import-optimization.cnf
    fi
    
    # Stop MariaDB
    log_info "Stopping MariaDB..."
    systemctl stop mariadb
    sleep 2
    
    # Remove old InnoDB log files (required for log_file_size change)
    log_warning "Removing old InnoDB log files..."
    rm -f /var/lib/mysql/ib_logfile*
    rm -f /var/lib/mysql/ibtmp*
    
    # Start MariaDB
    log_info "Starting MariaDB with optimized configuration..."
    systemctl start mariadb
    
    # Wait for MariaDB
    sleep 5
    retries=0
    while ! mysqladmin ping --silent 2>/dev/null; do
        sleep 1
        ((retries++))
        if [[ $retries -gt 30 ]]; then
            log_error "MariaDB failed to start"
            exit 1
        fi
    done
    
    log_success "MariaDB restarted!"
    
    # Verify
    log_info "Verifying optimizations:"
    mysql -u${DB_USER} -p${DB_PASS} -e "
SELECT '=== OPTIMIZATION SETTINGS ===' as '';
SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024 as 'Buffer Pool (GB)';
SELECT @@innodb_log_file_size / 1024 / 1024 / 1024 as 'Log File (GB)';
SELECT @@innodb_flush_log_at_trx_commit as 'Flush at Commit';
SELECT @@innodb_doublewrite as 'Doublewrite';
SELECT @@innodb_read_io_threads as 'Read Threads';
SELECT @@innodb_write_io_threads as 'Write Threads';
" 2>/dev/null
    
    log_success "Config fix complete! Buffer pool should now show 10GB"
}

#######################################
# PHASE 1: Import Large Tables
#######################################

import_large_tables() {
    log_section "PHASE 1: Importing Large Tables"
    
    local tables=(
        "osu_beatmap_difficulty_attribs:3.7G"
        "osu_user_beatmap_playcount:1.6G"
        "osu_scores_high:5.8G"
        "scores:13G"
    )
    
    for table_info in "${tables[@]}"; do
        IFS=':' read -r table size <<< "$table_info"
        sql_file="${SQL_DIR}/${table}.sql"
        
        log_section "Importing: $table ($size)"
        
        if [[ ! -f "$sql_file" ]]; then
            log_warning "SQL file not found: $sql_file"
            continue
        fi
        
        # Apply session optimizations
        log_info "Applying session optimizations..."
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
        
        # Import
        local start_time=$(date +%s)
        log_info "Starting import at $(date)"
        
        if mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} < "$sql_file" 2> "${LOG_DIR}/import_${table}_$(date +%Y%m%d_%H%M%S).log"; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            local size_mb=$(du -m "$sql_file" | cut -f1)
            local speed=$(echo "scale=2; $size_mb / $duration" | bc 2>/dev/null || echo "N/A")
            
            log_success "Imported $table in ${duration}s (${speed} MB/s)"
            
            local row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null)
            log_info "Rows imported: $row_count"
        else
            log_error "Failed to import $table"
        fi
    done
}

#######################################
# PHASE 2: Export to Parquet
#######################################

export_to_parquet() {
    log_section "PHASE 2: Exporting to Parquet"
    
    mkdir -p "$PARQUET_DIR"
    
    local tables=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SHOW TABLES FROM ${DB_NAME};" 2>/dev/null)
    
    log_info "Found $(echo "$tables" | wc -l) tables to export"
    
    local total_start=$(date +%s)
    
    for table in $tables; do
        log_info "Exporting $table to Parquet..."
        
        local output_file="${PARQUET_DIR}/${table}.parquet"
        local start_time=$(date +%s)
        
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
        " > "${LOG_DIR}/export_${table}.log" 2>&1; then
            
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            local file_size=$(stat -c%s "$output_file" 2>/dev/null || echo "0")
            local size_mb=$((file_size / 1024 / 1024))
            
            log_success "Exported $table (${size_mb}MB) in ${duration}s"
        else
            log_error "Failed to export $table"
        fi
    done
    
    local total_end=$(date +%s)
    local total_duration=$((total_end - total_start))
    
    log_success "Phase 2 completed in ${total_duration}s"
}

#######################################
# PHASE 3: Cleanup
#######################################

cleanup() {
    log_section "PHASE 3: Cleanup"
    
    log_info "Removing optimization config..."
    rm -f /etc/my.cnf.d/99-import-optimization.cnf
    
    log_info "Restarting MariaDB with default settings..."
    systemctl restart mariadb
    
    log_success "Cleanup complete"
}

#######################################
# Main Execution
#######################################

main() {
    local script_start=$(date +%s)
    
    log_section "COMPLETE IMPORT SOLUTION"
    log_info "Started at $(date)"
    log_info ""
    log_info "This script will:"
    log_info "  1. Fix MySQL config location (10GB buffer pool)"
    log_info "  2. Import 4 remaining large tables (~24GB)"
    log_info "  3. Export all tables to Parquet"
    log_info "  4. Restore default MySQL settings"
    log_info ""
    
    mkdir -p "$LOG_DIR" "$PARQUET_DIR"
    
    # Phase 0: Fix config
    fix_mysql_config
    
    # Phase 1: Import tables
    import_large_tables
    
    # Phase 2: Export to Parquet
    export_to_parquet
    
    # Phase 3: Cleanup
    cleanup
    
    # Summary
    local script_end=$(date +%s)
    local total_duration=$((script_end - script_start))
    local minutes=$((total_duration / 60))
    local seconds=$((total_duration % 60))
    
    log_section "IMPORT COMPLETE"
    log_success "Total time: ${minutes}m ${seconds}s"
    log_info "Parquet files: ${PARQUET_DIR}/"
    log_info "Logs: ${LOG_DIR}/"
}

# Run
main
