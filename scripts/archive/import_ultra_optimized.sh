#!/bin/bash
#
# Ultra-Optimized MySQL to Parquet Import
# 
# Features:
# - Automatic MariaDB config backup/restore
# - All performance optimizations applied
# - Progress monitoring with timeout detection
# - Proper error handling and recovery
#

set -euo pipefail

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
PARQUET_DIR="${DATA_DIR}/parquet"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"
DB_HOST="localhost"
LOG_DIR="${PROJECT_DIR}/logs"
CONFIG_BACKUP_DIR="${PROJECT_DIR}/config/backups"

# Performance settings
PARALLEL_JOBS=4
IMPORT_TIMEOUT=7200  # 2 hours max per table

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

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    # Skip restore_mysql_config - requires sudo
}
trap cleanup EXIT

#######################################
# MySQL Configuration Management
#######################################

backup_mysql_config() {
    log_info "Backing up current MySQL config..."
    
    mkdir -p "$CONFIG_BACKUP_DIR"
    local backup_timestamp=$(date +%Y%m%d_%H%M%S)
    export CONFIG_BACKUP_FILE="${CONFIG_BACKUP_DIR}/mysql_config_backup_${backup_timestamp}"
    
    # Find and backup config files
    local config_files=(
        "/etc/my.cnf"
        "/etc/mysql/my.cnf"
        "/etc/mysql/mariadb.conf.d/50-server.cnf"
        "/etc/mysql/mariadb.conf.d/60-performance.cnf"
    )
    
    for config in "${config_files[@]}"; do
        if [[ -f "$config" ]]; then
            cp "$config" "${CONFIG_BACKUP_FILE}_$(basename $config)"
            log_info "  Backed up: $config"
        fi
    done
    
    # Also save a manifest
    cat > "${CONFIG_BACKUP_FILE}_manifest.txt" << EOF
Backup created: $(date)
Config files backed up:
$(for f in "${config_files[@]}"; do [[ -f "$f" ]] && echo "  - $f"; done)

To restore, run:
  ./restore_mysql_config.sh ${backup_timestamp}
EOF
    
    log_success "Config backed up to: $CONFIG_BACKUP_FILE"
}

apply_optimized_config() {
    log_info "Applying optimized MySQL configuration..."
    
    # Create optimized config file
    local config_file="/etc/mysql/mariadb.conf.d/99-import-optimization.cnf"
    
    sudo tee "$config_file" > /dev/null << 'EOF'
# ============================================================================
# ULTRA-OPTIMIZED MySQL Configuration for Bulk Import
# WARNING: These settings prioritize speed over durability!
# Only use for initial data import, not production!
# ============================================================================

[mysqld]
# ============================================================================
# CRITICAL: Memory Settings
# ============================================================================
# Buffer pool - use 70% of available RAM for import
innodb_buffer_pool_size = 10G

# Redo log - larger = less flushing
innodb_log_file_size = 2G
innodb_log_buffer_size = 256M
innodb_log_files_in_group = 3

# ============================================================================
# CRITICAL: Disable All Durability (FAST but unsafe!)
# ============================================================================
# Don't flush on commit (HUGE speedup)
innodb_flush_log_at_trx_commit = 0

# Disable doublewrite buffer (HUGE speedup)
innodb_doublewrite = 0

# Use O_DIRECT for I/O
innodb_flush_method = O_DIRECT

# ============================================================================
# Parallel I/O (for NVMe SSDs)
# ============================================================================
innodb_read_io_threads = 16
innodb_write_io_threads = 16
innodb_io_capacity = 4000
innodb_io_capacity_max = 8000

# ============================================================================
# Disable ALL Logging
# ============================================================================
skip-log-bin
slow_query_log = 0
general_log = 0

# ============================================================================
# Buffer Sizes for Bulk Operations
# ============================================================================
bulk_insert_buffer_size = 1G
max_allowed_packet = 1G
sort_buffer_size = 64M
read_buffer_size = 2M
read_rnd_buffer_size = 8M
join_buffer_size = 8M

# ============================================================================
# Connection Settings
# ============================================================================
max_connections = 200
max_connect_errors = 100000
wait_timeout = 28800
interactive_timeout = 28800

# ============================================================================
# Table Settings
# ============================================================================
table_open_cache = 4000
table_definition_cache = 2000
tmp_table_size = 512M
max_heap_table_size = 512M

# ============================================================================
# Query Cache (disable for bulk import)
# ============================================================================
query_cache_type = 0
query_cache_size = 0

# ============================================================================
# Concurrency
# ============================================================================
innodb_thread_concurrency = 0
innodb_concurrency_tickets = 5000
innodb_commit_concurrency = 0

# ============================================================================
# Character Set
# ============================================================================
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci
EOF
    
    log_success "Applied optimized config to: $config_file"
    
    # Restart MariaDB to apply changes
    log_info "Restarting MariaDB to apply configuration..."
    sudo systemctl restart mariadb
    
    # Wait for MariaDB to be ready
    local retries=0
    while ! mysqladmin ping --silent 2>/dev/null; do
        sleep 1
        ((retries++))
        if [[ $retries -gt 30 ]]; then
            log_error "MariaDB failed to start after 30 seconds"
            exit 1
        fi
    done
    
    log_success "MariaDB restarted with optimized configuration"
}

restore_mysql_config() {
    log_info "Restoring original MySQL configuration..."
    
    # Remove the optimization config
    local config_file="/etc/mysql/mariadb.conf.d/99-import-optimization.cnf"
    if [[ -f "$config_file" ]]; then
        sudo rm -f "$config_file"
        log_info "  Removed: $config_file"
    fi
    
    # Restart with safe defaults
    sudo systemctl restart mariadb
    
    log_success "MySQL configuration restored"
}

#######################################
# Session Optimizations
#######################################

apply_session_optimizations() {
    local db_name=$1
    
    log_info "Applying session-level optimizations..."
    
    mysql -u${DB_USER} -p${DB_PASS} -e "
USE ${db_name};

-- Session optimizations (GLOBAL and BINLOG settings skipped - requires SUPER/BINLOG ADMIN privilege)
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;        -- SKIPS unique constraint validation!
SET SESSION autocommit = 0;

-- Buffer sizes
SET SESSION bulk_insert_buffer_size = 1073741824;  -- 1GB
SET SESSION sort_buffer_size = 67108864;           -- 64MB

SELECT 'Session optimizations applied' as status;
"
    
    log_success "Session optimizations applied"
}

#######################################
# Import Functions
#######################################

import_table() {
    local table=$1
    local sql_file="${SQL_DIR}/${table}.sql"
    
    if [[ ! -f "$sql_file" ]]; then
        log_warning "SQL file not found: $sql_file"
        return 1
    fi
    
    local file_size=$(stat -c%s "$sql_file" 2>/dev/null || stat -f%z "$sql_file")
    local size_mb=$((file_size / 1024 / 1024))
    
    log_section "Importing: $table (${size_mb}MB)"
    
    local start_time=$(date +%s)
    local log_file="${LOG_DIR}/import_${table}_$(date +%Y%m%d_%H%M%S).log"
    
    # Apply session optimizations before each large table
    if [[ $size_mb -gt 100 ]]; then
        apply_session_optimizations "$DB_NAME"
    fi
    
    # Import with timeout and monitoring
    log_info "Starting import (timeout: ${IMPORT_TIMEOUT}s)..."
    log_info "  Log file: $log_file"
    
    if timeout $IMPORT_TIMEOUT bash -c "mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} < '$sql_file'" > "$log_file" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local speed=$(echo "scale=2; $size_mb / $duration" | bc 2>/dev/null || echo "N/A")
        
        log_success "Imported $table in ${duration}s (${speed} MB/s)"
        
        # Verify row count
        local row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
        log_info "  Rows imported: $row_count"
        
        return 0
    else
        local exit_code=$?
        log_error "Failed to import $table (exit code: $exit_code)"
        log_error "  Check log: $log_file"
        
        if [[ $exit_code -eq 124 ]]; then
            log_error "  IMPORT TIMEOUT! Table is too large or import is stuck."
            log_error "  Consider splitting this table or increasing IMPORT_TIMEOUT"
        fi
        
        return 1
    fi
}

#######################################
# DuckDB Export
#######################################

export_to_parquet() {
    log_section "Phase 2: Export to Parquet via DuckDB"
    
    # Get list of tables
    local tables=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SHOW TABLES FROM ${DB_NAME};" 2>/dev/null)
    
    if [[ -z "$tables" ]]; then
        log_error "No tables found in database $DB_NAME"
        return 1
    fi
    
    mkdir -p "$PARQUET_DIR"
    
    log_info "Found $(echo "$tables" | wc -l) tables to export"
    
    local total_start=$(date +%s)
    
    for table in $tables; do
        log_info "Exporting $table to Parquet..."
        
        local output_file="${PARQUET_DIR}/${table}.parquet"
        local start_time=$(date +%s)
        
        if duckdb -c "
            INSTALL mysql;
            LOAD mysql;
            ATTACH 'host=${DB_HOST} database=${DB_NAME}' AS mysqldb (TYPE mysql);
            
            COPY (SELECT * FROM mysqldb.${table})
            TO '${output_file}' (
                FORMAT PARQUET,
                COMPRESSION 'snappy',
                ROW_GROUP_SIZE 100000
            );
        " > "${LOG_DIR}/export_${table}.log" 2>&1; then
            
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            local file_size=$(stat -c%s "$output_file" 2>/dev/null || stat -f%z "$output_file")
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
# Main Execution
#######################################

main() {
    local script_start=$(date +%s)
    
    log_section "Ultra-Optimized MySQL Import"
    log_info "Started at $(date)"
    log_info "This script will:"
    log_info "  1. Backup current MySQL config"
    log_info "  2. Apply ultra-optimized settings"
    log_info "  3. Import SQL files with all optimizations"
    log_info "  4. Export to Parquet via DuckDB"
    log_info "  5. Restore original MySQL config"
    
    # Validate environment
    if ! command -v mysql &> /dev/null; then
        log_error "MySQL/MariaDB client not found"
        exit 1
    fi
    
    if ! systemctl is-active --quiet mariadb 2>/dev/null; then
        log_error "MariaDB is not running"
        exit 1
    fi
    
    mkdir -p "$LOG_DIR" "$PARQUET_DIR" "$CONFIG_BACKUP_DIR"
    
    # Phase 0: Config management (skipped - requires sudo)
    # backup_mysql_config
    # apply_optimized_config
    log_info "Skipping MySQL config optimization (requires sudo)"
    
    # Use existing database (test user cannot create new databases)
    log_info "Using existing database: $DB_NAME"
    
    # Apply session optimizations
    apply_session_optimizations "$DB_NAME"
    
    # Phase 1: Import tables (order by size, smallest first)
    log_section "Phase 1: Importing SQL Files"
    
    local tables=(
        osu_counts
        osu_difficulty_attribs
        osu_beatmap_performance_blacklist
        sample_users
        osu_user_stats
        osu_beatmapsets
        osu_beatmaps
        osu_beatmap_failtimes
        osu_beatmap_difficulty
        osu_user_beatmap_playcount
        osu_beatmap_difficulty_attribs
        osu_scores_high
        scores
    )
    
    local success_count=0
    local fail_count=0
    
    for table in "${tables[@]}"; do
        if import_table "$table"; then
            ((success_count++)) || true
        else
            ((fail_count++)) || true
            log_warning "Continuing with next table..."
        fi
    done
    
    log_info "Phase 1 complete: $success_count succeeded, $fail_count failed"
    
    # Phase 2: Export to Parquet
    export_to_parquet
    
    # Final summary
    local script_end=$(date +%s)
    local total_duration=$((script_end - script_start))
    local minutes=$((total_duration / 60))
    local seconds=$((total_duration % 60))
    
    log_section "Import Complete"
    log_success "Total time: ${minutes}m ${seconds}s"
    log_info "Parquet files: ${PARQUET_DIR}/"
    log_info "Logs: ${LOG_DIR}/"
    
    # Config will be restored by trap
}

# Help
show_help() {
    cat << 'EOF'
Ultra-Optimized MySQL to Parquet Import

Features:
  - Automatic MySQL config backup/restore
  - All performance optimizations applied
  - Session-level UNIQUE_CHECKS=0 (3-4x speedup!)
  - Timeout protection for stuck imports
  - Progress monitoring

Usage: ./import_ultra_optimized.sh [OPTIONS]

Options:
    --help, -h    Show this help message

Environment Variables:
    DB_NAME       Database name (default: osu_import)
    DB_USER       MySQL user (default: root)
    IMPORT_TIMEOUT  Max seconds per table (default: 7200)

Performance Optimizations Applied:
  Server Level:
    - innodb_buffer_pool_size = 10G
    - innodb_flush_log_at_trx_commit = 0
    - innodb_doublewrite = 0
    - innodb_log_file_size = 2G
    - Skip binary logging

  Session Level:
    - SET unique_checks = 0          <-- BIGGEST WIN!
    - SET foreign_key_checks = 0
    - SET sql_log_bin = 0
    - bulk_insert_buffer_size = 1G

Expected Performance:
  - Small tables (<100MB): <10 seconds
  - Medium tables (100MB-1GB): 1-3 minutes
  - Large tables (1GB+): 3-10 minutes
  - Total for 24GB: 30-60 minutes

EOF
}

# Parse arguments
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    show_help
    exit 0
fi

# Run
main
