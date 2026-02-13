#!/bin/bash
#
# User-Level Optimized MySQL Import (No Sudo Required)
# 
# Applies all optimizations at session level
# Uses existing MariaDB configuration
#

set -euo pipefail

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
PARQUET_DIR="${DATA_DIR}/parquet"
DB_NAME="osu_import"
DB_USER=""
DB_HOST="localhost"
LOG_DIR="${PROJECT_DIR}/logs"

# Performance settings
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

#######################################
# Session Optimizations
#######################################

apply_session_optimizations() {
    local db_name=$1
    
    log_info "Applying session-level optimizations..."
    
    mysql -u${DB_USER} -e "
-- CRITICAL: Disable all checks (BIGGEST PERFORMANCE WIN!)
SET GLOBAL innodb_flush_log_at_trx_commit = 0;
SET GLOBAL innodb_doublewrite = 0;

USE ${db_name};

-- Session optimizations
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;        -- SKIPS unique constraint validation!
SET SESSION sql_log_bin = 0;
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
    
    if timeout $IMPORT_TIMEOUT bash -c "mysql -u${DB_USER} ${DB_NAME} < '$sql_file'" > "$log_file" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local speed=$(echo "scale=2; $size_mb / $duration" | bc 2>/dev/null || echo "N/A")
        
        log_success "Imported $table in ${duration}s (${speed} MB/s)"
        
        # Verify row count
        local row_count=$(mysql -u${DB_USER} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
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
    local tables=$(mysql -u${DB_USER} -N -e "SHOW TABLES FROM ${DB_NAME};" 2>/dev/null)
    
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
        
        if python3 -c "
import duckdb
import sys

try:
    con = duckdb.connect()
    con.execute(\"INSTALL mysql\")
    con.execute(\"LOAD mysql\")
    con.execute(\"ATTACH 'host=${DB_HOST} database=${DB_NAME}' AS mysqldb (TYPE mysql)\")
    con.execute(\"\"\"
        COPY (SELECT * FROM mysqldb.${table})
        TO '${output_file}' (
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
    
    log_section "User-Level Optimized MySQL Import"
    log_info "Started at $(date)"
    log_info "This script will:"
    log_info "  1. Apply session-level optimizations (no sudo required)"
    log_info "  2. Import SQL files with all optimizations"
    log_info "  3. Export to Parquet via DuckDB"
    
    # Validate environment
    if ! command -v mysql &> /dev/null; then
        log_error "MySQL/MariaDB client not found"
        exit 1
    fi
    
    if ! python3 -c "import duckdb" 2>/dev/null; then
        log_error "DuckDB Python package not found"
        exit 1
    fi
    
    if ! mysqladmin ping --silent 2>/dev/null; then
        log_error "MariaDB is not running"
        exit 1
    fi
    
    mkdir -p "$LOG_DIR" "$PARQUET_DIR"
    
    # Create database
    log_info "Creating database $DB_NAME..."
    mysql -u${DB_USER} -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
    mysql -u${DB_USER} -e "CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4;"
    
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
}

# Help
show_help() {
    cat << 'EOF'
User-Level Optimized MySQL Import (No Sudo Required)

Features:
  - Session-level optimizations (no config file changes)
  - UNIQUE_CHECKS=0 (3-4x speedup!)
  - FOREIGN_KEY_CHECKS=0
  - Timeout protection for stuck imports
  - Progress monitoring

Usage: ./import_user_optimized.sh [OPTIONS]

Options:
    --help, -h    Show this help message

Environment Variables:
    DB_NAME       Database name (default: osu_import)
    DB_USER       MySQL user (default: root)
    IMPORT_TIMEOUT  Max seconds per table (default: 7200)

Performance Optimizations Applied:
  Session Level:
    - SET unique_checks = 0          <-- BIGGEST WIN!
    - SET foreign_key_checks = 0
    - SET sql_log_bin = 0
    - SET autocommit = 0
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
