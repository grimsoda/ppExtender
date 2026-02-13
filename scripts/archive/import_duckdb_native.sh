#!/bin/bash
#
# Direct MySQL to Parquet Import using DuckDB
# 
# Uses DuckDB's MySQL extension to connect directly to MariaDB
# and COPY data to Parquet format with maximum performance.
#

set -euo pipefail

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
PARQUET_DIR="${DATA_DIR}/parquet"
DB_NAME="${DB_NAME:-test}"
DB_USER="${DB_USER:-}"
DB_HOST="${DB_HOST:-localhost}"
LOG_DIR="${PROJECT_DIR}/logs"

# DuckDB path (use project bin or PATH)
if [[ -f "${PROJECT_DIR}/bin/duckdb" ]]; then
    DUCKDB_CMD="${PROJECT_DIR}/bin/duckdb"
else
    DUCKDB_CMD="duckdb"
fi

# Performance settings
PARALLEL_JOBS=4
CHUNK_SIZE=1000000

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

validate_environment() {
    log_info "Validating environment..."

    if [[ ! -f "$DUCKDB_CMD" ]] && ! command -v duckdb &> /dev/null; then
        log_error "DuckDB not found. Install with: curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip && unzip -o /tmp/duckdb.zip -d /tmp/ && mv /tmp/duckdb ${PROJECT_DIR}/bin/"
        exit 1
    fi

    if ! command -v mariadb &> /dev/null && ! command -v mysql &> /dev/null; then
        log_error "MariaDB/MySQL client not found"
        exit 1
    fi

    if ! systemctl is-active --quiet mariadb 2>/dev/null && ! systemctl is-active --quiet mysql 2>/dev/null; then
        log_error "MariaDB/MySQL service is not running"
        exit 1
    fi

    mkdir -p "$PARQUET_DIR" "$LOG_DIR"
    log_success "Environment validated"
}

# Phase 1: Import SQL to MySQL with maximum performance
phase1_mysql_import() {
    log_section "Phase 1: Import SQL to MySQL"
    
    log_info "Creating database '${DB_NAME}'..."
    if [[ -n "${DB_USER}" ]]; then
        mariadb -u${DB_USER} -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
        mariadb -u${DB_USER} -e "CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4;"
        
        log_info "Applying optimized session settings..."
        mariadb -u${DB_USER} ${DB_NAME} << 'EOF'
SET GLOBAL innodb_flush_log_at_trx_commit = 0;
SET GLOBAL innodb_doublewrite = 0;
EOF
    else
        mariadb -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
        mariadb -e "CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4;"
    fi
    
    # Get list of SQL files
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
    
    log_info "Importing ${#tables[@]} tables..."
    local start_time=$(date +%s)
    
    for table in "${tables[@]}"; do
        local sql_file="${SQL_DIR}/${table}.sql"
        if [[ ! -f "$sql_file" ]]; then
            log_warning "Skipping ${table} - file not found"
            continue
        fi
        
        local file_size=$(stat -c%s "$sql_file" 2>/dev/null || stat -f%z "$sql_file")
        local size_mb=$((file_size / 1024 / 1024))
        
        log_info "Importing ${table} (${size_mb}MB)..."
        local table_start=$(date +%s)
        
        local mysql_cmd="mariadb"
        if [[ -n "${DB_USER}" ]]; then
            mysql_cmd="mariadb -u${DB_USER}"
        fi
        
        if $mysql_cmd ${DB_NAME} < "$sql_file" 2>&1; then
            local table_end=$(date +%s)
            local table_duration=$((table_end - table_start))
            log_success "Imported ${table} in ${table_duration}s"
        else
            log_error "Failed to import ${table}"
        fi
    done
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    log_success "Phase 1 completed in ${total_duration}s ($(echo "scale=1; ${total_duration}/60" | bc) minutes)"
}

# Phase 2: MySQL to Parquet using DuckDB
phase2_duckdb_export() {
    log_section "Phase 2: MySQL to Parquet via DuckDB"
    
    log_info "Installing DuckDB MySQL extension..."
    
    # Build connection string
    local conn_string="host=${DB_HOST} database=${DB_NAME}"
    if [[ -n "${DB_USER}" ]]; then
        conn_string="${conn_string} user=${DB_USER}"
    fi
    
    log_info "Testing DuckDB MySQL connection..."
    if ! echo "INSTALL mysql; LOAD mysql; ATTACH '${conn_string}' AS mysqldb (TYPE mysql); SELECT 1 as test;" | "$DUCKDB_CMD" -csv 2>&1 | head -20; then
        log_error "Failed to connect to MySQL via DuckDB"
        exit 1
    fi
    
    log_success "DuckDB MySQL connection successful"
    
    # Get table list
    local mysql_cmd="mariadb"
    if [[ -n "${DB_USER}" ]]; then
        mysql_cmd="mariadb -u${DB_USER}"
    fi
    local tables=$($mysql_cmd ${DB_NAME} -N -e "SHOW TABLES;" 2>/dev/null)
    
    log_info "Exporting tables to Parquet..."
    local start_time=$(date +%s)
    
    for table in $tables; do
        log_info "Exporting ${table}..."
        local table_start=$(date +%s)
        
        local output_file="${PARQUET_DIR}/${table}.parquet"
        
        # Use DuckDB to copy from MySQL to Parquet
        "$DUCKDB_CMD" -c "
            INSTALL mysql;
            LOAD mysql;
            ATTACH '${conn_string}' AS mysqldb (TYPE mysql);
            
            COPY (
                SELECT * FROM mysqldb.${table}
            ) TO '${output_file}' (
                FORMAT PARQUET,
                COMPRESSION 'snappy',
                ROW_GROUP_SIZE 100000
            );
        " 2>&1 | tee -a "${LOG_DIR}/duckdb_export_${table}.log"
        
        if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
            local table_end=$(date +%s)
            local table_duration=$((table_end - table_start))
            local file_size=$(stat -c%s "$output_file" 2>/dev/null || stat -f%z "$output_file")
            local size_mb=$((file_size / 1024 / 1024))
            log_success "Exported ${table} (${size_mb}MB) in ${table_duration}s"
        else
            log_error "Failed to export ${table}"
        fi
    done
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    log_success "Phase 2 completed in ${total_duration}s ($(echo "scale=1; ${total_duration}/60" | bc) minutes)"
}

# Alternative: Direct SQL to Parquet without MySQL (using DuckDB's CSV auto-detection)
phase2_direct_duckdb() {
    log_section "Phase 2 Alternative: Direct SQL to Parquet"
    log_warning "This requires converting SQL to CSV first"
    log_info "Consider using Phase 2 (MySQL -> Parquet) for better reliability"
}

# Show summary
show_summary() {
    log_section "Import Summary"
    
    log_info "Parquet files created:"
    ls -lh "${PARQUET_DIR}/"*.parquet 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
    
    log_info ""
    log_info "Next steps:"
    log_info "  1. Load into DuckDB warehouse:"
    log_info "     duckdb data/warehouse/2026-02/osu.duckdb"
    log_info ""
    log_info "  2. Create tables from Parquet:"
    log_info "     CREATE TABLE scores AS SELECT * FROM read_parquet('data/parquet/scores.parquet');"
}

# Main execution
main() {
    local start_time=$(date +%s)
    
    log_section "DuckDB Native SQL Import"
    log_info "Starting at $(date)"
    log_info "This will:"
    log_info "  1. Import SQL files to MySQL (optimized)"
    log_info "  2. Export MySQL tables to Parquet via DuckDB"
    
    validate_environment
    
    # Check if we should skip Phase 1
    if [[ "${SKIP_PHASE1:-}" != "true" ]]; then
        phase1_mysql_import
    else
        log_info "Skipping Phase 1 (SKIP_PHASE1=true)"
    fi
    
    # Phase 2: Always run DuckDB export
    phase2_duckdb_export
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    local minutes=$((total_duration / 60))
    local seconds=$((total_duration % 60))
    
    log_section "Import Complete"
    log_success "Total time: ${minutes}m ${seconds}s"
    
    show_summary
}

# Help
show_help() {
    cat << 'EOF'
DuckDB Native SQL Import

Imports MySQL SQL dumps and converts directly to Parquet using DuckDB's
MySQL extension. This provides the fastest path from SQL to Parquet.

Usage: ./import_duckdb_native.sh [OPTIONS]

Options:
    --skip-phase1       Skip MySQL import (if already loaded)
    --help, -h          Show this help message

Environment Variables:
    SKIP_PHASE1=true    Skip Phase 1 (MySQL import)

Process:
    Phase 1: Import SQL dumps to MariaDB with optimized settings
    Phase 2: Use DuckDB MySQL extension to COPY data to Parquet

Performance:
    Expected time for 24GB: 30-60 minutes total
    - Phase 1 (MySQL import): 20-40 minutes
    - Phase 2 (DuckDB export): 10-20 minutes

Requirements:
    - MariaDB/MySQL server running
    - DuckDB installed with MySQL extension
    - Sufficient disk space for Parquet files

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-phase1)
            export SKIP_PHASE1=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Run main
main
