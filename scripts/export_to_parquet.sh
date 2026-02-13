#!/bin/bash
#
# MySQL to Parquet Export Script
# Export MySQL tables to Parquet format using DuckDB
#
# Usage: ./export_to_parquet.sh [OPTIONS]
#
# Options:
#   --table <name>     Export specific table (can be used multiple times)
#   --all              Export all tables
#   --list             List tables in database
#   --output <dir>    Output directory (default: data/parquet)
#   --help             Show this help
#
# Examples:
#   ./export_to_parquet.sh --table scores                    # Export only scores
#   ./export_to_parquet.sh --table osu_scores_high --table scores  # Export 2 tables
#   ./export_to_parquet.sh --all                             # Export all tables
#

set -euo pipefail

PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
PARQUET_DIR="${DATA_DIR}/parquet"
LOG_DIR="${PROJECT_DIR}/logs"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"
DB_HOST="localhost"

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

show_help() {
    cat << EOF
MySQL to Parquet Export Script

Exports MySQL tables to Parquet format using DuckDB.

Usage: $0 [OPTIONS]

Options:
    --table <name>     Export specific table (can use multiple times)
    --all              Export all tables from database
    --list             List tables in database
    --output <dir>    Output directory (default: data/parquet)
    --help             Show this help message

Examples:
    $0 --table scores                    # Export only scores
    $0 --table osu_scores_high --table scores  # Export 2 tables
    $0 --all                              # Export all tables
    $0 --all --output ./my_parquet        # Export to custom directory

Environment:
    DB_NAME            Database name (default: osu)
    DB_USER            MySQL user (default: test)
    DB_PASS            MySQL password (default: test)
    DB_HOST            MySQL host (default: localhost)
EOF
}

list_tables() {
    echo "Tables in database '${DB_NAME}':"
    echo ""
    mysql -u${DB_USER} -p${DB_PASS} -e "SHOW TABLES FROM ${DB_NAME};" 2>/dev/null || {
        log_error "Cannot connect to database"
        exit 1
    }
    echo ""
}

export_table() {
    local table=$1
    local output_file="${PARQUET_DIR}/${table}.parquet"
    local start_time=$(date +%s)
    
    log_info "Exporting $table..."
    
    if python3 -c "
import duckdb
import sys
import os
table_name = '${table}'
output_dir = '${PARQUET_DIR}/${table}'
try:
    # Create output directory for this table
    os.makedirs(output_dir, exist_ok=True)
    
    con = duckdb.connect()
    
    # Performance optimizations
    con.execute('SET preserve_insertion_order = false')
    con.execute('SET mysql_experimental_filter_pushdown = true')
    
    con.execute('INSTALL mysql')
    con.execute('LOAD mysql')
    con.execute(\"\"\"
        ATTACH 'host=${DB_HOST} user=${DB_USER} password=${DB_PASS} database=${DB_NAME}' 
        AS mysqldb (TYPE mysql)
    \"\"\")
    
    # Export with PER_THREAD_OUTPUT for maximum parallelism
    con.execute(f\"\"\"
        COPY (SELECT * FROM mysqldb.{table_name})
        TO '{output_dir}/data.parquet' (
            FORMAT PARQUET,
            COMPRESSION 'zstd',
            COMPRESSION_LEVEL 3,
            ROW_GROUP_SIZE 1000000,
            ROW_GROUP_SIZE_BYTES '16MB',
            PARQUET_VERSION 'V2',
            PER_THREAD_OUTPUT true
        )
    \"\"\")
    con.close()
    sys.exit(0)
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
    " 2> "${LOG_DIR}/export_${table}.log"; then
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        # Count files and total size in directory
        local file_count=$(ls -1 "${PARQUET_DIR}/${table}"/*.parquet 2>/dev/null | wc -l)
        local total_size=$(du -sb "${PARQUET_DIR}/${table}" 2>/dev/null | cut -f1)
        local size_mb=$((total_size / 1024 / 1024))
        
        log_success "Exported ${table} (${file_count} files, ${size_mb}MB total) in ${duration}s"
        return 0
    else
        log_error "Failed to export $table"
        return 1
    fi
}

main() {
    local tables_to_export=()
    local export_all=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --table)
                if [[ -n "${2:-}" ]]; then
                    tables_to_export+=("$2")
                    shift 2
                else
                    log_error "--table requires a table name"
                    exit 1
                fi
                ;;
            --all)
                export_all=true
                shift
                ;;
            --list)
                list_tables
                exit 0
                ;;
            --output)
                if [[ -n "${2:-}" ]]; then
                    PARQUET_DIR="$2"
                    shift 2
                else
                    log_error "--output requires a directory path"
                    exit 1
                fi
                ;;
            --help)
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
    
    # Validate
    if [[ ${#tables_to_export[@]} -eq 0 && "$export_all" == false ]]; then
        log_error "No tables specified. Use --table, --all, or --list"
        show_help
        exit 1
    fi
    
    # If --all, get all tables from database
    if [[ "$export_all" == true ]]; then
        tables_to_export=()
        while IFS= read -r table; do
            [[ -n "$table" ]] && tables_to_export+=("$table")
        done < <(mysql -u${DB_USER} -p${DB_PASS} -N -e "SHOW TABLES FROM ${DB_NAME};" 2>/dev/null)
    fi
    
    log_section "MySQL to Parquet Export"
    log_info "Output directory: ${PARQUET_DIR}"
    log_info "Tables to export: ${#tables_to_export[@]}"
    for table in "${tables_to_export[@]}"; do
        echo "  - $table"
    done
    echo ""

    # Confirmation prompt
    echo ""
    read -p "Do you want to proceed with the export? (y/N) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Export cancelled by user"
        exit 0
    fi
    
    # Create output directory
    mkdir -p "$PARQUET_DIR"
    mkdir -p "$LOG_DIR"
    
    # Check DuckDB
    if ! python3 -c "import duckdb" 2>/dev/null; then
        log_error "DuckDB Python module not found. Install with: pip install duckdb"
        exit 1
    fi
    
    # Export each table
    local success_count=0
    local fail_count=0
    local total_start=$(date +%s)
    
    for table in "${tables_to_export[@]}"; do
        if export_table "$table"; then
            ((success_count++)) || true
        else
            ((fail_count++)) || true
        fi
    done
    
    local total_end=$(date +%s)
    local total_duration=$((total_end - total_start))
    
    log_section "Export Summary"
    log_success "Exported: $success_count tables in ${total_duration}s"
    if [[ $fail_count -gt 0 ]]; then
        log_error "Failed: $fail_count tables"
    fi
    log_info "Parquet files: ${PARQUET_DIR}/"
    log_info "Logs: ${LOG_DIR}/"
}

main "$@"
