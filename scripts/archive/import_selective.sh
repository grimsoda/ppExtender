#!/bin/bash
#
# Flexible MySQL Import Script - Import specific tables only
# Usage: ./import_selective.sh [OPTIONS]
#
# Options:
#   --table <name>      Import only specific table (can be used multiple times)
#   --all               Import all tables
#   --list              List available tables
#   --help              Show this help
#
# Examples:
#   ./import_selective.sh --table scores                    # Import only scores
#   ./import_selective.sh --table osu_scores_high --table scores  # Import 2 tables
#   ./import_selective.sh --all                             # Import all tables
#

set -euo pipefail

PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
LOG_DIR="${PROJECT_DIR}/logs"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"

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

# All available tables with sizes
declare -A TABLE_SIZES=(
    ["osu_counts"]="0.003"
    ["osu_difficulty_attribs"]="0.001"
    ["osu_beatmap_performance_blacklist"]="0.001"
    ["sample_users"]="0.3"
    ["osu_user_stats"]="2"
    ["osu_beatmapsets"]="42"
    ["osu_beatmaps"]="52"
    ["osu_beatmap_failtimes"]="134"
    ["osu_beatmap_difficulty"]="470"
    ["osu_user_beatmap_playcount"]="1600"
    ["osu_beatmap_difficulty_attribs"]="3700"
    ["osu_scores_high"]="5800"
    ["scores"]="20300"
)

list_tables() {
    echo "Available tables:"
    echo ""
    printf "%-40s %10s\n" "Table Name" "Size (MB)"
    echo "---------------------------------------- ----------"
    for table in "${!TABLE_SIZES[@]}"; do
        size="${TABLE_SIZES[$table]}"
        if [[ -f "${SQL_DIR}/${table}.sql" ]]; then
            actual_size=$(du -m "${SQL_DIR}/${table}.sql" 2>/dev/null | cut -f1)
            printf "%-40s %10s\n" "$table" "${actual_size:-$size}"
        else
            printf "%-40s %10s %s\n" "$table" "${size}" "(not found)"
        fi
    done
    echo ""
}

show_help() {
    cat << EOF
Enhanced MySQL Import Script with Progress Monitoring

Usage: $0 [OPTIONS]

Options:
    --table <name>     Import specific table (can use multiple times)
    --all              Import all tables
    --list             List available tables
    --help             Show this help message

Examples:
    $0 --table scores                    # Import only scores
    $0 --table osu_scores_high --table scores  # Import 2 tables
    $0 --all                              # Import all tables

Progress Monitoring:
    The script will display MySQL process information every 60 seconds
    to show that the import is still active.

Multi-threaded Import Options:
    For faster imports, consider these alternatives:
    
    1. mydumper/myloader (Recommended)
       Install: sudo apt-get install mydumper
       Usage: mydumper -B osu -o /backup/dir -t 8
              myloader -d /backup/dir -t 8
    
    2. LOAD DATA INFILE (Fastest for CSV)
       Convert SQL to CSV first, then:
       mysql -e "LOAD DATA INFILE 'data.csv' INTO TABLE scores"
    
    3. Parallel SQL Import
       Split SQL file and import chunks in parallel

Current Performance:
    - Single-threaded: 3-5 MB/s
    - With 10GB buffer pool: 5-10 MB/s
    - Expected time for 20GB: 30-60 minutes

Environment:
    DB_NAME            Database name (default: osu)
    DB_USER            MySQL user (default: test)
    DB_PASS            MySQL password (default: test)
EOF
}

monitor_progress() {
    local table=$1
    local start_time=$2
    local monitor_pid=""
    
    (
        while true; do
            sleep 60
            local elapsed=$(($(date +%s) - start_time))
            local minutes=$((elapsed / 60))
            
            echo ""
            echo "--- Progress Update [$minutes min elapsed] ---"
            
            mysql -u${DB_USER} -p${DB_PASS} -e "
                SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) as QUERY
                FROM INFORMATION_SCHEMA.PROCESSLIST 
                WHERE DB = '${DB_NAME}' AND COMMAND != 'Sleep'
                ORDER BY TIME DESC 
                LIMIT 3;
            " 2>/dev/null || echo "Cannot query process list"
            
            local row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
            echo "Current rows in ${table}: ${row_count}"
            echo "----------------------------------------"
        done
    ) &
    monitor_pid=$!
    
    echo $monitor_pid
}

import_table() {
    local table=$1
    local sql_file="${SQL_DIR}/${table}.sql"
    
    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        return 1
    fi
    
    local size_mb=$(du -m "$sql_file" | cut -f1)
    local estimated_minutes=$((size_mb / 5 / 60))
    
    log_section "Importing: $table (${size_mb}MB)"
    log_info "Estimated time: ${estimated_minutes} minutes (at 5 MB/s)"
    log_info "Progress monitoring started (updates every 60 seconds)"
    
    if [[ $size_mb -gt 100 ]]; then
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
        " 2>/dev/null || true
    fi
    
    local start_time=$(date +%s)
    local log_file="${LOG_DIR}/import_${table}_$(date +%Y%m%d_%H%M%S).log"
    
    local monitor_pid=$(monitor_progress "$table" "$start_time")
    
    local import_success=false
    if timeout 14400 mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} < "$sql_file" 2> "$log_file"; then
        import_success=true
    fi
    
    kill $monitor_pid 2>/dev/null || true
    wait $monitor_pid 2>/dev/null || true
    
    if [[ "$import_success" == true ]]; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local speed=$(echo "scale=2; $size_mb / $duration" | bc 2>/dev/null || echo "N/A")
        
        local row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
        
        log_success "Imported in ${duration}s (${speed} MB/s) - ${row_count} rows"
        return 0
    else
        log_error "Failed to import $table (check $log_file)"
        return 1
    fi
}

main() {
    local tables_to_import=()
    local import_all=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --table)
                if [[ -n "${2:-}" ]]; then
                    tables_to_import+=("$2")
                    shift 2
                else
                    log_error "--table requires a table name"
                    exit 1
                fi
                ;;
            --all)
                import_all=true
                shift
                ;;
            --list)
                list_tables
                exit 0
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
    if [[ ${#tables_to_import[@]} -eq 0 && "$import_all" == false ]]; then
        log_error "No tables specified. Use --table, --all, or --list"
        show_help
        exit 1
    fi
    
    # If --all, populate tables_to_import with all available tables
    if [[ "$import_all" == true ]]; then
        tables_to_import=()
        for table in "${!TABLE_SIZES[@]}"; do
            if [[ -f "${SQL_DIR}/${table}.sql" ]]; then
                tables_to_import+=("$table")
            fi
        done
    fi
    
    log_section "Selective Import with Progress Monitoring"
    log_info "Tables to import: ${#tables_to_import[@]}"
    for table in "${tables_to_import[@]}"; do
        echo "  - $table"
    done
    echo ""
    
    mkdir -p "$LOG_DIR"
    
    # Import each table
    local success_count=0
    local fail_count=0
    
    for table in "${tables_to_import[@]}"; do
        if import_table "$table"; then
            ((success_count++)) || true
        else
            ((fail_count++)) || true
        fi
    done
    
    log_section "Import Summary"
    log_success "Completed: $success_count tables"
    if [[ $fail_count -gt 0 ]]; then
        log_error "Failed: $fail_count tables"
    fi
}

main "$@"
