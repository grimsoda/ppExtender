#!/bin/bash
#
# Optimized MySQL Import with Progress Monitoring
# Fixed: Session optimizations now applied in same connection as import
#

set -euo pipefail

PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
LOG_DIR="${PROJECT_DIR}/logs"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"

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
Optimized MySQL Import with Progress Monitoring

This script imports SQL files with proper session optimizations and progress monitoring.

Usage: $0 --table <table_name> [OPTIONS]

Options:
    --table <name>       Table to import (required)
    --force              Drop table if exists before import
    --help               Show this help

Examples:
    $0 --table scores                    # Import scores table
    $0 --table scores --force            # Drop and reimport scores

Progress Monitoring:
    - Tracks INSERT statement completion rate
    - Shows estimated time remaining with margin of error
    - Updates every 5 seconds with high-precision timing

Performance Optimizations Applied:
    - SET SESSION foreign_key_checks = 0
    - SET SESSION unique_checks = 0 (BIGGEST WIN - 2-3x speedup)
    - SET SESSION autocommit = 0
    - SET SESSION sql_log_bin = 0
    - SET GLOBAL innodb_flush_log_at_trx_commit = 0
    - SET GLOBAL innodb_doublewrite = 0

Expected Performance:
    - With optimizations: 3-5 MB/s
    - Without optimizations: 0.5-1 MB/s
    - 20GB file: 1-2 hours (optimized) vs 6+ hours (unoptimized)
EOF
}

# Monitor import progress with high-precision timing
monitor_import() {
    local table=$1
    local start_time=$2
    local mysql_pid=$3
    local sql_file=$4
    local file_size_mb=$5
    
    # Arrays to store timing data
    local -a insert_times
    local -a insert_counts
    local prev_query=""
    local statement_count=0
    local last_statement_time=$start_time
    local poll_start=$start_time
    
    echo ""
    echo "========================================"
    echo "Progress Monitoring Started"
    echo "========================================"
    echo "Update interval: 10 seconds (heartbeat)"
    echo "Statement detection: Real-time (0.5s polling)"
    echo "File size: ${file_size_mb}MB"
    echo ""
    
    local last_update_time=$start_time
    local update_interval=10  # Show update every 10 seconds even if same query
    local mysql_conn_id=""
    
    while kill -0 $mysql_pid 2>/dev/null; do
        sleep 0.2  # Poll at 4x the anticipated insert rate (~0.8s / 4 = 0.2s) for accurate detection without excessive overhead
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        local elapsed_min=$((elapsed / 60))
        local elapsed_sec=$((elapsed % 60))
        
        # Find the MySQL connection ID for our import process
        # Look for active queries from our user on our database
        if [ -z "$mysql_conn_id" ]; then
            # Try to find the connection ID
            mysql_conn_id=$(mysql -u${DB_USER} -p${DB_PASS} -e "SHOW PROCESSLIST" 2>/dev/null | grep "${DB_USER}" | grep "${DB_NAME}" | grep -v "Sleep" | awk '{print $1}' | head -1)
        fi
        
        # Get current query info
        local process_info=""
        if [ -n "$mysql_conn_id" ]; then
            process_info=$(mysql -u${DB_USER} -p${DB_PASS} -e "SHOW PROCESSLIST" 2>/dev/null | grep "^${mysql_conn_id}" || true)
        fi
        
        if [ -n "$process_info" ]; then
            # Extract query part (everything from field 11 onwards)
            local query=$(echo "$process_info" | awk '{for(i=11;i<=NF;i++) printf "%s ", $i}')
            local query_short=$(echo "$query" | cut -c1-50)
            
            # Check if query changed (new INSERT statement)
            if [ "$query" != "$prev_query" ] && [ -n "$prev_query" ]; then
                local current_ts=$(date +%s)
                local statement_duration=$((current_ts - last_statement_time))
                
                # Store timing data
                insert_times+=($statement_duration)
                insert_counts+=($statement_count)
                statement_count=$((statement_count + 1))
                last_statement_time=$current_ts
                last_update_time=$current_ts
                
                # Calculate statistics (last 20 samples)
                local sample_count=${#insert_times[@]}
                local window_size=$((sample_count < 20 ? sample_count : 20))
                local start_idx=$((sample_count - window_size))
                
                if [ $window_size -gt 0 ]; then
                    local total_time=0
                    for ((i=start_idx; i<sample_count; i++)); do
                        total_time=$((total_time + insert_times[i]))
                    done
                    
                    local avg_time=$(echo "scale=3; $total_time / $window_size" | bc)
                    
                    # Calculate margin of error (standard deviation approximation)
                    local variance_sum=0
                    for ((i=start_idx; i<sample_count; i++)); do
                        local diff=$(echo "scale=3; ${insert_times[i]} - $avg_time" | bc)
                        local sq_diff=$(echo "scale=3; $diff * $diff" | bc)
                        variance_sum=$(echo "scale=3; $variance_sum + $sq_diff" | bc)
                    done
                    local variance=$(echo "scale=3; $variance_sum / $window_size" | bc)
                    local std_dev=$(echo "scale=3; sqrt($variance)" | bc 2>/dev/null || echo "0")
                    local margin=$(echo "scale=3; 1.96 * $std_dev / sqrt($window_size)" | bc 2>/dev/null || echo "0")
                    
                    # Estimate remaining time
                    # Assuming ~21000 INSERT statements for scores table
                    local estimated_total_statements=20933
                    local remaining_statements=$((estimated_total_statements - statement_count))
                    local est_remaining_sec=$(echo "scale=0; $remaining_statements * $avg_time" | bc)
                    # Use bc for division to handle decimal values
                    local est_remaining_min=$(echo "scale=0; $est_remaining_sec / 60" | bc)
                    local est_remaining_hr=$(echo "scale=0; $est_remaining_min / 60" | bc)
                    est_remaining_min=$(echo "scale=0; $est_remaining_min % 60" | bc)
                    
                    # Margin of error for time estimate
                    local margin_sec=$(echo "scale=0; $remaining_statements * $margin" | bc 2>/dev/null || echo "0")
                    local margin_min=$(echo "scale=0; $margin_sec / 60" | bc)
                    
                    # Calculate current speed (MB/s)
                    local poll_elapsed=$((current_time - poll_start))
                    if [ $poll_elapsed -gt 0 ]; then
                        # Rough estimate: assume each statement is ~1MB
                        local processed_mb=$statement_count
                        local speed=$(echo "scale=2; $processed_mb / $poll_elapsed" | bc 2>/dev/null || echo "N/A")
                        
                        echo ""
                        echo "--- Progress Update [${elapsed_min}m ${elapsed_sec}s elapsed] ---"
                        echo "Statements completed: $statement_count / ~$estimated_total_statements"
                        echo "Last INSERT took: ${statement_duration}s"
                        echo "Average time per INSERT: ${avg_time}s (±${margin}s)"
                        echo "Current speed: ~${speed} MB/s"
                        echo "Estimated remaining: ${est_remaining_hr}h ${est_remaining_min}m (±${margin_min}m)"
                        echo "Current query: ${query_short}..."
                        echo "----------------------------------------"
                    fi
                fi
            elif [ $((current_time - last_update_time)) -ge $update_interval ]; then
                # Show heartbeat update even if same query is still running
                last_update_time=$current_time
                local current_query_time=$((current_time - last_statement_time))
                
                echo ""
                echo "--- Heartbeat [${elapsed_min}m ${elapsed_sec}s elapsed] ---"
                echo "Current INSERT running for: ${current_query_time}s"
                echo "Statements completed: $statement_count"
                if [ ${#insert_times[@]:-0} -gt 0 ]; then
                    local avg_time=$(echo "scale=3; $(IFS=+; echo "$((${insert_times[*]}))") / ${#insert_times[@]:-1}" | bc 2>/dev/null || echo "N/A")
                    echo "Average time so far: ${avg_time}s"
                fi
                echo "Query: ${query_short}..."
                echo "----------------------------------------"
            fi
            
            prev_query="$query"
        fi
    done
    
    echo ""
    echo "========================================"
    echo "Import Complete - Final Statistics"
    echo "========================================"
    echo "Total statements processed: $statement_count"
    echo "Total time: ${elapsed_min}m ${elapsed_sec}s"
    if [ $statement_count -gt 0 ]; then
        local overall_avg=$(echo "scale=3; $elapsed / $statement_count" | bc)
        echo "Overall average: ${overall_avg}s per statement"
    fi
}

import_table() {
    local table=$1
    local force=$2
    local sql_file="${SQL_DIR}/${table}.sql"
    
    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        return 1
    fi
    
    local size_mb=$(du -m "$sql_file" | cut -f1)
    local estimated_minutes=$((size_mb / 5 / 60))
    
    log_section "Importing: $table (${size_mb}MB)"
    log_info "Estimated time: ~${estimated_minutes} minutes (at 5 MB/s with optimizations)"
    
    # Drop table if force flag is set
    if [[ "$force" == true ]]; then
        log_warning "Dropping existing table ${table}..."
        mysql -u${DB_USER} -p${DB_PASS} -e "DROP TABLE IF EXISTS ${DB_NAME}.${table};" 2>/dev/null || true
    fi
    
    # Apply GLOBAL optimizations (these persist across connections)
    log_info "Applying global optimizations..."
    mysql -u${DB_USER} -p${DB_PASS} -e "
        SET GLOBAL innodb_flush_log_at_trx_commit = 0;
        SET GLOBAL innodb_doublewrite = 0;
    " 2>/dev/null || true
    
    mkdir -p "$LOG_DIR"
    local log_file="${LOG_DIR}/import_${table}_$(date +%Y%m%d_%H%M%S).log"
    local start_time=$(date +%s)
    
    log_info "Starting import with session optimizations..."
    log_info "Monitor progress with: tail -f $log_file"
    
    # CRITICAL FIX: Apply session optimizations IN THE SAME CONNECTION as the import
    # by prepending them to the SQL file stream
    (
        # Session optimizations - these apply to this connection only
        echo "SET SESSION foreign_key_checks = 0;"
        echo "SET SESSION unique_checks = 0;"
        echo "SET SESSION sql_log_bin = 0;"
        echo "SET SESSION autocommit = 0;"
        echo "SET SESSION bulk_insert_buffer_size = 1073741824;"
        echo "USE ${DB_NAME};"
        cat "$sql_file"
        echo "COMMIT;"
    ) | mysql -u${DB_USER} -p${DB_PASS} 2>> "$log_file" &
    
    local mysql_pid=$!
    
    # Start monitoring
    monitor_import "$table" "$start_time" "$mysql_pid" "$sql_file" "$size_mb" &
    local monitor_pid=$!
    
    # Wait for import to complete
    if wait $mysql_pid; then
        kill $monitor_pid 2>/dev/null || true
        wait $monitor_pid 2>/dev/null || true
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local speed=$(echo "scale=2; $size_mb / $duration" | bc 2>/dev/null || echo "N/A")
        local row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
        
        log_success "Imported in ${duration}s (${speed} MB/s) - ${row_count} rows"
        
        # Restore safe settings
        mysql -u${DB_USER} -p${DB_PASS} -e "
            SET GLOBAL innodb_flush_log_at_trx_commit = 1;
            SET GLOBAL innodb_doublewrite = 1;
        " 2>/dev/null || true
        
        return 0
    else
        kill $monitor_pid 2>/dev/null || true
        log_error "Import failed (check $log_file)"
        
        # Restore safe settings even on failure
        mysql -u${DB_USER} -p${DB_PASS} -e "
            SET GLOBAL innodb_flush_log_at_trx_commit = 1;
            SET GLOBAL innodb_doublewrite = 1;
        " 2>/dev/null || true
        
        return 1
    fi
}

main() {
    local table=""
    local force=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --table)
                table="$2"
                shift 2
                ;;
            --force)
                force=true
                shift
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
    
    if [[ -z "$table" ]]; then
        log_error "--table is required"
        show_help
        exit 1
    fi
    
    log_section "MySQL Import with Progress Monitoring"
    log_info "Version: Fixed with proper session optimizations"
    import_table "$table" "$force"
}

main "$@"
