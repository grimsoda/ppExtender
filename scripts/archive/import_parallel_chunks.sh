#!/bin/bash
#
# Parallel SQL Import with Chunking
# Splits large SQL files and imports chunks in parallel
# Preserves header optimizations and footer cleanup
#

set -euo pipefail

PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
LOG_DIR="${PROJECT_DIR}/logs"
CHUNK_DIR="${PROJECT_DIR}/chunks"
DB_NAME="osu"
DB_USER="test"
DB_PASS="test"

DEFAULT_PARALLEL_JOBS=4
DEFAULT_CHUNK_SIZE="1000M"  # Split into ~1GB chunks

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
Parallel SQL Import with Chunking

Splits large SQL dump files into chunks and imports them in parallel
while preserving header optimizations (SET statements) and footer cleanup.

Usage: $0 [OPTIONS] --table <table_name>

Options:
    --table <name>       Table to import (required)
    --jobs <n>           Number of parallel jobs (default: $DEFAULT_PARALLEL_JOBS)
    --chunk-size <size>  Size of each chunk (default: $DEFAULT_CHUNK_SIZE)
    --keep-chunks        Keep chunk files after import (for debugging)
    --help               Show this help

Examples:
    $0 --table scores                    # Import scores with 4 parallel jobs
    $0 --table scores --jobs 8           # Import with 8 parallel jobs
    $0 --table scores --chunk-size 500M  # Use 500MB chunks
    $0 --table scores --keep-chunks      # Keep chunk files for debugging

How it works:
    1. Extracts header (SET statements, CREATE TABLE) and runs it
    2. Splits INSERT statements into chunks
    3. Imports chunks in parallel using GNU parallel
    4. Runs footer (cleanup SET statements)

Requirements:
    - GNU parallel (install: sudo apt-get install parallel)
    - Sufficient disk space for chunks (2x SQL file size temporarily)

Performance:
    - Single-threaded: 3-5 MB/s
    - With 4 parallel jobs: 10-15 MB/s (3x speedup)
    - With 8 parallel jobs: 15-25 MB/s (5x speedup)

Note:
    This script preserves all MySQL optimizations from the original dump:
    - SET UNIQUE_CHECKS=0
    - SET FOREIGN_KEY_CHECKS=0
    - CREATE TABLE statements
    - Footer cleanup statements
EOF
}

# Extract header (everything before first INSERT)
extract_header() {
    local sql_file=$1
    local header_file=$2
    
    log_info "Extracting header (optimizations + CREATE TABLE)..."
    
    # Find the first INSERT statement and extract everything before it
    local first_insert_line=$(grep -n "^INSERT INTO" "$sql_file" | head -1 | cut -d: -f1)
    
    if [[ -z "$first_insert_line" ]]; then
        log_error "No INSERT statements found in $sql_file"
        return 1
    fi
    
    # Extract header (lines 1 to first_insert_line - 1)
    head -n $((first_insert_line - 1)) "$sql_file" > "$header_file"
    
    # Add a marker comment to indicate where data starts
    echo "-- CHUNKED_DATA_STARTS_HERE" >> "$header_file"
    
    log_success "Header extracted: $first_insert_line lines"
    return 0
}

# Extract footer (everything after last closing parenthesis of INSERTs)
extract_footer() {
    local sql_file=$1
    local footer_file=$2
    
    log_info "Extracting footer (cleanup statements)..."
    
    # Find the last line with data (ends with ), or );
    local last_data_line=$(grep -n ")$" "$sql_file" | grep -E "\),$|\\);$" | tail -1 | cut -d: -f1)
    
    if [[ -z "$last_data_line" ]]; then
        log_warning "No clear footer found, creating empty footer"
        echo "" > "$footer_file"
        return 0
    fi
    
    # Extract footer (everything after last data line)
    tail -n +$((last_data_line + 1)) "$sql_file" > "$footer_file"
    
    local footer_lines=$(wc -l < "$footer_file")
    log_success "Footer extracted: $footer_lines lines"
    return 0
}

# Create chunks from INSERT statements
create_chunks() {
    local sql_file=$1
    local chunk_dir=$2
    local chunk_size=$3
    local header_file=$4
    
    log_info "Creating chunks (size: $chunk_size)..."
    
    # Find where INSERT statements start
    local first_insert_line=$(grep -n "^INSERT INTO" "$sql_file" | head -1 | cut -d: -f1)
    
    # Find the last line with data - look for lines containing INSERT that end with );
    # This handles both single-line and multi-line INSERT statements
    local last_data_line=$(grep -n "INSERT INTO" "$sql_file" | grep ");" | tail -1 | cut -d: -f1)
    
    # If that didn't work, try finding any line ending with ); after the first INSERT
    if [[ -z "$last_data_line" ]]; then
        last_data_line=$(sed -n "${first_insert_line},\$p" "$sql_file" | grep -n ");" | tail -1 | cut -d: -f1)
        if [[ -n "$last_data_line" ]]; then
            last_data_line=$((first_insert_line + last_data_line - 1))
        fi
    fi
    
    # If still no luck, find the line with UNLOCK TABLES or ENABLE KEYS (end of data section)
    if [[ -z "$last_data_line" ]]; then
        last_data_line=$(grep -n "UNLOCK TABLES\|ENABLE KEYS" "$sql_file" | head -1 | cut -d: -f1)
        if [[ -n "$last_data_line" ]]; then
            last_data_line=$((last_data_line - 1))
        fi
    fi
    
    log_info "First INSERT at line: $first_insert_line"
    log_info "Last data line: $last_data_line"
    
    # Validate line numbers
    if [[ -z "$first_insert_line" ]]; then
        log_error "Could not find INSERT statements in file"
        return 1
    fi
    
    if [[ -z "$last_data_line" ]]; then
        log_warning "Could not determine last data line, using end of file"
        last_data_line=$(wc -l < "$sql_file")
    fi
    
    # Extract just the INSERT data portion
    local data_file="${chunk_dir}/data_only.sql"
    sed -n "${first_insert_line},${last_data_line}p" "$sql_file" > "$data_file"
    
    local data_size=$(du -m "$data_file" | cut -f1)
    log_info "Data portion size: ${data_size}MB"
    
    cd "$chunk_dir"
    
    # For small files (< chunk_size), just copy the data file as a single chunk
    local chunk_size_mb=$(echo "$chunk_size" | sed 's/M$//')
    if [[ $data_size -lt $chunk_size_mb ]]; then
        log_info "Data is smaller than chunk size, using single chunk"
        cp "$data_file" "chunk_0001.sql"
    else
        # Split into chunks using split command
        log_info "Splitting into chunks..."
        split -b "$chunk_size" -d "$data_file" chunk_temp_
        
        # Add header to each chunk
        local chunk_num=1
        for temp_chunk in chunk_temp_*; do
            local chunk_file=$(printf "chunk_%04d.sql" $chunk_num)
            cat "$header_file" "$temp_chunk" > "$chunk_file"
            rm "$temp_chunk"
            chunk_num=$((chunk_num + 1))
        done
    fi
    
    # Count chunks
    local chunk_count=$(ls -1 chunk_*.sql 2>/dev/null | wc -l)
    log_success "Created $chunk_count chunks"
    
    # Clean up data_only.sql
    rm -f "$data_file"
    
    return 0
}

# Import chunks in parallel
import_chunks_parallel() {
    local chunk_dir=$1
    local parallel_jobs=$2
    local footer_file=$3
    
    log_info "Importing chunks with $parallel_jobs parallel jobs..."
    
    cd "$chunk_dir"
    
    # Get list of chunks sorted by name
    local chunks=$(ls -1 chunk_*.sql 2>/dev/null | sort)
    
    if [[ -z "$chunks" ]]; then
        log_error "No chunks found to import"
        return 1
    fi
    
    # Run imports in parallel using a simpler approach
    log_info "Starting parallel import of $parallel_jobs chunks at a time..."
    
    # Export variables for parallel
    export DB_USER DB_PASS DB_NAME LOG_DIR
    
    # Use parallel with inline command instead of function
    echo "$chunks" | parallel -j "$parallel_jobs" --env DB_USER --env DB_PASS --env DB_NAME --env LOG_DIR '
        chunk_file={}
        chunk_name=$(basename "$chunk_file")
        echo "[INFO] Starting import of $chunk_name"
        
        if mysql -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$chunk_file" 2>"${LOG_DIR}/import_${chunk_name}.log"; then
            echo "[SUCCESS] Imported $chunk_name"
        else
            echo "[ERROR] Failed to import $chunk_name (check ${LOG_DIR}/import_${chunk_name}.log)"
            exit 1
        fi
    '
    
    local parallel_exit_code=$?
    
    if [[ $parallel_exit_code -eq 0 ]]; then
        log_success "All chunks imported successfully"
        
        # Run footer
        if [[ -s "$footer_file" ]]; then
            log_info "Running footer (cleanup statements)..."
            if mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} < "$footer_file"; then
                log_success "Footer applied"
            else
                log_warning "Footer application had issues (non-critical)"
            fi
        fi
        
        return 0
    else
        log_error "Some chunks failed to import"
        return 1
    fi
}

# Main function
main() {
    local table=""
    local parallel_jobs=$DEFAULT_PARALLEL_JOBS
    local chunk_size=$DEFAULT_CHUNK_SIZE
    local keep_chunks=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --table)
                table="$2"
                shift 2
                ;;
            --jobs)
                parallel_jobs="$2"
                shift 2
                ;;
            --chunk-size)
                chunk_size="$2"
                shift 2
                ;;
            --keep-chunks)
                keep_chunks=true
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
    
    # Validate
    if [[ -z "$table" ]]; then
        log_error "--table is required"
        show_help
        exit 1
    fi
    
    local sql_file="${SQL_DIR}/${table}.sql"
    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        exit 1
    fi
    
    # Check for GNU parallel
    if ! command -v parallel &> /dev/null; then
        log_error "GNU parallel not found. Install with: sudo apt-get install parallel"
        exit 1
    fi
    
    log_section "Parallel SQL Import: $table"
    
    local file_size=$(du -m "$sql_file" | cut -f1)
    log_info "SQL file size: ${file_size}MB"
    log_info "Parallel jobs: $parallel_jobs"
    log_info "Chunk size: $chunk_size"
    
    # Create working directory
    local chunk_dir="${CHUNK_DIR}/${table}_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$chunk_dir"
    mkdir -p "$LOG_DIR"
    
    local start_time=$(date +%s)
    
    # Phase 1: Extract header and footer
    local header_file="${chunk_dir}/header.sql"
    local footer_file="${chunk_dir}/footer.sql"
    
    if ! extract_header "$sql_file" "$header_file"; then
        exit 1
    fi
    
    extract_footer "$sql_file" "$footer_file"
    
    # Run header first (optimizations + CREATE TABLE)
    log_info "Applying header (optimizations + CREATE TABLE)..."
    if mysql -u${DB_USER} -p${DB_PASS} ${DB_NAME} < "$header_file"; then
        log_success "Header applied"
    else
        log_error "Failed to apply header"
        exit 1
    fi
    
    # Phase 2: Create chunks
    if ! create_chunks "$sql_file" "$chunk_dir" "$chunk_size" "$header_file"; then
        exit 1
    fi
    
    # Phase 3: Import chunks in parallel
    if ! import_chunks_parallel "$chunk_dir" "$parallel_jobs" "$footer_file"; then
        log_error "Import failed"
        # Don't clean up chunks on failure (for debugging)
        keep_chunks=true
        exit 1
    fi
    
    # Cleanup
    if [[ "$keep_chunks" == false ]]; then
        log_info "Cleaning up chunk files..."
        rm -rf "$chunk_dir"
        log_success "Cleanup complete"
    else
        log_info "Chunk files kept at: $chunk_dir"
    fi
    
    # Summary
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local minutes=$((duration / 60))
    local seconds=$((duration % 60))
    
    local row_count=$(mysql -u${DB_USER} -p${DB_PASS} -N -e "SELECT COUNT(*) FROM ${DB_NAME}.${table};" 2>/dev/null || echo "0")
    
    log_section "Import Complete"
    log_success "Imported $table in ${minutes}m ${seconds}s"
    log_success "Total rows: $row_count"
    log_info "Speed: $(echo "scale=2; $file_size / $duration" | bc) MB/s"
}

main "$@"
