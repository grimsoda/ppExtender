#!/bin/bash
#
# Parallel LOAD DATA INFILE Script for MySQL/MariaDB
# Phase 2 of the parallel import pipeline
#
# This script orchestrates parallel loading of CSV chunks using
# GNU parallel for maximum throughput.
#

set -euo pipefail

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
CSV_DIR="${PROJECT_DIR}/data/csv_chunks"
DB_NAME="osu_import"
DB_USER="root"
DB_HOST="localhost"
LOG_DIR="${PROJECT_DIR}/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default settings
PARALLEL_JOBS=4
DRY_RUN=false
SKIP_SCHEMA=false
KEEP_CSV=false
TABLE=""

# Logging
LOG_FILE=""

#######################################
# Logging Functions
#######################################

log_info() {
    local msg="[INFO] $1"
    echo -e "${BLUE}${msg}${NC}"
    if [[ -n "$LOG_FILE" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $msg" >> "$LOG_FILE"
    fi
}

log_success() {
    local msg="[SUCCESS] $1"
    echo -e "${GREEN}${msg}${NC}"
    if [[ -n "$LOG_FILE" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $msg" >> "$LOG_FILE"
    fi
}

log_warning() {
    local msg="[WARNING] $1"
    echo -e "${YELLOW}${msg}${NC}"
    if [[ -n "$LOG_FILE" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $msg" >> "$LOG_FILE"
    fi
}

log_error() {
    local msg="[ERROR] $1"
    echo -e "${RED}${msg}${NC}" >&2
    if [[ -n "$LOG_FILE" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') $msg" >> "$LOG_FILE"
    fi
}

log_section() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
    if [[ -n "$LOG_FILE" ]]; then
        echo "" >> "$LOG_FILE"
        echo "========================================" >> "$LOG_FILE"
        echo "$1" >> "$LOG_FILE"
        echo "========================================" >> "$LOG_FILE"
    fi
}

#######################################
# Setup Functions
#######################################

setup_logging() {
    mkdir -p "$LOG_DIR"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    LOG_FILE="${LOG_DIR}/import_${timestamp}.log"
    touch "$LOG_FILE"
    log_info "Logging to: $LOG_FILE"
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    if ! command -v mysql &> /dev/null; then
        log_error "MySQL/MariaDB client not found"
        exit 1
    fi
    
    if ! command -v parallel &> /dev/null; then
        log_error "GNU parallel not found. Install with: sudo apt-get install parallel"
        exit 1
    fi
    
    # Check if MariaDB/MySQL is running
    if ! systemctl is-active --quiet mariadb 2>/dev/null && \
       ! systemctl is-active --quiet mysql 2>/dev/null && \
       ! mysqladmin ping &>/dev/null; then
        log_error "MariaDB/MySQL service is not running"
        exit 1
    fi
    
    log_success "All dependencies found"
}

#######################################
# Database Functions
#######################################

setup_database() {
    log_section "Database Setup"
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would create database: $DB_NAME"
        return
    fi
    
    log_info "Creating database '$DB_NAME'..."
    
    # Drop and recreate database
    mysql -u${DB_USER} -e "DROP DATABASE IF EXISTS ${DB_NAME};" 2>/dev/null || true
    mysql -u${DB_USER} -e "CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    
    log_success "Database created"
}

optimize_session() {
    log_info "Applying optimized session settings..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would apply session optimizations"
        return
    fi
    
    mysql -u${DB_USER} ${DB_NAME} << 'EOF'
-- Disable keys, foreign keys, and unique checks for speed
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET AUTOCOMMIT = 0;
SET SQL_LOG_BIN = 0;

-- Optimize for bulk insert
SET SESSION bulk_insert_buffer_size = 536870912;
SET SESSION innodb_buffer_pool_size = 8589934592;
SET SESSION innodb_log_buffer_size = 67108864;
SET SESSION innodb_flush_log_at_trx_commit = 0;
SET SESSION innodb_flush_method = O_DIRECT;
EOF
    
    log_success "Session optimized"
}

load_schema() {
    local table=$1
    local sql_file="${PROJECT_DIR}/data/ingest/2026-02/sql/${table}.sql"
    
    log_info "Loading schema for $table..."
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would load schema from: $sql_file"
        return
    fi
    
    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        return 1
    fi
    
    # Extract and execute CREATE TABLE statement
    local create_table=$(grep -Pzo 'CREATE TABLE[^;]+;' "$sql_file" 2>/dev/null | tr -d '\0' | head -1)
    
    if [[ -z "$create_table" ]]; then
        log_error "Could not extract CREATE TABLE statement from $sql_file"
        return 1
    fi
    
    # Execute the CREATE TABLE
    echo "$create_table" | mysql -u${DB_USER} ${DB_NAME}
    
    log_success "Schema loaded for $table"
}

#######################################
# Chunk Loading Functions
#######################################

load_chunk() {
    local chunk_file=$1
    local table=$2
    
    local chunk_name=$(basename "$chunk_file")
    local start_time=$(date +%s)
    
    # Build LOAD DATA INFILE command
    local load_cmd="LOAD DATA LOCAL INFILE '${chunk_file}'
INTO TABLE ${table}
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\\\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;"
    
    # Execute load
    if ! echo "$load_cmd" | mysql -u${DB_USER} ${DB_NAME} 2>&1; then
        log_error "Failed to load chunk: $chunk_name"
        return 1
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    log_success "Loaded $chunk_name in ${duration}s"
    return 0
}

export -f load_chunk log_info log_success log_error

load_table_parallel() {
    local table=$1
    local table_csv_dir="${CSV_DIR}/${table}"
    
    log_section "Loading Table: $table"
    
    # Check if CSV chunks exist
    if [[ ! -d "$table_csv_dir" ]]; then
        log_error "CSV directory not found: $table_csv_dir"
        log_info "Run Phase 1 first: python scripts/sql_to_csv_parallel.py --table $table"
        return 1
    fi
    
    # Get list of chunk files
    local chunks=($(ls -1 "${table_csv_dir}/${table}_chunk_"*.csv 2>/dev/null | sort))
    
    if [[ ${#chunks[@]} -eq 0 ]]; then
        log_error "No CSV chunks found in $table_csv_dir"
        return 1
    fi
    
    log_info "Found ${#chunks[@]} chunk(s) for $table"
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would load ${#chunks[@]} chunks with $PARALLEL_JOBS parallel jobs"
        for chunk in "${chunks[@]}"; do
            log_info "[DRY RUN] Would load: $(basename "$chunk")"
        done
        return 0
    fi
    
    # Load schema first
    if [[ "$SKIP_SCHEMA" == false ]]; then
        load_schema "$table" || return 1
    fi
    
    # Load chunks in parallel using GNU parallel
    local start_time=$(date +%s)
    
    log_info "Starting parallel load with $PARALLEL_JOBS jobs..."
    
    # Create a temporary script for parallel execution
    local tmp_script=$(mktemp)
    cat > "$tmp_script" << 'SCRIPT'
#!/bin/bash
chunk_file="$1"
table="$2"
DB_USER="$3"
DB_NAME="$4"

chunk_name=$(basename "$chunk_file")
echo "[$(date '+%H:%M:%S')] Loading $chunk_name..."

load_cmd="LOAD DATA LOCAL INFILE '${chunk_file}'
INTO TABLE ${table}
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\\\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;"

if echo "$load_cmd" | mysql -u${DB_USER} ${DB_NAME} 2>&1; then
    echo "[$(date '+%H:%M:%S')] SUCCESS: $chunk_name"
else
    echo "[$(date '+%H:%M:%S')] FAILED: $chunk_name" >&2
    exit 1
fi
SCRIPT
    chmod +x "$tmp_script"
    
    # Run parallel load
    local failed_chunks=0
    if ! parallel --jobs "$PARALLEL_JOBS" --progress "$tmp_script" {} "$table" "$DB_USER" "$DB_NAME" ::: "${chunks[@]}"; then
        log_warning "Some chunks may have failed"
        failed_chunks=1
    fi
    
    rm -f "$tmp_script"
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Get row count
    local row_count=$(mysql -u${DB_USER} ${DB_NAME} -N -e "SELECT COUNT(*) FROM ${table};" 2>/dev/null || echo "0")
    
    log_success "Table $table loaded in ${duration}s"
    log_info "Total rows: $(echo "$row_count" | xargs printf "%'d\n")"
    
    if [[ $failed_chunks -eq 0 ]]; then
        log_success "All chunks loaded successfully"
    else
        log_warning "Some chunks failed - check logs"
    fi
    
    return $failed_chunks
}

#######################################
# Post-Processing Functions
#######################################

enable_keys() {
    log_section "Enabling Keys"
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "[DRY RUN] Would re-enable keys and constraints"
        return
    fi
    
    log_info "Re-enabling foreign keys and unique checks..."
    
    mysql -u${DB_USER} ${DB_NAME} << 'EOF'
SET FOREIGN_KEY_CHECKS = 1;
SET UNIQUE_CHECKS = 1;
COMMIT;
EOF
    
    log_success "Keys enabled"
}

verify_import() {
    log_section "Import Verification"
    
    log_info "Row counts by table:"
    
    mysql -u${DB_USER} ${DB_NAME} -e "
        SELECT 
            table_name,
            table_rows,
            ROUND(data_length / 1024 / 1024, 2) AS size_mb
        FROM information_schema.tables
        WHERE table_schema = '${DB_NAME}'
        ORDER BY table_rows DESC;
    " 2>/dev/null || log_warning "Could not retrieve table statistics"
}

cleanup_csv() {
    if [[ "$KEEP_CSV" == false && "$DRY_RUN" == false ]]; then
        log_info "Cleaning up CSV chunks..."
        rm -rf "${CSV_DIR}"
        log_success "CSV chunks removed"
    fi
}

#######################################
# Main Functions
#######################################

show_help() {
    cat << EOF
Parallel LOAD DATA INFILE for MySQL/MariaDB

Usage: $0 [OPTIONS]

Options:
    --table TABLE       Load specific table only
    --all               Load all tables
    --jobs N            Number of parallel jobs (default: 4)
    --csv-dir DIR       CSV chunks directory (default: data/csv_chunks)
    --dry-run           Show what would be done without loading
    --skip-schema       Skip schema creation (table must exist)
    --keep-csv          Keep CSV files after loading
    --verify            Verify import and show row counts
    --help              Show this help message

Examples:
    $0 --table scores --jobs 8
    $0 --all --jobs 4
    $0 --table osu_counts --dry-run
    $0 --all --verify

EOF
}

main() {
    log_section "Parallel LOAD DATA INFILE"
    
    check_dependencies
    
    # Setup database
    setup_database
    optimize_session
    
    # Load tables
    if [[ -n "$TABLE" ]]; then
        load_table_parallel "$TABLE"
    else
        # Load all tables
        for table_dir in "$CSV_DIR"/*/; do
            if [[ -d "$table_dir" ]]; then
                local table=$(basename "$table_dir")
                load_table_parallel "$table" || true
            fi
        done
    fi
    
    # Post-processing
    enable_keys
    verify_import
    cleanup_csv
    
    log_section "Import Complete"
    log_success "All done! Database '$DB_NAME' is ready."
}

#######################################
# Parse Arguments
#######################################

while [[ $# -gt 0 ]]; do
    case $1 in
        --table)
            TABLE="$2"
            shift 2
            ;;
        --all)
            TABLE=""
            shift
            ;;
        --jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --csv-dir)
            CSV_DIR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-schema)
            SKIP_SCHEMA=true
            shift
            ;;
        --keep-csv)
            KEEP_CSV=true
            shift
            ;;
        --verify)
            # Just run verification
            setup_logging
            verify_import
            exit 0
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

# Setup logging
setup_logging

# Run main
main
