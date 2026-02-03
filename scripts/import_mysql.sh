#!/bin/bash
#
# Fast MySQL/MariaDB Import Script for osu! Recommender
# Optimized for large SQL dumps
#

set -e

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
DATA_DIR="${PROJECT_DIR}/data"
SQL_DIR="${DATA_DIR}/ingest/2026-02/sql"
DB_NAME="osu_import"
DB_USER="root"
DB_HOST="localhost"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Tables in order of dependency (small to large)
TABLES=(
    "osu_counts"
    "osu_difficulty_attribs"
    "osu_beatmap_performance_blacklist"
    "sample_users"
    "osu_user_stats"
    "osu_beatmapsets"
    "osu_beatmaps"
    "osu_beatmap_failtimes"
    "osu_beatmap_difficulty"
    "osu_user_beatmap_playcount"
    "osu_beatmap_difficulty_attribs"
    "osu_scores_high"
    "scores"
)

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root (needed for mysql operations)
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "Please run as root or with sudo"
        exit 1
    fi
}

# Check MariaDB/MySQL availability
check_mysql() {
    if ! command -v mysql &> /dev/null; then
        log_error "MySQL/MariaDB client not found"
        exit 1
    fi
    
    if ! systemctl is-active --quiet mariadb && ! systemctl is-active --quiet mysql; then
        log_error "MariaDB/MySQL service is not running"
        exit 1
    fi
    
    log_success "MySQL/MariaDB is available and running"
}

# Create database with optimized settings
setup_database() {
    log_info "Setting up database '${DB_NAME}'..."
    
    # Drop and recreate database
    mysql -u${DB_USER} -e "DROP DATABASE IF EXISTS ${DB_NAME};" || true
    mysql -u${DB_USER} -e "CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    
    log_success "Database '${DB_NAME}' created"
}

# Apply optimized session settings for fast import
optimize_session() {
    log_info "Applying optimized session settings..."
    
    mysql -u${DB_USER} ${DB_NAME} << EOF
-- Disable keys, foreign keys, and unique checks for speed
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;
SET AUTOCOMMIT = 0;
SET SQL_LOG_BIN = 0;

-- Optimize for bulk insert
SET SESSION bulk_insert_buffer_size = 536870912;
EOF
    
    log_success "Session optimized for fast import"
}

# Import a single SQL file
import_table() {
    local table=$1
    local sql_file="${SQL_DIR}/${table}.sql"
    
    if [ ! -f "$sql_file" ]; then
        log_warning "SQL file not found: ${sql_file}"
        return 1
    fi
    
    local file_size=$(stat -f%z "$sql_file" 2>/dev/null || stat -c%s "$sql_file" 2>/dev/null)
    local size_mb=$((file_size / 1024 / 1024))
    
    log_info "Importing ${table} (${size_mb}MB)..."
    
    local start_time=$(date +%s)
    
    # Import with pv for progress if available, otherwise standard
    if command -v pv &> /dev/null; then
        pv "$sql_file" | mysql -u${DB_USER} ${DB_NAME}
    else
        mysql -u${DB_USER} ${DB_NAME} < "$sql_file"
    fi
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    log_success "Imported ${table} in ${duration}s"
    
    # Return duration for statistics
    echo $duration
}

# Import all tables
import_all() {
    log_info "Starting import of ${#TABLES[@]} tables..."
    
    local total_start=$(date +%s)
    local total_rows=0
    
    for table in "${TABLES[@]}"; do
        log_info "========================================"
        if import_table "$table"; then
            ((total_rows++)) || true
        fi
    done
    
    local total_end=$(date +%s)
    local total_duration=$((total_end - total_start))
    
    log_info "========================================"
    log_success "Import completed!"
    log_info "Total time: ${total_duration}s ($(echo "scale=2; ${total_duration}/60" | bc) minutes)"
    log_info "Tables imported: ${total_rows}/${#TABLES[@]}"
}

# Verify import by counting rows
verify_import() {
    log_info "Verifying import..."
    
    mysql -u${DB_USER} ${DB_NAME} -e "
        SELECT 
            table_name,
            table_rows
        FROM information_schema.tables
        WHERE table_schema = '${DB_NAME}'
        ORDER BY table_rows DESC;
    "
}

# Export to TSV for DuckDB
export_to_tsv() {
    local output_dir="${DATA_DIR}/ingest/2026-02/tsv_export"
    
    log_info "Exporting tables to TSV..."
    mkdir -p "$output_dir"
    
    for table in "${TABLES[@]}"; do
        log_info "Exporting ${table} to TSV..."
        
        mysql -u${DB_USER} ${DB_NAME} -e "
            SELECT * FROM ${table}
            INTO OUTFILE '${output_dir}/${table}.tsv'
            FIELDS TERMINATED BY '\t'
            OPTIONALLY ENCLOSED BY ''
            LINES TERMINATED BY '\n';
        " 2>/dev/null || {
            # Fallback: use mysql client export
            mysql -u${DB_USER} ${DB_NAME} --batch --raw --skip-column-names -e "SELECT * FROM ${table}" > "${output_dir}/${table}.tsv"
        }
    done
    
    log_success "TSV export completed to ${output_dir}"
}

# Main execution
main() {
    echo "========================================"
    echo "Fast MySQL Import for osu! Recommender"
    echo "========================================"
    echo ""
    
    check_root
    check_mysql
    setup_database
    optimize_session
    import_all
    verify_import
    
    echo ""
    echo "========================================"
    log_success "All done! Database '${DB_NAME}' is ready."
    echo ""
    echo "Next steps:"
    echo "  1. Export to TSV: ./import_mysql.sh --export-tsv"
    echo "  2. Or use DuckDB MySQL extension to query directly"
    echo ""
}

# Handle command line arguments
case "${1:-}" in
    --export-tsv)
        export_to_tsv
        ;;
    --verify)
        verify_import
        ;;
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  (no args)     Import all SQL files into MySQL"
        echo "  --export-tsv  Export imported tables to TSV files"
        echo "  --verify      Verify import and show row counts"
        echo "  --help        Show this help message"
        ;;
    *)
        main
        ;;
esac
