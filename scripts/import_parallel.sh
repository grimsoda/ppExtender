#!/bin/bash
#
# Main Parallel Import Orchestrator
#
# Combines Phase 1 (SQL to CSV conversion) and Phase 2 (Parallel LOAD DATA INFILE)
# for maximum import speed.
#
# Usage:
#   ./import_parallel.sh --table scores
#   ./import_parallel.sh --all --jobs 8
#   ./import_parallel.sh --phase2-only --table scores
#

set -euo pipefail

# Configuration
PROJECT_DIR="/run/media/work/OS/ppExtender"
SQL_DIR="${PROJECT_DIR}/data/ingest/2026-02/sql"
CSV_DIR="${PROJECT_DIR}/data/csv_chunks"
DB_NAME="osu_import"
LOG_DIR="${PROJECT_DIR}/logs"

# Scripts
PHASE1_SCRIPT="${PROJECT_DIR}/scripts/sql_to_csv_parallel.py"
PHASE2_SCRIPT="${PROJECT_DIR}/scripts/parallel_load_data.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default settings
TABLE=""
ALL_TABLES=false
PARALLEL_JOBS=4
PHASE1_ONLY=false
PHASE2_ONLY=false
DRY_RUN=false
SKIP_SCHEMA=false
KEEP_CSV=false
VERBOSE=false

#######################################
# Logging Functions
#######################################

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
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_section() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

#######################################
# Validation Functions
#######################################

validate_environment() {
    log_info "Validating environment..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 not found"
        exit 1
    fi
    
    # Check scripts exist
    if [[ ! -f "$PHASE1_SCRIPT" ]]; then
        log_error "Phase 1 script not found: $PHASE1_SCRIPT"
        exit 1
    fi
    
    if [[ ! -f "$PHASE2_SCRIPT" ]]; then
        log_error "Phase 2 script not found: $PHASE2_SCRIPT"
        exit 1
    fi
    
    # Check SQL directory
    if [[ ! -d "$SQL_DIR" ]]; then
        log_error "SQL directory not found: $SQL_DIR"
        exit 1
    fi
    
    # Create required directories
    mkdir -p "$CSV_DIR" "$LOG_DIR"
    
    log_success "Environment validated"
}

#######################################
# Phase 1: SQL to CSV Conversion
#######################################

run_phase1() {
    log_section "Phase 1: SQL to CSV Conversion"
    
    local phase1_args=()
    
    if [[ "$ALL_TABLES" == true ]]; then
        phase1_args+=("--all")
    elif [[ -n "$TABLE" ]]; then
        phase1_args+=("--table" "$TABLE")
    fi
    
    if [[ "$DRY_RUN" == true ]]; then
        phase1_args+=("--dry-run")
    fi
    
    if [[ "$VERBOSE" == true ]]; then
        phase1_args+=("--verbose")
    fi
    
    if [[ "$KEEP_CSV" == true ]]; then
        phase1_args+=("--keep-existing")
    fi
    
    log_info "Running: python3 $PHASE1_SCRIPT ${phase1_args[*]}"
    
    if ! python3 "$PHASE1_SCRIPT" "${phase1_args[@]}"; then
        log_error "Phase 1 failed"
        exit 1
    fi
    
    log_success "Phase 1 completed"
}

#######################################
# Phase 2: Parallel LOAD DATA INFILE
#######################################

run_phase2() {
    log_section "Phase 2: Parallel LOAD DATA INFILE"
    
    local phase2_args=()
    
    if [[ "$ALL_TABLES" == true ]]; then
        phase2_args+=("--all")
    elif [[ -n "$TABLE" ]]; then
        phase2_args+=("--table" "$TABLE")
    fi
    
    phase2_args+=("--jobs" "$PARALLEL_JOBS")
    
    if [[ "$DRY_RUN" == true ]]; then
        phase2_args+=("--dry-run")
    fi
    
    if [[ "$SKIP_SCHEMA" == true ]]; then
        phase2_args+=("--skip-schema")
    fi
    
    if [[ "$KEEP_CSV" == true ]]; then
        phase2_args+=("--keep-csv")
    fi
    
    log_info "Running: $PHASE2_SCRIPT ${phase2_args[*]}"
    
    if ! bash "$PHASE2_SCRIPT" "${phase2_args[@]}"; then
        log_warning "Phase 2 completed with some warnings"
    fi
    
    log_success "Phase 2 completed"
}

#######################################
# Summary and Reporting
#######################################

show_summary() {
    log_section "Import Summary"
    
    log_info "Configuration:"
    log_info "  Database: $DB_NAME"
    log_info "  Parallel jobs: $PARALLEL_JOBS"
    log_info "  CSV directory: $CSV_DIR"
    
    if [[ -n "$TABLE" ]]; then
        log_info "  Table: $TABLE"
    else
        log_info "  Tables: ALL"
    fi
    
    if [[ "$DRY_RUN" == true ]]; then
        log_info "  Mode: DRY RUN"
    fi
    
    echo ""
    log_info "Next steps:"
    log_info "  1. Verify import: mysql -u root -e 'USE $DB_NAME; SHOW TABLES;'"
    log_info "  2. Check row counts: mysql -u root -e 'SELECT COUNT(*) FROM \`table_name\`;'"
    log_info "  3. View logs: ls -la $LOG_DIR/"
}

#######################################
# Main Execution
#######################################

main() {
    local start_time=$(date +%s)
    
    log_section "Parallel MySQL Import System"
    log_info "Starting at $(date)"
    
    validate_environment
    
    # Run phases
    if [[ "$PHASE2_ONLY" == false ]]; then
        run_phase1
    else
        log_info "Skipping Phase 1 (--phase2-only specified)"
    fi
    
    if [[ "$PHASE1_ONLY" == false ]]; then
        run_phase2
    else
        log_info "Skipping Phase 2 (--phase1-only specified)"
    fi
    
    # Summary
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local minutes=$((duration / 60))
    local seconds=$((duration % 60))
    
    log_section "Import Complete"
    log_success "Total time: ${minutes}m ${seconds}s"
    
    show_summary
}

#######################################
# Help
#######################################

show_help() {
    cat << EOF
Parallel MySQL Import System - Main Orchestrator

Combines SQL to CSV conversion and parallel LOAD DATA INFILE for maximum speed.

Usage: $0 [OPTIONS]

Options:
    --table TABLE       Import specific table only
    --all               Import all tables
    --jobs N            Number of parallel jobs (default: 4)
    
    --phase1-only       Run only Phase 1 (SQL to CSV conversion)
    --phase2-only       Run only Phase 2 (LOAD DATA INFILE)
                        Requires CSV chunks to exist
    
    --dry-run           Show what would be done without executing
    --skip-schema       Skip CREATE TABLE statements (table must exist)
    --keep-csv          Keep CSV files after import
    --verbose           Enable verbose output
    
    --help, -h          Show this help message

Examples:
    # Import single table with 8 parallel jobs
    $0 --table scores --jobs 8
    
    # Import all tables
    $0 --all --jobs 4
    
    # Dry run to see what would happen
    $0 --table scores --dry-run
    
    # Convert SQL to CSV only
    $0 --all --phase1-only
    
    # Load existing CSV chunks
    $0 --all --phase2-only --jobs 8
    
    # Import with custom settings
    $0 --table scores --jobs 8 --keep-csv --verbose

Performance Tips:
    - Use --jobs 4-8 for large tables (scores, osu_scores_high)
    - Use --jobs 2 for medium tables
    - Use --jobs 1 for small tables (< 100MB)
    - SSD storage recommended for CSV chunks
    - Ensure MariaDB has sufficient buffer pool (8GB+)

EOF
}

#######################################
# Parse Arguments
#######################################

if [[ $# -eq 0 ]]; then
    show_help
    exit 0
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --table)
            TABLE="$2"
            shift 2
            ;;
        --all)
            ALL_TABLES=true
            shift
            ;;
        --jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --phase1-only)
            PHASE1_ONLY=true
            shift
            ;;
        --phase2-only)
            PHASE2_ONLY=true
            shift
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
        --verbose)
            VERBOSE=true
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

# Validate arguments
if [[ -z "$TABLE" && "$ALL_TABLES" == false ]]; then
    log_error "Must specify --table or --all"
    show_help
    exit 1
fi

# Run main
main
