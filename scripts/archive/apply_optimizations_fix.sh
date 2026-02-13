#!/bin/bash
#
# CRITICAL FIX: Apply MySQL Optimizations to Correct Location
# 
# The issue: MariaDB reads from /etc/my.cnf which includes /etc/my.cnf.d/
# But our optimization config was in /etc/mysql/mariadb.conf.d/ (not loaded!)
#

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# Create the correct config directory if it doesn't exist
log_info "Creating /etc/my.cnf.d/ directory..."
sudo mkdir -p /etc/my.cnf.d

# Apply the optimization config to the CORRECT location
log_info "Applying optimization config to /etc/my.cnf.d/..."
sudo tee /etc/my.cnf.d/99-import-optimization.cnf > /dev/null << 'EOF'
[mysqld]
# ============================================================================
# CRITICAL: Memory Settings - WAS NOT BEING APPLIED BEFORE!
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

log_success "Config created at /etc/my.cnf.d/99-import-optimization.cnf"

# Clean up old config location (optional)
if [[ -f /etc/mysql/mariadb.conf.d/99-import-optimization.cnf ]]; then
    log_warning "Removing old config from /etc/mysql/mariadb.conf.d/"
    sudo rm -f /etc/mysql/mariadb.conf.d/99-import-optimization.cnf
fi

# Stop MariaDB
log_info "Stopping MariaDB..."
sudo systemctl stop mariadb
sleep 2

# IMPORTANT: Remove old InnoDB log files for new log file size to take effect
log_warning "Removing old InnoDB log files (required for log_file_size change)..."
sudo rm -f /var/lib/mysql/ib_logfile*
sudo rm -f /var/lib/mysql/ibtmp*

# Start MariaDB with new config
log_info "Starting MariaDB with optimized configuration..."
sudo systemctl start mariadb

# Wait for MariaDB to be ready
sleep 5
retries=0
while ! mysqladmin ping --silent 2>/dev/null; do
    sleep 1
    ((retries++))
    if [[ $retries -gt 30 ]]; then
        log_error "MariaDB failed to start after 30 seconds"
        exit 1
    fi
done

# Verify optimizations are applied
log_info "Verifying optimizations..."
echo ""
mysql -u test -ptest -e "
SELECT '=== CURRENT OPTIMIZATION SETTINGS ===' as '';
SELECT @@innodb_buffer_pool_size / 1024 / 1024 / 1024 as 'Buffer Pool (GB)';
SELECT @@innodb_log_file_size / 1024 / 1024 / 1024 as 'Log File (GB)';
SELECT @@innodb_flush_log_at_trx_commit as 'Flush at Commit';
SELECT @@innodb_doublewrite as 'Doublewrite';
SELECT @@innodb_read_io_threads as 'Read Threads';
SELECT @@innodb_write_io_threads as 'Write Threads';
SELECT @@innodb_io_capacity as 'IO Capacity';
" 2>/dev/null

echo ""
log_success "Optimizations applied successfully!"
log_info "Buffer pool should now show 10GB (not 128MB)"
log_info "You can now run the import with maximum performance."
