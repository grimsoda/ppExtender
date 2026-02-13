#!/bin/bash
#
# Move MariaDB data directory to project volume for more storage
#

set -e

PROJECT_DIR="/run/media/work/OS/ppExtender"
NEW_DATA_DIR="${PROJECT_DIR}/data/mysql"
OLD_DATA_DIR="/var/lib/mysql"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check root
if [ "$EUID" -ne 0 ]; then
    log_error "Please run as root"
    exit 1
fi

log_info "Moving MariaDB data directory..."
log_info "From: ${OLD_DATA_DIR}"
log_info "To: ${NEW_DATA_DIR}"

# Stop MariaDB
log_info "Stopping MariaDB..."
systemctl stop mariadb || systemctl stop mysql

# Create new directory
log_info "Creating new data directory..."
mkdir -p "${NEW_DATA_DIR}"
chown mysql:mysql "${NEW_DATA_DIR}"
chmod 700 "${NEW_DATA_DIR}"

# Copy data
log_info "Copying data (this may take a while)..."
rsync -avP "${OLD_DATA_DIR}/" "${NEW_DATA_DIR}/"

# Backup old directory
log_info "Backing up old data directory..."
mv "${OLD_DATA_DIR}" "${OLD_DATA_DIR}.backup.$(date +%Y%m%d_%H%M%S)"

# Create symlink
log_info "Creating symlink..."
ln -s "${NEW_DATA_DIR}" "${OLD_DATA_DIR}"

# Update SELinux if needed
if command -v semanage &> /dev/null; then
    log_info "Updating SELinux context..."
    semanage fcontext -a -t mysqld_db_t "${NEW_DATA_DIR}(/.*)?"
    restorecon -Rv "${NEW_DATA_DIR}"
fi

# Start MariaDB
log_info "Starting MariaDB..."
systemctl start mariadb || systemctl start mysql

# Verify
if systemctl is-active --quiet mariadb || systemctl is-active --quiet mysql; then
    log_success "MariaDB is running with new data directory!"
    log_info "New location: ${NEW_DATA_DIR}"
    log_info "Available space: $(df -h "${NEW_DATA_DIR}" | awk 'NR==2 {print $4}')"
else
    log_error "MariaDB failed to start. Please check logs."
    exit 1
fi
