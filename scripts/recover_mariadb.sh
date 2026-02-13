#!/bin/bash
#
# RECOVER MARIADB AFTER INNODB FAILURE
# Run with sudo
#

set -e

echo "=========================================="
echo "MariaDB Recovery Script"
echo "=========================================="

# Step 1: Stop MariaDB if running
echo "[1/5] Stopping MariaDB..."
systemctl stop mariadb 2>/dev/null || true
sleep 2

# Step 2: Remove broken config
echo "[2/5] Removing broken optimization config..."
rm -f /etc/my.cnf.d/99-import-optimization.cnf
rm -f /etc/mysql/mariadb.conf.d/99-import-optimization.cnf

# Step 3: Try recovery with force_recovery
echo "[3/5] Attempting recovery with innodb_force_recovery=1..."

# Add recovery mode temporarily
cat > /etc/mysql/mariadb.conf.d/99-recovery.cnf << 'EOF'
[mysqld]
innodb_force_recovery = 1
EOF

# Try to start
if systemctl start mariadb; then
    echo "✓ Recovery successful with force_recovery=1"
    
    # Wait for it to be ready
    sleep 3
    
    # Dump existing data
    echo "[4/5] Backing up existing databases..."
    mkdir -p /tmp/mysql_backup
    mysqldump -u test -ptest --all-databases > /tmp/mysql_backup/all_databases.sql 2>/dev/null || echo "Warning: Could not dump all databases"
    
    # Stop MariaDB
    systemctl stop mariadb
    sleep 2
    
    # Remove recovery mode
    rm -f /etc/mysql/mariadb.conf.d/99-recovery.cnf
    
    # Start normally
    echo "[5/5] Starting MariaDB normally..."
    systemctl start mariadb
    
    echo ""
    echo "=========================================="
    echo "✓ RECOVERY SUCCESSFUL"
    echo "=========================================="
    echo "Your data has been preserved!"
    echo ""
    echo "Next steps:"
    echo "  1. Verify MariaDB is running: sudo systemctl status mariadb"
    echo "  2. Check your tables: mysql -u test -ptest -e 'USE osu; SHOW TABLES;'"
    echo "  3. Apply optimizations properly (see docs)"
    
else
    echo "✗ Recovery with force_recovery=1 failed"
    echo ""
    echo "Attempting with force_recovery=2..."
    
    cat > /etc/mysql/mariadb.conf.d/99-recovery.cnf << 'EOF'
[mysqld]
innodb_force_recovery = 2
EOF
    
    if systemctl start mariadb; then
        echo "✓ Recovery successful with force_recovery=2"
        
        # Backup and restart normally
        sleep 3
        mysqldump -u test -ptest --all-databases > /tmp/mysql_backup/all_databases.sql 2>/dev/null || true
        systemctl stop mariadb
        sleep 2
        rm -f /etc/mysql/mariadb.conf.d/99-recovery.cnf
        systemctl start mariadb
        
        echo ""
        echo "=========================================="
        echo "✓ RECOVERY SUCCESSFUL"
        echo "=========================================="
        
    else
        echo "✗ Recovery failed with force_recovery=2"
        echo ""
        echo "=========================================="
        echo "⚠ CLEAN REINSTALL REQUIRED"
        echo "=========================================="
        echo ""
        echo "Your existing data will be lost."
        echo "To proceed with clean reinstall, run:"
        echo ""
        echo "  sudo systemctl stop mariadb"
        echo "  sudo rm -rf /var/lib/mysql/*"
        echo "  sudo mysql_install_db --user=mysql --basedir=/usr --datadir=/var/lib/mysql"
        echo "  sudo systemctl start mariadb"
        echo "  sudo mysql -e \"CREATE USER 'test'@'localhost' IDENTIFIED BY 'test';\""
        echo "  sudo mysql -e \"GRANT ALL PRIVILEGES ON *.* TO 'test'@'localhost';\""
        echo "  sudo mysql -e \"FLUSH PRIVILEGES;\""
        echo ""
        exit 1
    fi
fi
