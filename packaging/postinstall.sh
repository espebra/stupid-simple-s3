#!/bin/sh
set -e

# Create stupid user and group if they don't exist
if ! getent group stupid >/dev/null; then
    groupadd --system stupid
fi

if ! getent passwd stupid >/dev/null; then
    useradd --system --gid stupid --no-create-home --shell /usr/sbin/nologin stupid
fi

# Create data directories
mkdir -p /var/lib/stupid-simple-s3/data /var/lib/stupid-simple-s3/tmp
chown -R stupid:stupid /var/lib/stupid-simple-s3

# Ensure config directory exists
mkdir -p /etc/stupid-simple-s3

# Reload systemd
systemctl daemon-reload || true
