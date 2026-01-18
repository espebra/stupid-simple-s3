#!/bin/sh
set -e

# Create sss user and group if they don't exist
if ! getent group sss >/dev/null; then
    groupadd --system sss
fi

if ! getent passwd sss >/dev/null; then
    useradd --system --gid sss --no-create-home --shell /usr/sbin/nologin sss
fi

# Create data directories
mkdir -p /var/lib/sss/data /var/lib/sss/tmp
chown -R sss:sss /var/lib/sss

# Reload systemd
systemctl daemon-reload || true
