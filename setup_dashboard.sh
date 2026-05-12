#!/usr/bin/env bash
# setup_dashboard.sh — Deploy the RERA Crawlers Dashboard as a public web service.
#
# What it does:
#   1. Installs gunicorn into the project virtualenv
#   2. Creates a systemd service that auto-starts on boot
#   3. Opens the dashboard port in ufw (if ufw is present)
#   4. Prints the public URL
#
# Usage:
#   bash setup_dashboard.sh               # install on port 8080 (default)
#   PORT=9090 bash setup_dashboard.sh     # install on a custom port
#   bash setup_dashboard.sh --remove      # stop + remove the service and firewall rule
#
# Requirements:
#   - Linux server with systemd
#   - Python virtualenv at ./venv  (run: python3 -m venv venv && pip install -r requirements.txt)
#   - sudo access (needed to write the systemd unit file and manage the service)

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PORT="${PORT:-8080}"
WORKERS="${WORKERS:-2}"
SERVICE_NAME="rera-dashboard"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# ── Resolve project root from script location ─────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SCRIPT_DIR/venv"
GUNICORN="$VENV/bin/gunicorn"
PYTHON="$VENV/bin/python3"

# ── Helpers ───────────────────────────────────────────────────────────────────
SEP="──────────────────────────────────────────────────────────────"

err() { echo "ERROR: $*" >&2; exit 1; }

check_prereqs() {
    [[ -f "$PYTHON" ]] \
        || err "virtualenv not found at $VENV\n       Run first:  python3 -m venv venv && pip install -r requirements.txt"
    [[ -f "$SCRIPT_DIR/dashboard.py" ]] \
        || err "dashboard.py not found in $SCRIPT_DIR"
    command -v systemctl &>/dev/null \
        || err "systemd not found — this script requires a Linux server with systemd"
}

public_ip() {
    # Try common metadata/lookup endpoints; fall back to local IP
    curl -s --max-time 3 https://api.ipify.org 2>/dev/null \
        || curl -s --max-time 3 http://ifconfig.me 2>/dev/null \
        || hostname -I 2>/dev/null | awk '{print $1}' \
        || echo "<your-server-ip>"
}

# ── Remove mode ───────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--remove" ]]; then
    echo "$SEP"
    echo "  Removing RERA Dashboard service"
    echo "$SEP"

    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        sudo systemctl stop "$SERVICE_NAME"
        echo "✓ Service stopped"
    fi
    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
        sudo systemctl disable "$SERVICE_NAME"
        echo "✓ Service disabled"
    fi
    if [[ -f "$SERVICE_FILE" ]]; then
        sudo rm "$SERVICE_FILE"
        sudo systemctl daemon-reload
        echo "✓ Service file removed"
    fi
    if command -v ufw &>/dev/null; then
        sudo ufw delete allow "${PORT}/tcp" 2>/dev/null && echo "✓ Firewall rule removed" || true
    fi

    echo ""
    echo "Dashboard service removed."
    exit 0
fi

# ── Install mode ──────────────────────────────────────────────────────────────
check_prereqs

echo ""
echo "$SEP"
echo "  RERA Crawlers Dashboard — Setup"
echo "  Project : $SCRIPT_DIR"
echo "  Port    : $PORT"
echo "  Workers : $WORKERS"
echo "  Service : $SERVICE_NAME"
echo "$SEP"
echo ""

# 1. Install gunicorn into the venv
echo "→ Installing gunicorn..."
"$VENV/bin/pip" install gunicorn --quiet
echo "  gunicorn $("$GUNICORN" --version 2>&1 | awk '{print $NF}')"

# 2. Write systemd unit file
echo "→ Creating systemd service at $SERVICE_FILE..."
CURRENT_USER="$(whoami)"

sudo tee "$SERVICE_FILE" > /dev/null << EOF
[Unit]
Description=RERA Crawlers Dashboard
Documentation=https://github.com/your-org/rera-crawlers
After=network.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${SCRIPT_DIR}
ExecStart=${GUNICORN} dashboard:app \\
    --bind 0.0.0.0:${PORT} \\
    --workers ${WORKERS} \\
    --timeout 30 \\
    --access-logfile - \\
    --error-logfile -
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

# 3. Enable and start
echo "→ Enabling and starting service..."
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

# Give it a moment to come up
sleep 2
if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo "  ✓ Service is running"
else
    echo "  ✗ Service failed to start — check logs:"
    echo "    sudo journalctl -u $SERVICE_NAME -n 30 --no-pager"
    exit 1
fi

# 4. Open firewall port
if command -v ufw &>/dev/null; then
    echo "→ Opening port $PORT in ufw..."
    sudo ufw allow "${PORT}/tcp" comment "RERA Dashboard" 2>/dev/null && echo "  ✓ Firewall rule added" || true
else
    echo "→ ufw not found — make sure port $PORT is open in your server's firewall/security group"
fi

# 5. Print access info
IP="$(public_ip)"
echo ""
echo "$SEP"
echo "  ✓  Dashboard is live!"
echo ""
echo "     URL  →  http://${IP}:${PORT}"
echo ""
echo "  Useful commands:"
echo "    sudo systemctl status  $SERVICE_NAME"
echo "    sudo systemctl restart $SERVICE_NAME"
echo "    sudo systemctl stop    $SERVICE_NAME"
echo "    sudo journalctl -u $SERVICE_NAME -f        # live logs"
echo ""
echo "  To remove:  bash $SCRIPT_DIR/setup_dashboard.sh --remove"
echo "$SEP"
echo ""
