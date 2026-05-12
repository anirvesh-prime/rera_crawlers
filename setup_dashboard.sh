#!/usr/bin/env bash
# setup_dashboard.sh — Bootstrap and deploy the RERA Crawlers Dashboard.
#
# Handles everything from a blank Debian/Ubuntu server:
#   1. Installs python3-venv and python3-pip via apt (if missing)
#   2. Creates the virtualenv and installs all requirements
#   3. Creates a systemd service so the dashboard starts on boot
#   4. Opens the dashboard port in ufw (if present)
#   5. Prints the public URL
#
# Usage:
#   bash setup_dashboard.sh               # port 8080 (default)
#   PORT=9090 bash setup_dashboard.sh     # custom port
#   bash setup_dashboard.sh --remove      # stop + remove service + firewall rule
#
# Requirements: Debian/Ubuntu server with systemd and sudo access.

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PORT="${PORT:-8080}"
WORKERS="${WORKERS:-2}"
SERVICE_NAME="rera-dashboard"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SCRIPT_DIR/venv"
PYTHON="$VENV/bin/python3"
PIP="$VENV/bin/pip"
GUNICORN="$VENV/bin/gunicorn"

SEP="──────────────────────────────────────────────────────────────"

err()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "→ $*"; }
ok()   { echo "  ✓ $*"; }

# ── Detect system Python version (e.g. "3.10", "3.11") ───────────────────────
detect_python() {
    # Prefer python3 from PATH; must be 3.8+
    local py bin
    for bin in python3 python3.13 python3.12 python3.11 python3.10 python3.9 python3.8; do
        if command -v "$bin" &>/dev/null; then
            py="$bin"
            break
        fi
    done
    [[ -n "${py:-}" ]] || err "No Python 3 installation found. Install python3 first."
    local ver
    ver="$("$py" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    echo "$py $ver"   # two words: binary  major.minor
}

# ── Install python3-venv + python3-pip via apt ────────────────────────────────
ensure_venv_package() {
    local pyver="$1"   # e.g. "3.10"

    # Check if ensurepip works (venv package already installed)
    if python3 -c "import ensurepip" &>/dev/null 2>&1; then
        return 0
    fi

    info "Installing python${pyver}-venv and python3-pip via apt..."
    if ! command -v apt-get &>/dev/null; then
        err "apt-get not found. Install python3-venv manually for your distro, then re-run this script."
    fi
    sudo apt-get update -qq
    # Try the version-specific package first (e.g. python3.10-venv), fall back to generic
    sudo apt-get install -y "python${pyver}-venv" python3-pip \
        || sudo apt-get install -y python3-venv python3-pip
    ok "python${pyver}-venv installed"
}

# ── Create or repair the virtualenv ───────────────────────────────────────────
ensure_venv() {
    local pybinary="$1"

    if [[ -f "$PYTHON" ]]; then
        # Verify it's functional (broken venvs leave the file but fail to import)
        if "$PYTHON" -c "import sys" &>/dev/null 2>&1; then
            ok "Virtualenv already exists at $VENV"
            return 0
        fi
        info "Existing venv appears broken — recreating..."
        rm -rf "$VENV"
    fi

    info "Creating virtualenv at $VENV..."
    "$pybinary" -m venv "$VENV"
    ok "Virtualenv created"
}

# ── Install Python dependencies ───────────────────────────────────────────────
install_requirements() {
    local req="$SCRIPT_DIR/requirements.txt"
    [[ -f "$req" ]] || err "requirements.txt not found at $req"

    info "Installing Python dependencies..."
    "$PIP" install --quiet --upgrade pip
    "$PIP" install --quiet -r "$req"
    ok "Dependencies installed"
}

# ── Get public IP ─────────────────────────────────────────────────────────────
public_ip() {
    curl -s --max-time 4 https://api.ipify.org 2>/dev/null \
        || curl -s --max-time 4 http://ifconfig.me 2>/dev/null \
        || hostname -I 2>/dev/null | awk '{print $1}' \
        || echo "<your-server-ip>"
}

# ════════════════════════════════════════════════════════════════════════════════
# Remove mode
# ════════════════════════════════════════════════════════════════════════════════
if [[ "${1:-}" == "--remove" ]]; then
    echo "$SEP"
    echo "  Removing RERA Dashboard service"
    echo "$SEP"

    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        sudo systemctl stop "$SERVICE_NAME" && ok "Service stopped"
    fi
    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
        sudo systemctl disable "$SERVICE_NAME" && ok "Service disabled"
    fi
    if [[ -f "$SERVICE_FILE" ]]; then
        sudo rm "$SERVICE_FILE"
        sudo systemctl daemon-reload
        ok "Service file removed"
    fi
    if command -v ufw &>/dev/null; then
        sudo ufw delete allow "${PORT}/tcp" 2>/dev/null && ok "Firewall rule removed" || true
    fi
    echo ""
    echo "Dashboard service removed."
    exit 0
fi

# ════════════════════════════════════════════════════════════════════════════════
# Install mode
# ════════════════════════════════════════════════════════════════════════════════
[[ -f "$SCRIPT_DIR/dashboard.py" ]] \
    || err "dashboard.py not found in $SCRIPT_DIR — run this script from the project root"
command -v systemctl &>/dev/null \
    || err "systemd not found — this script requires a Linux server with systemd"

echo ""
echo "$SEP"
echo "  RERA Crawlers Dashboard — Setup"
echo "  Project : $SCRIPT_DIR"
echo "  Port    : $PORT"
echo "  Workers : $WORKERS"
echo "$SEP"
echo ""

# Step 1 — Python environment
read -r PY_BIN PY_VER <<< "$(detect_python)"
info "Found Python $PY_VER at $PY_BIN"
ensure_venv_package "$PY_VER"
ensure_venv "$PY_BIN"
install_requirements

# Step 2 — Systemd service
info "Creating systemd service at $SERVICE_FILE..."
CURRENT_USER="$(whoami)"

sudo tee "$SERVICE_FILE" > /dev/null << EOF
[Unit]
Description=RERA Crawlers Dashboard
After=network.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${SCRIPT_DIR}
ExecStart=${GUNICORN} dashboard:app \
    --bind 0.0.0.0:${PORT} \
    --workers ${WORKERS} \
    --timeout 30 \
    --access-logfile - \
    --error-logfile -
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF
ok "Service file written"

# Step 3 — Enable and start
info "Enabling and starting $SERVICE_NAME..."
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

sleep 2
if systemctl is-active --quiet "$SERVICE_NAME"; then
    ok "Service is running"
else
    echo "  ✗ Service failed to start. Check logs with:"
    echo "    sudo journalctl -u $SERVICE_NAME -n 40 --no-pager"
    exit 1
fi

# Step 4 — Firewall
if command -v ufw &>/dev/null; then
    info "Opening port $PORT in ufw..."
    sudo ufw allow "${PORT}/tcp" comment "RERA Dashboard" 2>/dev/null && ok "Firewall rule added" || true
else
    echo "  (ufw not found — open port $PORT in your cloud security group / firewall manually)"
fi

# Step 5 — Done
IP="$(public_ip)"
echo ""
echo "$SEP"
echo "  ✓  Dashboard is live!"
echo ""
echo "     URL  →  http://${IP}:${PORT}"
echo ""
echo "  Useful commands:"
echo "    sudo systemctl status  $SERVICE_NAME"
echo "    sudo systemctl restart $SERVICE_NAME   # after code changes"
echo "    sudo systemctl stop    $SERVICE_NAME"
echo "    sudo journalctl -u $SERVICE_NAME -f    # live logs"
echo ""
echo "  To remove:  bash $SCRIPT_DIR/setup_dashboard.sh --remove"
echo "$SEP"
echo ""
