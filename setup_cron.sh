#!/usr/bin/env bash
# setup_cron.sh — Install the RERA daily + weekly cron jobs.
#
# Usage:
#   bash setup_cron.sh               # default: daily at 02:00, weekly at 03:00 Sunday
#   bash setup_cron.sh --remove      # remove both cron jobs
#   DAILY_HOUR=6 WEEKLY_HOUR=4 bash setup_cron.sh   # custom hours
#
# Safe to run multiple times — existing jobs are detected and not duplicated.

set -euo pipefail

# ── Configurable times (override via env vars) ────────────────────────────────
DAILY_HOUR="${DAILY_HOUR:-2}"       # hour for daily_light  (0-23)
DAILY_MIN="${DAILY_MIN:-0}"         # minute               (0-59)
WEEKLY_HOUR="${WEEKLY_HOUR:-3}"     # hour for weekly_deep (0-23)
WEEKLY_MIN="${WEEKLY_MIN:-0}"       # minute               (0-59)
WEEKLY_DOW="${WEEKLY_DOW:-0}"       # day of week: 0=Sunday … 6=Saturday

# ── Resolve paths relative to this script ────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$SCRIPT_DIR/venv/bin/python3"
RUNNER="$SCRIPT_DIR/run_crawlers.py"
LOG_DIR="$SCRIPT_DIR/logs"

# ── Validation ────────────────────────────────────────────────────────────────
if [[ ! -f "$PYTHON" ]]; then
    echo "ERROR: virtualenv python not found at $PYTHON"
    echo "       Create it first:  python3 -m venv venv && pip install -r requirements.txt"
    exit 1
fi
if [[ ! -f "$RUNNER" ]]; then
    echo "ERROR: run_crawlers.py not found at $RUNNER"
    exit 1
fi

mkdir -p "$LOG_DIR"

# ── Build the two cron lines ──────────────────────────────────────────────────
DAILY_CMD="cd \"$SCRIPT_DIR\" && \"$PYTHON\" run_crawlers.py --mode daily_light >> \"$LOG_DIR/cron_daily.log\" 2>&1"
WEEKLY_CMD="cd \"$SCRIPT_DIR\" && \"$PYTHON\" run_crawlers.py --mode weekly_deep >> \"$LOG_DIR/cron_weekly.log\" 2>&1"

DAILY_LINE="${DAILY_MIN} ${DAILY_HOUR} * * *   $DAILY_CMD"
WEEKLY_LINE="${WEEKLY_MIN} ${WEEKLY_HOUR} * * ${WEEKLY_DOW}   $WEEKLY_CMD"

# Unique marker strings used to detect existing jobs (avoids duplicate installs)
DAILY_MARKER="run_crawlers.py --mode daily_light"
WEEKLY_MARKER="run_crawlers.py --mode weekly_deep"

# ── Remove mode ───────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--remove" ]]; then
    echo "Removing RERA cron jobs..."
    crontab -l 2>/dev/null \
        | grep -v "$DAILY_MARKER" \
        | grep -v "$WEEKLY_MARKER" \
        | crontab -
    echo "Done. Remaining crontab:"
    crontab -l 2>/dev/null || echo "  (empty)"
    exit 0
fi

# ── Install mode ──────────────────────────────────────────────────────────────
CURRENT_CRON="$(crontab -l 2>/dev/null || true)"

DAILY_EXISTS=false
WEEKLY_EXISTS=false
echo "$CURRENT_CRON" | grep -q "$DAILY_MARKER"  && DAILY_EXISTS=true
echo "$CURRENT_CRON" | grep -q "$WEEKLY_MARKER" && WEEKLY_EXISTS=true

if $DAILY_EXISTS && $WEEKLY_EXISTS; then
    echo "Both cron jobs are already installed — nothing to do."
    echo ""
    crontab -l
    exit 0
fi

NEW_CRON="$CURRENT_CRON"

if ! $DAILY_EXISTS; then
    NEW_CRON="${NEW_CRON}
# RERA Crawlers — daily light crawl
${DAILY_LINE}"
    echo "Adding daily_light job:  ${DAILY_MIN} ${DAILY_HOUR}:00 every day"
fi

if ! $WEEKLY_EXISTS; then
    NEW_CRON="${NEW_CRON}
# RERA Crawlers — weekly deep crawl
${WEEKLY_LINE}"
    DOW_NAMES=(Sun Mon Tue Wed Thu Fri Sat)
    echo "Adding weekly_deep job:  ${WEEKLY_MIN} ${WEEKLY_HOUR}:00 every ${DOW_NAMES[$WEEKLY_DOW]}"
fi

# Strip leading blank lines, then install
echo "$NEW_CRON" | sed '/^[[:space:]]*$/d' | crontab -

echo ""
echo "Cron jobs installed successfully."
echo "Logs will be written to:"
echo "  $LOG_DIR/cron_daily.log"
echo "  $LOG_DIR/cron_weekly.log"
echo ""
echo "Current crontab:"
crontab -l
