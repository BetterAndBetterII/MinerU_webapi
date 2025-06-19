#!/usr/bin/env bash
set -euo pipefail

. /app/venv/bin/activate
exec python /app/worker.py "$@"
