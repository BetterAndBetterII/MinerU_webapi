#!/usr/bin/env bash
set -euo pipefail

# Activate the virtual environment
. /app/venv/bin/activate

# Execute uvicorn using the virtual environment's python
# This is more explicit and robust than relying on the PATH set by activate
exec /app/venv/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8000
