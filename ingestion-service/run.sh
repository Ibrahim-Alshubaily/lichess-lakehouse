#!/bin/bash
set -e

cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# lint and format
.venv/bin/ruff check .
.venv/bin/black .

python submit.py "$@"
