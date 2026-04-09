#!/bin/sh
set -eu

python3 -m pip install --quiet --no-cache-dir requests python-dotenv
python3 /workspace/vdc_api/tools/ontop/scripts/dremio_setup.py
