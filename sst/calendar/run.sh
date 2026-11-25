#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
source "$here/../../../SST/env.sh"
export PYTHONPATH="$here/../../src${PYTHONPATH:+:$PYTHONPATH}"
exec "$SST_PYTHON" "$here/run.py" "$@"
