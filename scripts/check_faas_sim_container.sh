#!/usr/bin/env bash
set -euo pipefail

# Backward-compatible wrapper. The old upstream faas-sim requirements are not
# installable on current Python, so the supported check is the modern one.

bash /work/scripts/check_faas_sim_modern.sh
