#!/usr/bin/env bash
set -euo pipefail

# Local faas-sim setup helper with modern dependencies.
#
# The repository is cloned under data/tools so it stays outside git. faas-sim
# is used as the first simulator target because it is trace-driven and already
# models scheduling, autoscaling, load balancing, and placement extension points.
#
# Upstream faas-sim pins old 2020-era package versions that are not installable
# on current Python. This helper installs current packages from
# requirements-faas-sim-modern.txt and installs faas-sim itself with --no-deps.
#
# Usage:
#   bash scripts/setup_faas_sim.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/data/tools"
FAAS_SIM_DIR="${TOOLS_DIR}/faas-sim"

mkdir -p "${TOOLS_DIR}"

if [[ -d "${FAAS_SIM_DIR}/.git" ]]; then
  echo "Already cloned: ${FAAS_SIM_DIR}"
else
  git clone https://github.com/edgerun/faas-sim.git "${FAAS_SIM_DIR}"
fi

cd "${FAAS_SIM_DIR}"

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r "${ROOT_DIR}/requirements-faas-sim-modern.txt"
pip install --no-deps edgerun-ether
pip install --no-deps -e .

python - <<'PY'
import sys

import numpy
import pandas
import scipy
import simpy

import sim
from sim.faassim import Simulation
from sim.topology import Topology

print(f"python={sys.version.split()[0]}")
print(f"numpy={numpy.__version__}")
print(f"pandas={pandas.__version__}")
print(f"scipy={scipy.__version__}")
print(f"simpy={simpy.__version__}")
print(f"sim={sim.__file__}")
print(f"Simulation={Simulation.__name__}")
print(f"Topology={Topology.__name__}")
PY

cat <<EOF

faas-sim is available at:
  ${FAAS_SIM_DIR}

Activate it with:
  source ${FAAS_SIM_DIR}/.venv/bin/activate
EOF
