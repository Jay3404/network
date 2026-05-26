#!/usr/bin/env bash
set -euo pipefail

# Check faas-sim with modern dependencies.
#
# Upstream faas-sim pins old packages that are not compatible with current
# Python releases. This script validates the import surface needed for our
# week-3/4 simulator-input preparation without using upstream dependency pins.

python -m pip install --upgrade pip
pip install -r /work/requirements-faas-sim-modern.txt
pip install --no-deps edgerun-ether

cp -R /work/data/tools/faas-sim /tmp/faas-sim
cd /tmp/faas-sim
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
