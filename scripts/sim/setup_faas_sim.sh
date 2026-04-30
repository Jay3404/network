#!/usr/bin/env bash
set -euo pipefail

# Set up edgerun/faas-sim in a local ignored workspace.
#
# This script requires network access the first time it is run.
# It clones faas-sim into tools/faas-sim and installs it into .venv/faas-sim.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/tools"
REPO_DIR="${TOOLS_DIR}/faas-sim"
VENV_DIR="${ROOT_DIR}/.venv/faas-sim"

mkdir -p "${TOOLS_DIR}" "${ROOT_DIR}/.venv"

if [[ -d "${REPO_DIR}/.git" ]]; then
  echo "faas-sim already cloned: ${REPO_DIR}"
else
  git clone https://github.com/edgerun/faas-sim.git "${REPO_DIR}"
fi

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip setuptools wheel
python -m pip install -r "${REPO_DIR}/requirements.txt"
python -m pip install -e "${REPO_DIR}"

python - <<'PY'
import sim
print("faas-sim import OK:", sim.__file__)
PY

echo
echo "Activate with:"
echo "  source ${VENV_DIR}/bin/activate"
