#!/usr/bin/env bash
set -euo pipefail

# Clone EdgeCloudSim into a local ignored workspace and check Java.
#
# EdgeCloudSim is Java/CloudSim based. The upstream README currently recommends
# Java 21+ and running tutorials/sample apps through an IDE or the bundled
# application scripts.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TOOLS_DIR="${ROOT_DIR}/tools"
REPO_DIR="${TOOLS_DIR}/EdgeCloudSim"

mkdir -p "${TOOLS_DIR}"

if [[ -d "${REPO_DIR}/.git" ]]; then
  echo "EdgeCloudSim already cloned: ${REPO_DIR}"
else
  git clone https://github.com/CagataySonmez/EdgeCloudSim.git "${REPO_DIR}"
fi

if command -v java >/dev/null 2>&1; then
  java -version
else
  cat <<'EOF'
Java was not found.

Install Java 21+ before running EdgeCloudSim tutorials.
On macOS, one option is:
  brew install openjdk@21
EOF
fi

echo
echo "Repository:"
echo "  ${REPO_DIR}"
echo
echo "Typical next step:"
echo "  import ${REPO_DIR} into IntelliJ IDEA or Eclipse and run a tutorial MainApp.java"
