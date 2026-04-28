#!/usr/bin/env bash
set -euo pipefail

# Azure Functions trace download helper
#
# Usage:
#   bash scripts/download_azure_traces.sh
#
# This script downloads and extracts:
#   - Azure Functions Trace 2019
#   - Azure Functions Invocation Trace 2021
#
# macOS note:
#   The 2021 dataset is distributed as a .rar archive. If neither `unar` nor
#   `unrar` is installed, install one with:
#     brew install unar

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DATA_2019_DIR="${ROOT_DIR}/data/azure_functions_2019"
DATA_2021_DIR="${ROOT_DIR}/data/azure_functions_2021"
EXTRACT_2019_DIR="${DATA_2019_DIR}/extracted"
EXTRACT_2021_DIR="${DATA_2021_DIR}/extracted"

URL_2019="https://azurepublicdatasettraces.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/azurefunctions-dataset2019.tar.xz"
URL_2021="https://github.com/Azure/AzurePublicDataset/raw/master/data/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar"

ARCHIVE_2019="${DATA_2019_DIR}/azurefunctions-dataset2019.tar.xz"
ARCHIVE_2021="${DATA_2021_DIR}/AzureFunctionsInvocationTraceForTwoWeeksJan2021.rar"

mkdir -p \
  "${DATA_2019_DIR}" \
  "${DATA_2021_DIR}" \
  "${EXTRACT_2019_DIR}" \
  "${EXTRACT_2021_DIR}"

download_if_missing() {
  local url="$1"
  local output_path="$2"
  local partial_path="${output_path}.part"

  if [[ -f "${output_path}" ]]; then
    echo "Already downloaded: ${output_path}"
    return
  fi

  echo "Downloading: ${url}"
  rm -f "${partial_path}"
  curl --fail --location --progress-bar --output "${partial_path}" "${url}"
  mv "${partial_path}" "${output_path}"
}

directory_has_files() {
  local dir="$1"
  [[ -n "$(find "${dir}" -mindepth 1 -print -quit)" ]]
}

download_if_missing "${URL_2019}" "${ARCHIVE_2019}"
download_if_missing "${URL_2021}" "${ARCHIVE_2021}"

if directory_has_files "${EXTRACT_2019_DIR}"; then
  echo "Already extracted: ${EXTRACT_2019_DIR}"
else
  echo "Extracting 2019 trace to: ${EXTRACT_2019_DIR}"
  tar -xJf "${ARCHIVE_2019}" -C "${EXTRACT_2019_DIR}"
fi

if directory_has_files "${EXTRACT_2021_DIR}"; then
  echo "Already extracted: ${EXTRACT_2021_DIR}"
else
  echo "Extracting 2021 trace to: ${EXTRACT_2021_DIR}"

  if command -v unar >/dev/null 2>&1; then
    unar -quiet -o "${EXTRACT_2021_DIR}" "${ARCHIVE_2021}"
  elif command -v unrar >/dev/null 2>&1; then
    unrar x -o+ "${ARCHIVE_2021}" "${EXTRACT_2021_DIR}/"
  else
    cat <<'EOF'
RAR extractor not found.

Install unar on macOS, then rerun this script:
  brew install unar
EOF
    exit 1
  fi
fi

echo
echo "Extracted 2019 files:"
find "${EXTRACT_2019_DIR}" -maxdepth 3 -type f | sort | head -n 50

echo
echo "Extracted 2021 files:"
find "${EXTRACT_2021_DIR}" -maxdepth 3 -type f | sort | head -n 50
