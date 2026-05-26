#!/usr/bin/env bash
set -euo pipefail

# Alibaba Microservices Trace v2021 download helper.
#
# The full dataset is large:
#   Node      ~1.1 GiB
#   MSCallGraph ~25 GiB
#   MSResource  ~16 GiB
#   MSRTQps     ~19 GiB
#
# Defaults download the smaller node table plus the first resource and callgraph
# shards for week-3/4 pipeline smoke tests. Increase the *_MAX_INDEX variables
# when running the full experiment.
#
# Use ALIBABA_DATA_DIR to place raw/extracted data on external storage or NAS.
# Use DELETE_ARCHIVE_AFTER_EXTRACT=1 to keep only extracted CSV files.
#
# Usage:
#   bash scripts/download_alibaba_microservices.sh
#
# Full graph/resource data on NAS:
#   ALIBABA_DATA_DIR=/Volumes/<NAS>/alibaba_microservices_2021 \
#   DELETE_ARCHIVE_AFTER_EXTRACT=1 \
#   NODE_MAX_INDEX=0 MSRESOURCE_MAX_INDEX=11 MSRTQPS_MAX_INDEX=-1 MSCALLGRAPH_MAX_INDEX=144 \
#     bash scripts/download_alibaba_microservices.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BASE_URL="http://aliopentrace.oss-cn-beijing.aliyuncs.com/v2021MicroservicesTraces"
DATA_DIR="${ALIBABA_DATA_DIR:-${ROOT_DIR}/data/alibaba_microservices_2021}"
RAW_DIR="${DATA_DIR}/raw"
EXTRACT_DIR="${DATA_DIR}/extracted"

NODE_MAX_INDEX="${NODE_MAX_INDEX:-0}"
MSRESOURCE_MAX_INDEX="${MSRESOURCE_MAX_INDEX:-0}"
MSRTQPS_MAX_INDEX="${MSRTQPS_MAX_INDEX:--1}"
MSCALLGRAPH_MAX_INDEX="${MSCALLGRAPH_MAX_INDEX:-0}"
EXTRACT_AFTER_DOWNLOAD="${EXTRACT_AFTER_DOWNLOAD:-1}"
DELETE_ARCHIVE_AFTER_EXTRACT="${DELETE_ARCHIVE_AFTER_EXTRACT:-0}"
CURL_RETRY="${CURL_RETRY:-5}"
CURL_RETRY_DELAY="${CURL_RETRY_DELAY:-10}"
CURL_SPEED_LIMIT="${CURL_SPEED_LIMIT:-51200}"
CURL_SPEED_TIME="${CURL_SPEED_TIME:-45}"

mkdir -p \
  "${RAW_DIR}/Node" \
  "${RAW_DIR}/MSResource" \
  "${RAW_DIR}/MSRTQps" \
  "${RAW_DIR}/MSCallGraph" \
  "${EXTRACT_DIR}/Node" \
  "${EXTRACT_DIR}/MSResource" \
  "${EXTRACT_DIR}/MSRTQps" \
  "${EXTRACT_DIR}/MSCallGraph"

download_if_missing() {
  local url="$1"
  local output_path="$2"
  local partial_path="${output_path}.part"

  if [[ -f "${output_path}" ]]; then
    echo "Already downloaded: ${output_path}"
    return
  fi

  echo "Downloading: ${url}"
  curl \
    --fail \
    --location \
    --continue-at - \
    --retry "${CURL_RETRY}" \
    --retry-delay "${CURL_RETRY_DELAY}" \
    --retry-all-errors \
    --retry-connrefused \
    --speed-limit "${CURL_SPEED_LIMIT}" \
    --speed-time "${CURL_SPEED_TIME}" \
    --progress-bar \
    --output "${partial_path}" \
    "${url}"
  mv "${partial_path}" "${output_path}"
}

extract_archive() {
  local archive_path="$1"
  local target_dir="$2"
  local marker="${target_dir}/.$(basename "${archive_path}").extracted"

  if [[ "${EXTRACT_AFTER_DOWNLOAD}" != "1" ]]; then
    return
  fi
  if [[ -f "${marker}" ]]; then
    echo "Already extracted: ${archive_path}"
    return
  fi

  echo "Extracting: ${archive_path}"
  tar -xzf "${archive_path}" -C "${target_dir}"
  touch "${marker}"

  if [[ "${DELETE_ARCHIVE_AFTER_EXTRACT}" == "1" ]]; then
    echo "Deleting archive after successful extraction: ${archive_path}"
    rm -f "${archive_path}"
  fi
}

download_range() {
  local component="$1"
  local url_component="$2"
  local prefix="$3"
  local max_index="$4"

  if (( max_index < 0 )); then
    echo "Skipping ${component}"
    return
  fi

  for index in $(seq 0 "${max_index}"); do
    local archive_name="${prefix}_${index}.tar.gz"
    local raw_path="${RAW_DIR}/${component}/${archive_name}"
    local extract_path="${EXTRACT_DIR}/${component}"
    download_if_missing "${BASE_URL}/${url_component}/${archive_name}" "${raw_path}"
    extract_archive "${raw_path}" "${extract_path}"
  done
}

download_range "Node" "node" "Node" "${NODE_MAX_INDEX}"
download_range "MSResource" "MSResource" "MSResource" "${MSRESOURCE_MAX_INDEX}"
download_range "MSRTQps" "MSRTQps" "MSRTQps" "${MSRTQPS_MAX_INDEX}"
download_range "MSCallGraph" "MSCallGraph" "MSCallGraph" "${MSCALLGRAPH_MAX_INDEX}"

echo
echo "Extracted Alibaba files:"
find "${EXTRACT_DIR}" -maxdepth 3 -type f | sort | head -n 80
