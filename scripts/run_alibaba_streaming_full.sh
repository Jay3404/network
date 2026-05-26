#!/usr/bin/env bash
set -euo pipefail

# Full Alibaba graph/resource preprocessing with shard-level cleanup.
#
# Processing pattern:
#   download shard archive -> extract one CSV -> ingest into SQLite aggregate DB
#   -> delete archive and extracted CSV -> continue
#
# Recommended NAS usage:
#   ALIBABA_DATA_DIR=/Volumes/<NAS>/alibaba_microservices_2021 \
#   STREAM_OUTPUT_DIR="$PWD/outputs/week3_4/alibaba_streaming_full" \
#   bash scripts/run_alibaba_streaming_full.sh
#
# MSRTQps is intentionally skipped by default because week-3/4 uses Alibaba
# call graph, node state, and MS resource state. Set MSRTQPS_MAX_INDEX only when
# a later experiment explicitly consumes RT/QPS data.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BASE_URL="http://aliopentrace.oss-cn-beijing.aliyuncs.com/v2021MicroservicesTraces"
DATA_DIR="${ALIBABA_DATA_DIR:-${ROOT_DIR}/data/alibaba_microservices_2021_streaming}"
RAW_DIR="${DATA_DIR}/raw"
EXTRACT_DIR="${DATA_DIR}/extracted"

PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"
STREAM_OUTPUT_DIR="${STREAM_OUTPUT_DIR:-${ROOT_DIR}/outputs/week3_4/alibaba_streaming_full}"
STREAM_DB_PATH="${STREAM_DB_PATH:-${STREAM_OUTPUT_DIR}/alibaba_streaming.sqlite}"
CHUNK_SIZE="${CHUNK_SIZE:-500000}"

NODE_MAX_INDEX="${NODE_MAX_INDEX:-0}"
MSRESOURCE_MAX_INDEX="${MSRESOURCE_MAX_INDEX:-11}"
MSCALLGRAPH_MAX_INDEX="${MSCALLGRAPH_MAX_INDEX:-144}"
MSRTQPS_MAX_INDEX="${MSRTQPS_MAX_INDEX:--1}"

DELETE_ARCHIVE_AFTER_EXTRACT="${DELETE_ARCHIVE_AFTER_EXTRACT:-1}"
DELETE_EXTRACTED_AFTER_INGEST="${DELETE_EXTRACTED_AFTER_INGEST:-1}"
CURL_RETRY="${CURL_RETRY:-20}"
CURL_RETRY_DELAY="${CURL_RETRY_DELAY:-10}"
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-30}"
CURL_SPEED_LIMIT="${CURL_SPEED_LIMIT:-1024}"
CURL_SPEED_TIME="${CURL_SPEED_TIME:-120}"

mkdir -p \
  "${RAW_DIR}/Node" \
  "${RAW_DIR}/MSResource" \
  "${RAW_DIR}/MSRTQps" \
  "${RAW_DIR}/MSCallGraph" \
  "${EXTRACT_DIR}/Node" \
  "${EXTRACT_DIR}/MSResource" \
  "${EXTRACT_DIR}/MSRTQps" \
  "${EXTRACT_DIR}/MSCallGraph" \
  "${STREAM_OUTPUT_DIR}"

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
    --connect-timeout "${CURL_CONNECT_TIMEOUT}" \
    --speed-limit "${CURL_SPEED_LIMIT}" \
    --speed-time "${CURL_SPEED_TIME}" \
    --silent \
    --show-error \
    --output "${partial_path}" \
    "${url}"
  mv "${partial_path}" "${output_path}"
}

ingested() {
  local source_id="$1"
  "${PYTHON_BIN}" "${ROOT_DIR}/01_jay/alibaba_microservices_streaming.py" \
    --mode is_ingested \
    --db-path "${STREAM_DB_PATH}" \
    --source-id "${source_id}" \
    >/dev/null 2>&1
}

process_shard() {
  local storage_component="$1"
  local url_component="$2"
  local prefix="$3"
  local max_index="$4"
  local stream_component="$5"

  if (( max_index < 0 )); then
    echo "Skipping ${storage_component}"
    return
  fi

  for index in $(seq 0 "${max_index}"); do
    local source_id="${stream_component}:${prefix}_${index}"

    if ingested "${source_id}"; then
      echo "Already ingested: ${source_id}"
      continue
    fi

    local archive_name="${prefix}_${index}.tar.gz"
    local csv_name="${prefix}_${index}.csv"
    local raw_path="${RAW_DIR}/${storage_component}/${archive_name}"
    local extract_path="${EXTRACT_DIR}/${storage_component}"
    local csv_path="${extract_path}/${csv_name}"

    download_if_missing "${BASE_URL}/${url_component}/${archive_name}" "${raw_path}"

    if [[ ! -f "${csv_path}" ]]; then
      echo "Extracting: ${raw_path}"
      tar -xzf "${raw_path}" -C "${extract_path}"
    else
      echo "Already extracted: ${csv_path}"
    fi

    if [[ ! -f "${csv_path}" ]]; then
      echo "Expected CSV not found after extraction: ${csv_path}" >&2
      exit 1
    fi

    "${PYTHON_BIN}" "${ROOT_DIR}/01_jay/alibaba_microservices_streaming.py" \
      --mode ingest \
      --db-path "${STREAM_DB_PATH}" \
      --component "${stream_component}" \
      --file "${csv_path}" \
      --source-id "${source_id}" \
      --chunk-size "${CHUNK_SIZE}"

    if [[ "${DELETE_ARCHIVE_AFTER_EXTRACT}" == "1" ]]; then
      echo "Deleting archive: ${raw_path}"
      rm -f "${raw_path}"
    fi
    if [[ "${DELETE_EXTRACTED_AFTER_INGEST}" == "1" ]]; then
      echo "Deleting extracted CSV: ${csv_path}"
      rm -f "${csv_path}"
    fi
  done
}

"${PYTHON_BIN}" "${ROOT_DIR}/01_jay/alibaba_microservices_streaming.py" \
  --mode init \
  --db-path "${STREAM_DB_PATH}"

process_shard "Node" "node" "Node" "${NODE_MAX_INDEX}" "node"
process_shard "MSResource" "MSResource" "MSResource" "${MSRESOURCE_MAX_INDEX}" "resource"
process_shard "MSCallGraph" "MSCallGraph" "MSCallGraph" "${MSCALLGRAPH_MAX_INDEX}" "callgraph"

if (( MSRTQPS_MAX_INDEX >= 0 )); then
  echo "MSRTQps streaming is not implemented because week-3/4 does not consume it." >&2
  echo "Set MSRTQPS_MAX_INDEX=-1 or add an RT/QPS consumer before enabling it." >&2
  exit 1
fi

"${PYTHON_BIN}" "${ROOT_DIR}/01_jay/alibaba_microservices_streaming.py" \
  --mode finalize \
  --db-path "${STREAM_DB_PATH}" \
  --output-dir "${STREAM_OUTPUT_DIR}" \
  --chunk-size "${CHUNK_SIZE}"

echo
echo "Streaming preprocessing complete."
echo "Output directory: ${STREAM_OUTPUT_DIR}"
echo "SQLite aggregate DB: ${STREAM_DB_PATH}"
