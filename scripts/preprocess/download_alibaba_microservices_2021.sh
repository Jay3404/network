#!/usr/bin/env bash
set -euo pipefail

# Alibaba Cluster Trace microservices v2021 download helper.
#
# The full dataset is large:
#   Node       ~1.1 GiB
#   MSCallGraph ~25 GiB
#   MSResource  ~16 GiB
#   MSRTQps     ~19 GiB
#
# Usage:
#   bash scripts/preprocess/download_alibaba_microservices_2021.sh --manifest-only
#   bash scripts/preprocess/download_alibaba_microservices_2021.sh --resource-only
#   bash scripts/preprocess/download_alibaba_microservices_2021.sh --rtqps-limit 1 --callgraph-limit 1 --resource-limit 1
#   bash scripts/preprocess/download_alibaba_microservices_2021.sh --all --extract

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_DIR="${ROOT_DIR}/data/alibaba_clusterdata/raw/microservices_v2021"
BASE_URL="http://aliopentrace.oss-cn-beijing.aliyuncs.com/v2021MicroservicesTraces"

DOWNLOAD_NODE=0
DOWNLOAD_RESOURCE=0
DOWNLOAD_RTQPS=0
DOWNLOAD_CALLGRAPH=0
EXTRACT=0
MANIFEST_ONLY=0

NODE_LIMIT=1
RESOURCE_LIMIT=12
RTQPS_LIMIT=25
CALLGRAPH_LIMIT=145

usage() {
  sed -n '1,40p' "$0"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)
      DOWNLOAD_NODE=1
      DOWNLOAD_RESOURCE=1
      DOWNLOAD_RTQPS=1
      DOWNLOAD_CALLGRAPH=1
      shift
      ;;
    --node-only)
      DOWNLOAD_NODE=1
      shift
      ;;
    --resource-only)
      DOWNLOAD_RESOURCE=1
      shift
      ;;
    --rtqps-only)
      DOWNLOAD_RTQPS=1
      shift
      ;;
    --callgraph-only)
      DOWNLOAD_CALLGRAPH=1
      shift
      ;;
    --node-limit)
      NODE_LIMIT="$2"
      shift 2
      ;;
    --resource-limit)
      RESOURCE_LIMIT="$2"
      shift 2
      ;;
    --rtqps-limit)
      RTQPS_LIMIT="$2"
      shift 2
      ;;
    --callgraph-limit)
      CALLGRAPH_LIMIT="$2"
      shift 2
      ;;
    --extract)
      EXTRACT=1
      shift
      ;;
    --manifest-only)
      MANIFEST_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

mkdir -p \
  "${DATA_DIR}/Node" \
  "${DATA_DIR}/MSResource" \
  "${DATA_DIR}/MSRTQps" \
  "${DATA_DIR}/MSCallGraph"

archive_path() {
  local table="$1"
  local name="$2"
  echo "${DATA_DIR}/${table}/${name}.tar.gz"
}

archive_url() {
  local table="$1"
  local name="$2"
  local remote_table="$table"
  if [[ "$table" == "Node" ]]; then
    remote_table="node"
  fi
  echo "${BASE_URL}/${remote_table}/${name}.tar.gz"
}

print_manifest() {
  echo "table,file,url,local_path"
  echo "Node,Node_0,$(archive_url Node Node_0),$(archive_path Node Node_0)"
  for ((i=0; i<12; i++)); do
    echo "MSResource,MSResource_${i},$(archive_url MSResource MSResource_${i}),$(archive_path MSResource MSResource_${i})"
  done
  for ((i=0; i<25; i++)); do
    echo "MSRTQps,MSRTQps_${i},$(archive_url MSRTQps MSRTQps_${i}),$(archive_path MSRTQps MSRTQps_${i})"
  done
  for ((i=0; i<145; i++)); do
    echo "MSCallGraph,MSCallGraph_${i},$(archive_url MSCallGraph MSCallGraph_${i}),$(archive_path MSCallGraph MSCallGraph_${i})"
  done
}

download_one() {
  local table="$1"
  local name="$2"
  local url
  local output_path
  local partial_path

  url="$(archive_url "$table" "$name")"
  output_path="$(archive_path "$table" "$name")"
  partial_path="${output_path}.part"

  if [[ -f "$output_path" ]]; then
    echo "Already downloaded: $output_path"
  else
    echo "Downloading: $url"
    rm -f "$partial_path"
    curl --fail --location --retry 5 --connect-timeout 30 --output "$partial_path" "$url"
    mv "$partial_path" "$output_path"
  fi

  if [[ "$EXTRACT" -eq 1 ]]; then
    local extract_dir="${DATA_DIR}/${table}"
    local expected_csv="${extract_dir}/${name}.csv"
    if [[ -f "$expected_csv" ]]; then
      echo "Already extracted: $expected_csv"
    else
      echo "Extracting: $output_path"
      tar -xzf "$output_path" -C "$extract_dir"
    fi
  fi
}

if [[ "$MANIFEST_ONLY" -eq 1 ]]; then
  print_manifest
  exit 0
fi

if [[ "$DOWNLOAD_NODE" -eq 1 ]]; then
  for ((i=0; i<NODE_LIMIT; i++)); do
    download_one Node "Node_${i}"
  done
fi

if [[ "$DOWNLOAD_RESOURCE" -eq 1 ]]; then
  for ((i=0; i<RESOURCE_LIMIT; i++)); do
    download_one MSResource "MSResource_${i}"
  done
fi

if [[ "$DOWNLOAD_RTQPS" -eq 1 ]]; then
  for ((i=0; i<RTQPS_LIMIT; i++)); do
    download_one MSRTQps "MSRTQps_${i}"
  done
fi

if [[ "$DOWNLOAD_CALLGRAPH" -eq 1 ]]; then
  for ((i=0; i<CALLGRAPH_LIMIT; i++)); do
    download_one MSCallGraph "MSCallGraph_${i}"
  done
fi

echo
echo "Current Alibaba microservices files:"
find "$DATA_DIR" -maxdepth 2 -type f | sort | head -n 200
