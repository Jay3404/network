# Week 3-4. Alibaba Graph/Resource Prior 및 Edge Simulation 준비

## 1. 현재 상태 확인

1-2주차 작업은 Azure Functions trace 기반 burst 분석과 warm policy baseline으로 정리되어 있다.

현재 보완한 재현성 포인트:

| 항목 | 기존 상태 | 보완 |
|---|---|---|
| main subset | `week1.md`는 99% app coverage 기준을 설명 | `azure_trace_week1.py`에 `--coverage 0.99` 선택 경로 추가 |
| baseline 단위 | 문서는 app-level이지만 코드 기본값은 function top-k | `baseline_2019` 기본 단위를 app으로 변경하고 `--baseline-level` 추가 |
| latency tail | 기존 p95/p99는 active app-minute 기준 | request volume 가중 p95/p99 컬럼 추가 |

권장 재현 명령:

```bash
python code/azure_trace_week1.py --mode app_2019 --coverage 0.99 --window 60 --z 3
python code/azure_trace_week1.py --mode baseline_2019 --baseline-level app --coverage 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
```

## 2. 한계점 보완 전략

| 한계 | 영향 | 이번 보완 |
|---|---|---|
| Azure 2019/2021에는 edge topology가 없음 | network-aware placement를 직접 검증하기 어려움 | Alibaba node utilization으로 synthetic edge node pool과 link topology 생성 |
| Azure 2021 duration에는 cold start가 직접 포함되지 않음 | 실제 cold start latency 분리 불가 | cold-start penalty를 simulator parameter로 유지하고 민감도 분석 대상으로 둠 |
| Alibaba trace는 edge-native FaaS가 아님 | 직접 workload로 쓰면 연구 가정이 섞임 | workload stream은 Azure, dependency/resource prior는 Alibaba로 역할 분리 |
| Alibaba call graph는 sampled trace이고 일부 필드 누락 가능 | edge prior에 noise가 들어갈 수 있음 | `nan`, `(?)`, empty service id 제거 및 `min_call_count` 필터 적용 |
| 전체 Alibaba 데이터가 큼 | 빠른 반복 실험이 어려움 | shard 제한 다운로드와 `--max-files` smoke-test 경로 제공 |

핵심 원칙:

```text
Azure trace = invocation workload / burst timing
Alibaba trace = service dependency prior / resource affinity prior / node load prior
Synthetic topology = edge collaboration and placement experiment surface
```

## 3. 추가된 파일

| 파일 | 역할 |
|---|---|
| `scripts/download_alibaba_microservices.sh` | Alibaba Microservices v2021 다운로드/압축 해제 helper |
| `code/alibaba_microservices_week3.py` | call graph, resource, placement, topology prior 생성 |
| `code/alibaba_microservices_streaming.py` | 전체 shard를 SQLite aggregate DB로 streaming ingest |
| `scripts/run_alibaba_streaming_full.sh` | shard별 다운로드/해제/ingest/삭제 full runner |
| `scripts/setup_faas_sim.sh` | faas-sim을 `data/tools/faas-sim`에 설치하는 helper |
| `week3_4.md` | 3-4주차 실험 설계 및 실행 절차 |

## 4. Alibaba 데이터 준비

Python 의존성:

```bash
pip install -r requirements.txt
```

기본 명령은 node/resource/callgraph shard를 각각 일부만 받아 smoke test를 할 수 있게 되어 있다.

```bash
bash scripts/download_alibaba_microservices.sh
```

전체 데이터가 필요하면 다음처럼 범위를 늘린다.

```bash
NODE_MAX_INDEX=0 MSRESOURCE_MAX_INDEX=11 MSRTQPS_MAX_INDEX=24 MSCALLGRAPH_MAX_INDEX=144 \
  bash scripts/download_alibaba_microservices.sh
```

주의:

- 전체 call graph/resource/RT-QPS 데이터는 매우 크다.
- 원자료는 `data/alibaba_microservices_2021/` 아래에 저장되며 git에 포함하지 않는다.
- 초기 실험은 `MSCALLGRAPH_MAX_INDEX=0`, `MSRESOURCE_MAX_INDEX=0`으로 파이프라인을 먼저 검증한 뒤 확장한다.

NAS 또는 외장 저장소에서 전체 graph/resource 데이터를 처리할 때는 shard streaming runner를 사용한다.

```bash
ALIBABA_DATA_DIR=/Volumes/<NAS>/alibaba_microservices_2021 \
STREAM_OUTPUT_DIR="$PWD/outputs/week3_4/alibaba_streaming_full" \
NODE_MAX_INDEX=0 \
MSRESOURCE_MAX_INDEX=11 \
MSCALLGRAPH_MAX_INDEX=144 \
MSRTQPS_MAX_INDEX=-1 \
DELETE_ARCHIVE_AFTER_EXTRACT=1 \
DELETE_EXTRACTED_AFTER_INGEST=1 \
bash scripts/run_alibaba_streaming_full.sh
```

이 방식은 각 shard마다 다음 순서로 동작한다.

```text
download archive -> extract CSV -> ingest aggregate DB -> delete archive -> delete CSV
```

누적 상태는 `STREAM_OUTPUT_DIR/alibaba_streaming.sqlite`에 저장되고, 최종 prior CSV는 같은 output directory에 생성된다.
다운로드는 저속 timeout과 retry를 적용하고, 실패해도 `.part` 파일과 SQLite `ingested_sources` 상태를 이용해 재개할 수 있다.

## 5. Alibaba 구조 점검

```bash
python code/alibaba_microservices_week3.py --mode inspect
```

출력:

```text
outputs/week3_4/alibaba/inspect_summary.json
```

목적:

- 압축 해제 위치가 맞는지 확인
- 각 component 후보 파일 수 확인
- header 유무와 delimiter 확인

## 6. Graph Prior 정의

Alibaba `MS_CallGraph_Table`에서 upstream microservice `um`과 downstream microservice `dm` 간 edge를 만든다.

기본 edge 집계:

```text
edge = (um, dm, rpctype)
call_count = observed calls on edge
rt_abs_mean_ms = mean(abs(rt))
call_weight = call_count / max(call_count)
rt_weight = rt_abs_mean_ms / max(rt_abs_mean_ms)
graph_score = call_weight * (1 + rt_weight)
```

의미:

- `call_count`가 큰 edge는 같이 배치하거나 가까운 노드에 둘 가치가 높다.
- `rt_abs_mean_ms`가 큰 edge는 network latency가 추가될 때 tail latency 위험이 커진다.
- `graph_score`는 collaborative prewarming/placement에서 edge criticality prior로 사용한다.

실행:

```bash
python code/alibaba_microservices_week3.py --mode graph_prior --min-call-count 2 --top-edges 100000
```

출력:

| 파일 | 내용 |
|---|---|
| `graph_edges.csv` | 상위 graph edge prior |
| `service_graph_scores.csv` | service별 in/out degree, call count, graph score |
| `graph_prior_metadata.json` | 입력 파일, row 수, 필터 정보 |

## 7. Resource / Placement Prior 정의

Alibaba node table과 MS resource table에서 다음 prior를 만든다.

Node prior:

```text
available_cpu_score = 1 - avg_cpu_utilization
available_memory_score = 1 - avg_memory_utilization
available_score = 0.5 * available_cpu_score + 0.5 * available_memory_score
```

MS-node affinity:

```text
observation_share = observations(ms, node) / observations(ms)
affinity_score = observation_share * (1 + node_available_score)
```

실행:

```bash
python code/alibaba_microservices_week3.py --mode resource_prior --candidate-nodes-per-service 3
```

출력:

| 파일 | 내용 |
|---|---|
| `node_resource_summary.csv` | node별 평균 CPU/memory utilization과 available score |
| `ms_resource_summary.csv` | microservice별 resource demand 요약 |
| `ms_node_affinity.csv` | microservice-node 관측 affinity |
| `placement_candidates.csv` | service별 placement 후보 node |

## 8. Synthetic Edge Topology

Azure trace에 edge topology가 없으므로, Alibaba node summary에서 여유 자원이 큰 node를 edge node 후보로 선택한다.

실행:

```bash
python code/alibaba_microservices_week3.py --mode topology --edge-nodes 32 --neighbor-degree 2
```

출력:

| 파일 | 내용 |
|---|---|
| `edge_topology_nodes.csv` | synthetic edge node 목록 |
| `edge_topology_links.csv` | ring+chord 형태의 deterministic neighbor links |
| `edge_topology_metadata.json` | topology 생성 설정 |

이 topology는 실제 edge topology라고 주장하지 않는다. 목적은 topology density, hop latency, neighbor sharing의 민감도 분석을 위한 controlled experiment surface를 만드는 것이다.

## 9. faas-sim 준비

faas-sim은 trace-driven FaaS simulator이며 scheduling, autoscaling, load balancing, placement 전략 평가에 적합하다.

공식 faas-sim `requirements.txt`는 2020년대 초반 패키지를 강하게 pinning하고 있어 최신 Python에서는 설치가 깨진다. 따라서 본 실험에서는 simulator 소스는 공식 저장소를 사용하되, 의존성은 최신 호환 버전으로 올린 `requirements-faas-sim-modern.txt`를 사용한다. faas-sim 자체는 upstream pin을 피하기 위해 `--no-deps`로 설치한다.

설치 helper:

```bash
bash scripts/setup_faas_sim.sh
```

설치 위치:

```text
data/tools/faas-sim
```

Python 3.14 컨테이너 검증 결과:

```text
python=3.14.4
numpy=2.4.4
pandas=3.0.2
scipy=1.17.1
simpy=4.1.1
faas-sim import: OK
```

다음 단계의 simulator adapter는 아래 입력을 사용하면 된다.

| 입력 | 생성 파일 |
|---|---|
| Azure workload stream | `outputs/week1/.../*_workload.csv` |
| graph prior | `outputs/week3_4/alibaba/graph_edges.csv` |
| placement prior | `outputs/week3_4/alibaba/placement_candidates.csv` |
| edge topology | `outputs/week3_4/alibaba/edge_topology_nodes.csv`, `edge_topology_links.csv` |

## 10. 3-4주차 완료 기준

이번 단계의 완료 기준은 다음과 같다.

1. Alibaba trace를 로컬에서 일부 shard 이상 다운로드/해제한다.
2. `inspect`로 파일 구조를 확인한다.
3. `graph_prior`로 call graph edge prior를 생성한다.
4. `resource_prior`로 node/resource/placement prior를 생성한다.
5. `topology`로 synthetic edge topology를 생성한다.
6. 다음 단계에서 Azure workload와 Alibaba priors를 결합할 simulator adapter 입력 계약을 고정한다.

## 11. 실행 결과

실행 환경:

```text
Python 3.14.4 container
Alibaba Microservices v2021 shard set:
  Node_0.csv
  MSResource_0.csv
  MSCallGraph_0.csv
```

처리 규모:

| 항목 | 결과 |
|---|---:|
| call graph rows | 6,088,846 |
| valid call graph rows | 5,352,508 |
| graph edges after filter | 14,137 |
| upstream services | 1,385 |
| downstream services | 6,616 |
| node resource rows | 18,868,400 |
| node count | 13,116 |
| MS resource rows | 11,564,394 |
| microservices with resource summary | 1,303 |
| MS-node affinity pairs | 93,464 |
| placement candidates | 3,729 |
| synthetic edge nodes | 32 |
| synthetic topology links | 64 |

Graph prior 결과:

| 지표 | 값 |
|---|---:|
| edge call count p50 | 12 |
| edge call count p95 | 686.8 |
| edge call count p99 | 5,879.6 |
| max edge call count | 280,234 |
| edge mean RT p50 | 1.0 ms |
| edge mean RT p95 | 42.8 ms |
| edge mean RT p99 | 198.19 ms |
| max edge mean RT | 7,504.13 ms |

Resource/placement prior 결과:

| 지표 | 값 |
|---|---:|
| node available score p50 | 0.2968 |
| node available score p95 | 0.3848 |
| node available score p99 | 0.7755 |
| service node count p50 | 22 |
| service node count p95 | 314.9 |
| service node count p99 | 676.7 |
| resource demand score p50 | 0.3752 |
| resource demand score p95 | 0.5203 |
| rank-1 placement candidates | 1,303 |
| rank-2 placement candidates | 1,241 |
| rank-3 placement candidates | 1,185 |

Topology 결과:

| 지표 | 값 |
|---|---:|
| edge nodes | 32 |
| bidirectional ring-chord links | 64 |
| link latency p50 | 2.585 ms |
| link latency p95 | 3.035 ms |
| max link latency | 3.168 ms |

생성 파일:

| 파일 | 내용 |
|---|---|
| `outputs/week3_4/alibaba/graph_edges.csv` | graph-aware placement/prewarming edge prior |
| `outputs/week3_4/alibaba/service_graph_scores.csv` | service-level graph centrality/criticality score |
| `outputs/week3_4/alibaba/node_resource_summary.csv` | node resource state prior |
| `outputs/week3_4/alibaba/ms_resource_summary.csv` | microservice resource demand prior |
| `outputs/week3_4/alibaba/ms_node_affinity.csv` | observed service-node affinity |
| `outputs/week3_4/alibaba/placement_candidates.csv` | service별 top placement candidates |
| `outputs/week3_4/alibaba/edge_topology_nodes.csv` | synthetic edge nodes |
| `outputs/week3_4/alibaba/edge_topology_links.csv` | synthetic edge links |
| `outputs/week3_4/week3_4_summary.json` | 결과 요약 JSON |

전체 streaming 실행 결과:

```text
Output directory:
  outputs/week3_4/alibaba_streaming_full

Ingested sources:
  Node: 1 shard
  MSResource: 12 shards
  MSCallGraph: 145 shards
  Total: 158 sources

Cleanup:
  data/alibaba_microservices_2021_streaming: 0B remaining files
```

전체 처리 규모:

| 항목 | 결과 |
|---|---:|
| call graph shards | 145 |
| call graph rows | 996,409,549 |
| valid call graph rows | 848,641,265 |
| graph edges after filter | 52,641 |
| upstream services | 3,494 |
| downstream services | 15,936 |
| node shards | 1 |
| node resource rows | 18,868,400 |
| valid node resource rows | 18,865,518 |
| node count | 13,116 |
| MS resource shards | 12 |
| MS resource rows | 138,619,760 |
| valid MS resource rows | 138,603,758 |
| microservices with resource summary | 1,303 |
| MS-node affinity pairs | 93,540 |
| placement candidates | 3,729 |
| synthetic edge nodes | 32 |
| synthetic topology links | 64 |

전체 graph prior 결과:

| 지표 | 값 |
|---|---:|
| edge call count p50 | 50 |
| edge call count p95 | 14,985 |
| edge call count p99 | 203,587.4 |
| max edge call count | 38,705,878 |
| edge mean RT p50 | 1.0 ms |
| edge mean RT p95 | 43.78 ms |
| edge mean RT p99 | 256.94 ms |
| max edge mean RT | 38,242.5 ms |

전체 resource/placement prior 결과:

| 지표 | 값 |
|---|---:|
| node available score p50 | 0.2968 |
| node available score p95 | 0.3848 |
| node available score p99 | 0.7755 |
| service node count p50 | 22 |
| service node count p95 | 314.9 |
| service node count p99 | 676.7 |
| resource demand score p50 | 0.3747 |
| resource demand score p95 | 0.5157 |
| resource demand score p99 | 0.5902 |

전체 streaming 생성 파일:

| 파일 | 내용 |
|---|---|
| `outputs/week3_4/alibaba_streaming_full/alibaba_streaming.sqlite` | 재개 가능한 aggregate DB |
| `outputs/week3_4/alibaba_streaming_full/graph_edges.csv` | 전체 call graph 기반 edge prior |
| `outputs/week3_4/alibaba_streaming_full/service_graph_scores.csv` | 전체 call graph 기반 service score |
| `outputs/week3_4/alibaba_streaming_full/node_resource_summary.csv` | node resource prior |
| `outputs/week3_4/alibaba_streaming_full/ms_resource_summary.csv` | microservice resource demand prior |
| `outputs/week3_4/alibaba_streaming_full/ms_node_affinity.csv` | service-node affinity |
| `outputs/week3_4/alibaba_streaming_full/placement_candidates.csv` | service별 top placement candidates |
| `outputs/week3_4/alibaba_streaming_full/edge_topology_nodes.csv` | synthetic edge nodes |
| `outputs/week3_4/alibaba_streaming_full/edge_topology_links.csv` | synthetic edge links |
| `outputs/week3_4/alibaba_streaming_full/streaming_summary.json` | 전체 결과 요약 JSON |
| `outputs/week3_4/alibaba_streaming_full/streaming_metadata.json` | ingested source 목록과 설정 |

## 12. 결과 점검 및 재설계

초기 설계에서는 faas-sim upstream requirements를 그대로 설치하려고 했으나, 현재 Python/macOS 및 Python 3.14 환경에서 실패했다.

실패 원인:

```text
faas-sim upstream requirements:
  scipy==1.4.1
  numpy==1.22.0
  pandas==1.0.1
  scikit-learn==0.23.2

Python 3.14 failure:
  scipy==1.4.1 build dependency pulls numpy==1.17.3
  numpy==1.17.3 does not support Python 3.14 metadata/build path
```

재설계:

| 기존 | 변경 |
|---|---|
| upstream `requirements.txt` 그대로 설치 | 최신 패키지 기반 `requirements-faas-sim-modern.txt` 사용 |
| `pip install -e .`로 upstream dependency pin 강제 | `pip install --no-deps -e .`로 faas-sim 소스만 설치 |
| Python 3.8/3.9 legacy dependency 조합 시도 | Python 3.14.4 + latest dependency import 검증 |

최신 의존성 검증 결과:

```text
python=3.14.4
numpy=2.4.4
pandas=3.0.2
scipy=1.17.1
simpy=4.1.1
faas-sim import: OK
Simulation class import: OK
Topology class import: OK
```

로컬 macOS Python 3.9 환경에서도 같은 modern setup helper가 통과했다.

```text
python=3.9.6
numpy=2.0.2
pandas=2.3.3
scipy=1.13.1
simpy=4.1.1
faas-sim import: OK
Simulation class import: OK
Topology class import: OK
```

따라서 3-4주차 simulator setup은 upstream 구버전 pin을 따르지 않고, 최신 Python과 최신 dependency로 faas-sim 소스의 import surface를 확보하는 방식으로 보완한다.

## 13. 결론

3-4주차 목표는 완료되었다.

1. Alibaba call graph에서 service dependency edge prior를 생성했다.
2. Alibaba node/resource table에서 node resource state, microservice demand, service-node affinity를 생성했다.
3. Azure trace의 edge topology 부재 한계를 Alibaba node utilization 기반 synthetic topology로 보완했다.
4. faas-sim은 최신 Python에서 upstream dependency pin이 깨지는 것을 확인했고, 최신 의존성 기반 설치 방식으로 재설계했다.
5. 다음 단계의 collaborative prewarming simulator는 Azure workload stream, Alibaba graph/resource priors, synthetic edge topology를 입력으로 받을 수 있다.

핵심 해석:

- Alibaba call graph는 소수 edge에 호출이 집중되는 heavy-tail 구조를 보인다. 전체 streaming 기준 p99 edge call count가 203,587.4이고 max는 38,705,878로, graph-aware placement가 단순 랜덤 배치보다 유리할 여지가 크다.
- service별 node 분포도 넓다. service node count p95가 314.9이고 p99가 676.7이므로, resource affinity와 placement 후보를 분리해 모델링할 필요가 있다.
- topology는 실제 edge trace가 아니므로 실제성을 주장하지 않고, neighbor sharing과 hop latency sensitivity를 통제하는 실험 surface로 사용한다.
- 5-6주차에서는 이 산출물을 사용해 local-only, neighbor-aware, graph-aware collaborative warming을 같은 workload/latency/cost 지표로 비교하면 된다.

## 14. 다음 구현 대상

5-6주차 구현은 다음 순서가 적절하다.

| 순서 | 구현 |
|---|---|
| 1 | Azure app workload를 simulator event stream으로 변환 |
| 2 | local-only predictive warming을 node-local policy로 이식 |
| 3 | neighbor-aware shared warm pool 구현 |
| 4 | graph-aware placement score 추가 |
| 5 | burst predictor 오차를 주입하는 sensitivity 실험 |
| 6 | cold-start penalty, topology density, capacity 변화 ablation |

## 참고

- Alibaba Cluster Trace Microservices v2021: https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021
- faas-sim documentation: https://edgerun.github.io/faas-sim/
- faas-sim repository: https://github.com/edgerun/faas-sim
