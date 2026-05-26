# 3-4주차 발표용 정리

## Slide 1. 주제

### Alibaba Microservices Trace 기반 Graph/Resource Prior 및 Simulation 준비

핵심 메시지:

- Azure trace에는 edge topology, service dependency, node resource 정보가 없다.
- Alibaba Microservices Trace를 이용해 service graph, resource state, placement prior를 생성했다.
- 5-6주차 collaborative prewarming 실험에 사용할 입력 기반을 준비했다.

## Slide 2. 1-2주차와의 연결

1-2주차에서 확인한 것:

- Azure Functions workload는 burst-heavy하다.
- local predictive와 z2-trigger가 강한 prewarming baseline이다.

그러나 Azure trace만으로는 다음 요소를 검증하기 어렵다.

| 부족한 정보 | 영향 |
|---|---|
| edge topology | neighbor collaboration 평가 어려움 |
| service dependency | graph-aware placement 평가 어려움 |
| node resource state | resource-aware placement 평가 어려움 |
| service-node affinity | realistic placement prior 부족 |

따라서 3-4주차에서는 Alibaba trace를 이용해 이 한계를 보완했다.

## Slide 3. 핵심 설계 원칙

Azure와 Alibaba의 역할을 분리했다.

| 역할 | 데이터 |
|---|---|
| invocation workload | Azure Functions Trace |
| burst timing | Azure Functions Trace |
| service dependency prior | Alibaba call graph |
| node resource prior | Alibaba node table |
| placement affinity prior | Alibaba MS resource table |
| edge collaboration surface | synthetic topology |

중요한 점:

```text
Alibaba trace를 FaaS workload로 직접 쓰지 않는다.
Azure workload에 graph/resource context를 부여하는 prior로 사용한다.
```

## Slide 4. 추가 구현 파일

| 파일 | 역할 |
|---|---|
| scripts/download_alibaba_microservices.sh | Alibaba shard 다운로드/해제 |
| code/alibaba_microservices_week3.py | graph/resource/topology prior 생성 |
| code/alibaba_microservices_streaming.py | 전체 shard streaming ingest |
| scripts/run_alibaba_streaming_full.sh | 다운로드, ingest, 삭제 자동화 |
| scripts/setup_faas_sim.sh | faas-sim 설치 helper |
| requirements-faas-sim-modern.txt | 최신 Python 호환 의존성 |

구현 방향:

- 작은 shard로 먼저 smoke test
- 전체 데이터는 streaming 방식으로 처리
- 원자료는 처리 후 삭제하여 local storage 부담 완화

## Slide 5. Streaming 처리 방식

전체 Alibaba call graph/resource 데이터는 매우 크기 때문에 shard streaming 방식으로 처리했다.

처리 흐름:

```text
download archive
-> extract CSV
-> ingest aggregate DB
-> delete archive
-> delete CSV
-> next shard
```

장점:

- 전체 원자료를 로컬에 계속 보관하지 않아도 된다.
- 실패 시 SQLite 상태를 이용해 재개할 수 있다.
- NAS 또는 외장 저장소와 결합하기 쉽다.

## Slide 6. 전체 처리 규모

전체 streaming 결과:

| 항목 | 결과 |
|---|---:|
| Node shards | 1 |
| MSResource shards | 12 |
| MSCallGraph shards | 145 |
| total ingested sources | 158 |
| call graph rows | 996,409,549 |
| valid call graph rows | 848,641,265 |
| MS resource rows | 138,619,760 |
| node resource rows | 18,868,400 |

생성된 주요 산출물:

| 파일 | 내용 |
|---|---|
| graph_edges.csv | service dependency edge prior |
| service_graph_scores.csv | service-level graph score |
| node_resource_summary.csv | node resource prior |
| ms_resource_summary.csv | microservice demand prior |
| ms_node_affinity.csv | service-node affinity |
| placement_candidates.csv | service별 placement 후보 |
| edge_topology_nodes.csv | synthetic edge nodes |
| edge_topology_links.csv | synthetic topology links |

## Slide 7. Graph Prior 정의

Alibaba call graph에서 upstream service와 downstream service 간 edge를 구성했다.

```text
edge = (upstream service, downstream service, rpc type)
call_count = observed calls on edge
rt_abs_mean_ms = mean absolute response time
graph_score = call_weight * (1 + rt_weight)
```

의미:

- call_count가 큰 edge는 함께 배치하거나 가까운 node에 둘 가치가 높다.
- response time이 큰 edge는 network latency가 추가될 때 tail latency 위험이 크다.
- graph_score는 collaborative prewarming과 placement의 중요도 prior로 사용된다.

## Slide 8. Graph Prior 결과

전체 streaming 기준 graph prior:

| 지표 | 값 |
|---|---:|
| graph edges after filter | 52,641 |
| upstream services | 3,494 |
| downstream services | 15,936 |
| edge call count p50 | 50 |
| edge call count p95 | 14,985 |
| edge call count p99 | 203,587.4 |
| max edge call count | 38,705,878 |
| edge mean RT p95 | 43.78 ms |
| edge mean RT p99 | 256.94 ms |
| max edge mean RT | 38,242.5 ms |

해석:

- service dependency는 heavy-tail 구조를 가진다.
- 일부 edge는 호출량이 매우 크고 response time 위험도 크다.
- graph-aware placement가 효과를 낼 여지가 있다.

## Slide 9. Resource / Placement Prior 정의

Node prior:

```text
available_cpu_score = 1 - avg_cpu_utilization
available_memory_score = 1 - avg_memory_utilization
available_score = 0.5 * cpu_score + 0.5 * memory_score
```

Service-node affinity:

```text
observation_share = observations(ms, node) / observations(ms)
affinity_score = observation_share * (1 + node_available_score)
```

의미:

- resource 여유가 큰 node를 placement 후보로 선호한다.
- 특정 service가 자주 관측된 node를 affinity가 높은 후보로 본다.
- 이후 placement-aware warming 정책의 입력으로 사용한다.

## Slide 10. Resource Prior 결과

전체 streaming 기준 resource/placement prior:

| 지표 | 값 |
|---|---:|
| node count | 13,116 |
| microservices with resource summary | 1,303 |
| MS-node affinity pairs | 93,540 |
| node available score p50 | 0.2968 |
| node available score p95 | 0.3848 |
| node available score p99 | 0.7755 |
| service node count p50 | 22 |
| service node count p95 | 314.9 |
| service node count p99 | 676.7 |
| resource demand score p95 | 0.5157 |

해석:

- node별 resource 여유도가 다르다.
- service별로 관측된 node 후보가 넓게 분포한다.
- 단순 random placement보다 resource/affinity 기반 placement가 더 타당하다.

## Slide 11. Synthetic Edge Topology

Azure trace에는 edge topology가 없으므로, Alibaba node summary에서 여유 자원이 큰 node를 edge node 후보로 선택했다.

| 항목 | 값 |
|---|---:|
| synthetic edge nodes | 32 |
| topology links | 64 |
| topology 형태 | deterministic ring + chord |
| 목적 | neighbor sharing, hop latency sensitivity 평가 |

중요한 해석:

```text
이 topology는 실제 edge topology라고 주장하지 않는다.
controlled experiment surface로 사용한다.
```

즉, topology density와 hop latency를 바꿔가며 collaborative prewarming의 민감도를 평가하기 위한 실험 표면이다.

## Slide 12. faas-sim 환경 보완

초기 문제:

- faas-sim upstream requirements는 오래된 dependency를 강하게 pinning한다.
- Python 3.14 환경에서 scipy/numpy 구버전 build가 실패했다.

보완:

| 기존 | 변경 |
|---|---|
| upstream requirements 그대로 설치 | 최신 호환 dependency 사용 |
| pip install -e . | pip install --no-deps -e . |
| 구버전 scipy/numpy 강제 | Python 3.14 호환 package 사용 |

검증 결과:

```text
faas-sim import: OK
Simulation class import: OK
Topology class import: OK
```

## Slide 13. 3-4주차 결론

3-4주차에서 완료한 것:

- Alibaba 전체 call graph/resource shard를 streaming 방식으로 처리했다.
- service dependency edge prior를 생성했다.
- node resource state와 microservice resource demand prior를 생성했다.
- service-node affinity와 placement candidates를 생성했다.
- synthetic edge topology를 생성했다.
- faas-sim 최신 Python 호환 실행 환경을 준비했다.

논문 관점의 의미:

```text
Azure workload만으로는 부족했던 graph/resource/topology 정보를
Alibaba trace 기반 prior로 보완했다.
```

## Slide 14. 5-6주차로 이어지는 실험

다음 단계에서 비교할 정책:

| policy | 의미 |
|---|---|
| local-only predictive | 각 node가 자기 workload만 보고 prewarming |
| neighbor-aware warming | 인접 edge node의 warm pool 공유 |
| graph-aware warming | service dependency를 고려해 연관 service까지 prewarming |
| graph + resource placement | graph score와 node resource score를 함께 고려 |

평가 지표:

- cold-start rate
- average latency
- p95/p99 latency
- resource cost
- cross-edge redirection overhead
- SLO violation rate

최종 목표:

```text
z2-trigger baseline 대비 graph-aware collaborative prewarming이
tail latency와 cold-start rate를 추가로 줄일 수 있는지 검증한다.
```

