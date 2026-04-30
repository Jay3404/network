# Week 2. Alibaba Microservices Trace 기반 Graph Prior 및 Simulator 준비

## 핵심 요약

Week 2의 목적은 Week 1에서 부족했던 두 부분을 보완하는 것이다.

1. Week 1의 local predictive baseline은 각 app의 local history만 사용했다. 따라서 function/service 간 의존성이 있는 workload에서는 downstream burst를 사전에 반영하지 못한다.
2. Week 1의 capacity, latency, resource cost는 단순 simulation assumption이었다. 따라서 Week 2부터는 Alibaba microservices trace의 call graph와 resource table을 이용해 graph-aware prewarming으로 확장할 기반을 만든다.

Week 2에서 준비할 실험 축은 다음 세 가지다.

| 작업 | 목적 | 산출물 |
|---|---|---|
| Alibaba call graph / resource table 정리 | service dependency와 resource profile 확보 | `callgraph_edge_summary.csv`, `service_resource_summary.csv`, `service_rtqps_summary.csv` |
| graph prior 정의 | upstream burst가 downstream warm 필요성으로 전파되는 기준 정의 | `graph_prior_edges.csv`, `graph_prior_services.csv` |
| faas-sim 또는 EdgeCloudSim 실행 환경 구축 | trace-driven graph-aware policy를 simulator로 검증할 준비 | setup scripts, simulator 선택 기준 |

Week 2의 방향은 다음 한 문장으로 정리할 수 있다.

> Week 1의 강한 local-only baseline인 `local_mean_std_60_z1`을 기준으로 두고, Week 2에서는 Alibaba microservices call graph를 이용해 upstream service의 burst signal을 downstream service prewarming prior로 전파하는 graph-aware baseline을 설계한다.

## 1. Week 1에서 보완할 점

Week 1의 결론은 `local_mean_std_60_z1`이 매우 강한 local-only baseline이라는 것이다. 하지만 다음 한계가 남아 있다.

| 한계 | 설명 | Week 2 보완 |
|---|---|---|
| local-only 예측 | 각 app/service 자신의 과거 호출량만 사용 | call graph 기반 neighbor signal 추가 |
| capacity 가정 | `capacity=20 invocations/min`은 Azure 공식값이 아니라 보수적 proxy | Alibaba resource/RT/QPS 기반 sensitivity 설계 |
| latency 가정 | `execution_ms=100`, `cold_start_penalty=800`은 lightweight function scenario | trace 기반 RT table과 simulator에서 latency 분리 |
| function dependency 미반영 | upstream burst가 downstream 호출 증가로 이어지는 구조를 반영하지 못함 | `P(downstream | upstream)` graph prior 정의 |
| simulator 없음 | Week 1은 trace replay 계산에 가까움 | faas-sim 또는 EdgeCloudSim 환경 구축 |

## 2. Alibaba Microservices Trace 2021

공식 Alibaba `cluster-trace-microservices-v2021` README 기준으로 trace는 12시간 동안 수집된 microservices runtime trace다. 주요 테이블은 네 가지다.

| Table | 의미 | Week 2 사용 |
|---|---|---|
| `Node` | bare-metal node CPU/memory utilization | edge/cloud node capacity 참고 |
| `MSResource` | microservice container별 CPU/memory utilization | service별 warm cost/resource profile |
| `MSRTQps` | microservice call rate와 response time | workload intensity와 service RT |
| `MSCallGraph` | upstream/downstream microservice call graph | dependency graph, graph prior |

공식 README의 데이터 규모는 대략 다음과 같다.

| Directory | 크기 |
|---|---:|
| `Node` | 약 1.1 GiB |
| `MSCallGraph` | 약 25 GiB |
| `MSResource` | 약 16 GiB |
| `MSRTQps` | 약 19 GiB |

따라서 전체 다운로드와 전처리는 시간이 오래 걸린다. Week 2에서는 먼저 chunk-based preprocessing script를 만들고, 필요하면 일부 파일로 smoke test를 수행한 뒤 전체 처리로 확장한다.

현재 로컬 상태:

```text
data/alibaba_clusterdata/raw/microservices_v2021/
  MSCallGraph/  # directory exists, CSV not downloaded yet
  MSResource/   # directory exists, CSV not downloaded yet
  MSRTQps/      # directory exists, CSV not downloaded yet
  Node/         # directory exists, CSV not downloaded yet
```

## 3. 테이블 스키마 정리

### 3.1 MSCallGraph

| Column | 의미 |
|---|---|
| `timestamp` | call record timestamp |
| `traceid` | call graph identifier |
| `rpcid` | call identifier within trace |
| `um` | upstream microservice |
| `rpctype` | communication type, e.g. RPC/HTTP/MQ/DB/MC |
| `interface` | called interface |
| `dm` | downstream microservice |
| `rt` | response time in ms |

Week 2에서 먼저 만드는 edge summary:

```text
edge = (um, dm, rpctype)
call_count = count(edge)
avg_abs_rt_ms = mean(abs(rt))
p_downstream_given_upstream = call_count(um -> dm) / total_outgoing_calls(um)
```

`rt`는 RPC/HTTP에서 upstream/downstream 관점에 따라 양수/음수로 기록될 수 있으므로, 초기 prior에서는 `abs(rt)`를 사용한다.

### 3.2 MSResource

| Column | 의미 |
|---|---|
| `timestamp` | resource timestamp |
| `msname` | microservice name |
| `msinstanceid` | container instance id |
| `nodeid` | hosting node id |
| `cpu_utilization` | service instance CPU utilization |
| `memory_utilization` | service instance memory utilization |

Week 2 resource summary:

```text
service_resource_summary:
  msname
  sample_count
  instance_count
  node_count
  avg_cpu_utilization
  avg_memory_utilization
```

### 3.3 MSRTQps

| Column | 의미 |
|---|---|
| `timestamp` | metric timestamp |
| `msname` | microservice name |
| `msinstanceid` | container instance id |
| `metrics` | MCR/RT metric name |
| `value` | metric value |

공식 README에 따르면 `metrics`는 다음 계열을 포함한다.

```text
consumerRPC_MCR, providerRPC_MCR, HTTP_MCR,
providerMQ_MCR, consumerMQ_MCR,
consumerRPC_RT, providerRPC_RT, HTTP_RT,
providerMQ_RT, consumerMQ_RT
```

여기서 `MCR`은 call rate, `RT`는 response time이다.

## 4. Graph Prior 정의

Week 2의 graph prior는 복잡한 GNN 모델이 아니라, call graph에서 바로 계산 가능한 lightweight prior로 시작한다.

기본 edge prior:

```text
P(dm | um) =
  call_count(um -> dm)
  ---------------------
  total_outgoing_calls(um)
```

latency-aware edge weight:

```text
latency_decay =
  1 / (1 + avg_abs_rt_ms / rt_scale_ms)

graph_prior_weight =
  P(dm | um) * latency_decay
```

기본값:

| Parameter | 값 | 이유 |
|---|---:|---|
| `rt_scale_ms` | 100 ms | Week 1의 warm execution latency와 맞춘 초기 scale |
| edge direction | `um -> dm` | upstream burst가 downstream demand로 이어지는 방향 |
| edge probability | outgoing-normalized call count | service별 outgoing volume 차이를 제거 |
| latency transform | `1 / (1 + rt / scale)` | RT가 긴 edge보다 가까운 downstream edge를 우선 |

이 prior는 다음처럼 prewarming score로 확장할 수 있다.

```text
graph_warm_score(dm, t)
  = local_score(dm, t)
    + alpha * sum_{um in Parents(dm)}
        graph_prior_weight(um, dm) * burst_score(um, t - lag)
```

Week 2에서는 먼저 `graph_prior_edges.csv`를 만들고, 이후 Azure/Alibaba workload replay나 simulator에서 `alpha`, `lag`, `capacity` sensitivity를 돌린다.

## 5. 구현된 스크립트

### 5.1 Alibaba 다운로드 helper

```bash
bash scripts/preprocess/download_alibaba_microservices_2021.sh --manifest-only
```

용도:

- 공식 OSS archive URL manifest 출력
- `Node`, `MSResource`, `MSRTQps`, `MSCallGraph` 부분 다운로드 지원
- 전체 다운로드가 크므로 `--resource-only`, `--rtqps-limit`, `--callgraph-limit`로 smoke test 가능

예시:

```bash
# resource table만 다운로드
bash scripts/preprocess/download_alibaba_microservices_2021.sh --resource-only --extract

# call graph/RT-QPS/resource를 파일 1개씩만 smoke test
bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --callgraph-only --callgraph-limit 1 --extract

bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --rtqps-only --rtqps-limit 1 --extract

bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --resource-only --resource-limit 1 --extract
```

### 5.2 Alibaba Week2 전처리

```bash
python code/alibaba_trace_week2.py --mode inspect
```

출력:

```text
outputs/week2/alibaba_microservices_2021/dataset_manifest.csv
```

데이터 CSV가 준비된 뒤:

```bash
python code/alibaba_trace_week2.py \
  --mode all \
  --chunksize 500000 \
  --rt-scale-ms 100
```

출력 예정:

| Output | 의미 |
|---|---|
| `dataset_manifest.csv` | 로컬 Alibaba 파일 현황 |
| `callgraph_edge_summary.csv` | `um -> dm` edge별 call count, average RT, transition probability |
| `callgraph_service_summary.csv` | service별 incoming/outgoing call count |
| `service_resource_summary.csv` | service별 instance/node/resource utilization |
| `service_rtqps_summary.csv` | service별 MCR/RT metric 평균 |
| `graph_prior_edges.csv` | graph-aware prewarming prior edge |
| `graph_prior_services.csv` | service centrality/resource summary |

## 6. Simulator 선택

### 6.1 faas-sim

Week 2의 1순위 simulator는 `faas-sim`이다.

선정 이유:

| 이유 | 설명 |
|---|---|
| serverless/FaaS 전용 | scheduling, autoscaling, load balancing 정책 평가에 맞음 |
| trace-driven | 실제 trace를 request generator나 function profile로 연결하기 좋음 |
| Python/SimPy 기반 | Week 1/2 Python preprocessing 결과와 연결이 쉬움 |
| function lifecycle abstraction | deploy/startup/invoke/teardown 단계를 모델링 가능 |

setup script:

```bash
bash scripts/sim/setup_faas_sim.sh
```

첫 실행은 `tools/faas-sim` clone과 `.venv/faas-sim` 생성을 수행하므로 network access가 필요하다.

### 6.2 EdgeCloudSim

EdgeCloudSim은 edge computing system simulation에는 강하지만, serverless function lifecycle 자체는 faas-sim보다 직접적이지 않다.

사용 가능성:

| 장점 | 한계 |
|---|---|
| edge/cloud node, network, mobility modeling에 강함 | FaaS cold start/prewarming model은 직접 추가해야 함 |
| Java/CloudSim 기반으로 성숙한 edge simulator | Python preprocessing과 연결하려면 CSV bridge 필요 |
| edge orchestrator 확장 가능 | Java 21+ 및 IDE 기반 실행 흐름이 필요 |

setup script:

```bash
bash scripts/sim/setup_edgecloudsim.sh
```

Week 2에서는 faas-sim을 primary simulator로 두고, EdgeCloudSim은 edge topology/network simulation이 중요해지는 경우 secondary option으로 둔다.

## 7. Week 2 실험 계획

### Step 1. Alibaba manifest 확인

```bash
python code/alibaba_trace_week2.py --mode inspect
```

목표:

- raw directory 구조 확인
- CSV/archive 개수 확인
- 다운로드가 필요한 테이블 확인

### Step 2. 부분 다운로드 smoke test

전체 데이터는 크므로 먼저 각 테이블 1개 archive만 내려받아 처리한다.

```bash
bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --callgraph-only --callgraph-limit 1 --extract

bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --resource-only --resource-limit 1 --extract

bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --rtqps-only --rtqps-limit 1 --extract
```

### Step 3. chunk-based summary 생성

```bash
python code/alibaba_trace_week2.py \
  --mode all \
  --max-files 1 \
  --chunksize 500000
```

목표:

- schema detection 정상 동작 확인
- edge/resource/RT-QPS summary 생성 확인
- graph prior edge weight sanity check

### Step 4. full processing

부분 처리 결과가 정상일 때 전체 파일로 확장한다.

```bash
python code/alibaba_trace_week2.py \
  --mode all \
  --chunksize 500000 \
  --rt-scale-ms 100
```

### Step 5. simulator 연결

faas-sim에 연결할 최소 입력은 다음과 같다.

| Input | 출처 |
|---|---|
| service/function list | `graph_prior_services.csv` |
| dependency edges | `graph_prior_edges.csv` |
| request arrival pattern | `service_rtqps_summary.csv` 또는 MSRTQps time series |
| execution/RT profile | `MSRTQps` RT metrics 또는 `MSCallGraph` RT |
| resource profile | `service_resource_summary.csv` |

## 8. 이번 주의 성공 기준

Week 2가 끝났다고 볼 수 있는 최소 기준:

1. Alibaba microservices trace의 local manifest가 정리되어 있다.
2. 적어도 sample file 기준으로 `callgraph_edge_summary.csv`, `service_resource_summary.csv`, `graph_prior_edges.csv`가 생성된다.
3. graph prior 수식과 output schema가 문서화되어 있다.
4. faas-sim 또는 EdgeCloudSim 중 하나의 실행 환경 setup path가 정해져 있다.
5. Week 3에서 graph-aware prewarming policy를 구현할 수 있는 입력 파일 형식이 확정되어 있다.

## 참고 링크

- Alibaba clusterdata: https://github.com/alibaba/clusterdata
- Alibaba microservices v2021 README: https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021
- faas-sim docs: https://edgerun.github.io/faas-sim/
- faas-sim GitHub: https://github.com/edgerun/faas-sim
- EdgeCloudSim GitHub: https://github.com/CagataySonmez/EdgeCloudSim
