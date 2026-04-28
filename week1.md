# Week 1. Azure Functions Trace 기반 App-level Burst 분석 및 Warm Policy Baseline

## 1. 연구 설계 요약

이번 주차의 실험 단위는 **application**으로 정리한다. Azure Functions에서는 여러 function이 하나의 application 아래에 묶이고, 실제 platform의 resource allocation과 keep-alive/prewarming decision도 application 단위 해석이 가능하기 때문이다.

이번 문서의 핵심 설계는 다음과 같다.

| 항목 | 설계 |
|---|---|
| 분석 데이터 | Azure Functions Trace 2019, 14일 minute-level invocation |
| 주 분석 단위 | application |
| application 선택 기준 | 전체 invocation의 99% 이상을 커버하는 최소 app 집합 |
| 선택된 app 수 | 2,278개 |
| coverage | 99.0006% |
| burst 기본 기준 | 최근 60분 rolling mean + 3σ |
| baseline policy | reactive, static warm, local predictive |

이 기준은 임의의 top-k가 아니다. 전체 app을 호출량 기준으로 정렬한 뒤, 누적 invocation coverage가 99% 이상이 되는 최소 app 수를 선택한 것이다.

수식으로 쓰면 다음과 같다.

```text
Find the minimum K such that:

sum_{i=1}^{K} invocations(app_i)
-------------------------------- >= 0.99
sum_{j=1}^{N} invocations(app_j)

where apps are sorted by total invocation count in descending order.
```

이번 데이터에서는 다음 결과가 나왔다.

```text
전체 app 수 = 24,274
99% coverage를 만족하는 최소 app 수 = 2,278
coverage = 99.0006%
```

따라서 본 실험의 main workload subset은 **99% invocation coverage app subset**이라고 부르는 것이 가장 정확하다.

## 2. 데이터 구성

### 2.1 Azure Functions Trace 2019

Azure 2019 trace는 function별 1분 단위 invocation count를 제공한다.

Invocation count 데이터의 기본 구조는 다음과 같다.

| 컬럼 | 의미 |
|---|---|
| `HashOwner` | 익명화된 owner/subscription 식별자 |
| `HashApp` | 익명화된 application 식별자 |
| `HashFunction` | 익명화된 function 식별자 |
| `Trigger` | trigger type |
| `1` ~ `1440` | 하루 1440분의 invocation count |

확인된 전체 규모:

| 항목 | 값 |
|---|---:|
| 기간 | 14일 |
| 시간 해상도 | 1분 |
| 총 minute 수 | 20,160 |
| unique apps | 24,274 |
| unique functions | 72,359 |
| total invocations | 12,495,810,846 |

Application 구성:

| 항목 | 값 |
|---|---:|
| 평균 functions / app | 2.98 |
| median functions / app | 1 |
| p95 functions / app | 10 |
| p99 functions / app | 24 |
| max functions / app | 302 |

해석:

- 대부분 app은 소수 function만 가진다.
- 일부 app은 많은 function을 포함한다.
- app 단위 분석은 function-level noise를 줄이고, platform-level resource allocation 관점과 더 잘 맞는다.

### 2.2 Azure Functions Invocation Trace 2021

Azure 2021 trace는 2019와 달리 per-invocation event log 형태다.

컬럼 구조:

| 컬럼 | 의미 |
|---|---|
| `app` | 익명화된 app 식별자 |
| `func` | 익명화된 function 식별자 |
| `end_timestamp` | invocation 종료 시점 |
| `duration` | invocation duration |

확인된 기본 통계:

| 항목 | 값 |
|---|---:|
| invocation rows | 1,980,951 |
| unique apps | 119 |
| unique functions | 424 |
| timestamp 범위 | 약 14일 |
| duration mean | 3.348초 |
| duration p50 | 0.031초 |
| duration p95 | 14.416초 |
| duration p99 | 72.200초 |

이번 주차 baseline은 2019의 minute-level workload를 사용했다. 2021 trace는 이후 per-invocation arrival/duration 기반 latency simulation에 활용하는 것이 적절하다.

## 3. App Selection: 99% Invocation Coverage

전체 24,274개 application을 총 invocation 기준으로 내림차순 정렬하고, 누적 coverage를 계산했다.

| 기준 | app 수 | 전체 invocation coverage |
|---|---:|---:|
| top-20 apps | 20 | 57.59% |
| top-100 apps | 100 | 78.45% |
| top-500 apps | 500 | 93.77% |
| 95% coverage apps | 627 | 95.003% |
| 99% coverage apps | 2,278 | 99.0006% |
| top-5000 apps | 5,000 | 99.70% |

99% coverage subset의 의미:

| 항목 | 값 |
|---|---:|
| selected apps | 2,278 |
| 전체 app 중 비율 | 9.38% |
| selected invocations | 12,370,934,000 수준 |
| coverage | 99.0006% |
| subset 내 최소 app total invocation | 76,295 |
| subset 내 최소 app 평균 invocation/day | 5,449.64 |

해석:

- 전체 app의 약 9.4%만 선택해도 전체 invocation의 99%를 설명한다.
- long-tail app은 개수는 많지만 전체 workload volume 기여도는 작다.
- main experiment는 99% coverage subset으로 수행하고, long-tail app은 별도 characterization 대상으로 두는 것이 합리적이다.

## 4. Burst 기준

### 4.1 기본 정의

각 application의 minute-level invocation time-series를 다음과 같이 둔다.

```text
x_t = minute t의 invocation count
```

최근 `W`분 rolling window에 대해 아래 값을 계산했다.

| Metric | 정의 | 의미 |
|---|---|---|
| `rolling_mean` | 최근 W분 평균 | local baseline load |
| `rolling_std` | 최근 W분 표준편차 | local variability |
| `z_threshold` | `rolling_mean + z * rolling_std` | z-score 기반 burst threshold |
| `is_burst_z` | `x_t > z_threshold` | z 기준 burst 여부 |
| `rolling_q95` | 최근 W분 95th percentile | 최근 상위 5% 기준 |
| `rolling_q99` | 최근 W분 99th percentile | 최근 상위 1% 기준 |
| `burst_intensity` | `x_t / rolling_mean` | 최근 평균 대비 몇 배인지 |
| `cv_window` | `rolling_std / rolling_mean` | 최근 window 내 변동계수 |

기본값:

| 항목 | 값 |
|---|---:|
| rolling window | 60분 |
| main burst threshold | mean + 3σ |
| sensitivity threshold | mean + 1σ, 2σ, 3σ |

### 4.2 1σ, 2σ, 3σ의 일반적 의미

정규분포를 가정하면 empirical rule은 다음과 같다.

| 기준 | 양쪽 구간 포함 비율 | upper-tail 초과 비율 | 해석 |
|---|---:|---:|---|
| mean + 1σ | 약 68.27% 범위 안 | 약 15.87% | 높은 load warning |
| mean + 2σ | 약 95.45% 범위 안 | 약 2.28% | moderately rare event |
| mean + 3σ | 약 99.73% 범위 안 | 약 0.135% | strong anomaly/outlier |

주의:

- serverless workload는 정규분포가 아니다.
- 실제 invocation workload는 periodicity, sparsity, heavy-tail, burstiness가 강하다.
- 따라서 1σ/2σ/3σ는 이론적 확률값이라기보다 threshold 강도를 조절하는 실험 파라미터로 보는 것이 맞다.

### 4.3 선행연구에서 burst 기준은 어떻게 잡는가?

Serverless cold-start/prewarming 선행연구에서 **mean + 3σ가 표준 burst 기준**이라고 단정하기는 어렵다. 분야별로 쓰는 기준이 다르다.

| 연구 흐름 | 주로 쓰는 기준 | 해석 |
|---|---|---|
| Serverless workload characterization | invocation distribution, idle time, inter-arrival time, p95/p99 | workload의 long-tail과 burstiness 설명 |
| Azure `Serverless in the Wild` | fixed keep-alive, idle-time histogram, 1분 bin | cold start mitigation을 idle time 기반으로 다룸 |
| Predictive scaling | 최근 history 기반 forecast, time-series prediction | 미래 invocation을 직접 예측 |
| Cloud anomaly detection/control chart | mean + 3σ | 강한 anomaly/outlier 탐지 기준 |
| SLO/tail latency 연구 | p95, p99, p999 | tail behavior 평가 |

따라서 본 연구에서는 다음처럼 사용하는 것이 안전하다.

| 목적 | 권장 기준 |
|---|---|
| 강한 burst event 분석 | mean + 3σ |
| prewarming trigger 후보 | mean + 2σ |
| early warning | mean + 1σ |
| tail workload 보조 지표 | rolling q95/q99 |

즉, `3σ`는 serverless 분야의 절대 표준이라기보다 **보수적인 burst/anomaly 기준**으로 사용하고, 논문에서는 `2σ`, `3σ`, `q99`를 함께 비교하는 것이 좋다.

## 5. App-level Burst 결과

### 5.1 Sigma threshold sensitivity

99% invocation coverage app subset, 60분 rolling window 기준 결과:

| threshold | burst points | burst ratio | entity median burst ratio | entity p95 burst ratio |
|---|---:|---:|---:|---:|
| mean + 1σ | 5,134,661 | 11.181% | 12.649% | 20.915% |
| mean + 2σ | 1,619,580 | 3.527% | 3.487% | 7.820% |
| mean + 3σ | 571,694 | 1.245% | 0.928% | 3.452% |

해석:

- 1σ는 전체 app-minute의 약 11.2%를 burst로 분류한다. 너무 넓어서 강한 burst 기준으로는 느슨하다.
- 2σ는 약 3.5%를 burst로 잡는다. prewarming trigger 후보로 적절하다.
- 3σ는 약 1.25%만 burst로 잡는다. 강한 burst event 분석 기준으로 적절하다.
- 정규분포라면 3σ upper-tail은 약 0.135%인데, 실제 app workload에서는 1.245%다. 이는 Azure workload가 정규분포보다 burst-heavy하다는 근거다.

### 5.2 Rolling window sensitivity

3σ 기준에서 window를 바꾼 결과:

| rolling window | burst ratio | entity median burst ratio | entity p95 burst ratio |
|---|---:|---:|---:|
| 30분 | 1.195% | 0.754% | 3.617% |
| 60분 | 1.245% | 0.928% | 3.452% |
| 120분 | 1.345% | 1.096% | 3.444% |

해석:

- 30/60/120분 사이 burst ratio 차이는 크지 않다.
- 60분은 짧은 spike에 너무 민감하지 않고, hour-level 변화에는 반응하는 중간 기준이다.
- main experiment는 60분으로 두고, sensitivity로 30/120분을 제시하는 방식이 적절하다.

### 5.3 호출량 상위 app의 burst

총 invocation 기준 상위 app 5개:

| rank | total invocations | CV | z=3 burst ratio | q99 burst ratio | max intensity |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,800,297,327 | 0.113 | 0.327% | 1.935% | 1.375x |
| 2 | 1,303,897,114 | 0.046 | 1.696% | 1.662% | 1.406x |
| 3 | 908,443,266 | 0.708 | 0.491% | 2.465% | 60.000x |
| 4 | 644,038,364 | 0.110 | 0.759% | 3.165% | 1.544x |
| 5 | 251,016,433 | 0.421 | 0.838% | 2.049% | 17.868x |

해석:

- 호출량이 큰 app이라고 항상 bursty한 것은 아니다.
- 3위 app은 호출량도 크고 max burst intensity가 60x로 매우 크다.
- high-volume app과 bursty app은 구분해서 분석해야 한다.

### 5.4 Burstiness 상위 app

CV 기준 상위 app 5개:

| rank | total invocations | CV | z=3 burst ratio | q99 burst ratio | max intensity |
|---:|---:|---:|---:|---:|---:|
| 1 | 169,176 | 63.620 | 0.258% | 0.164% | 60.000x |
| 2 | 81,623 | 54.615 | 0.278% | 0.387% | 60.000x |
| 3 | 481,098 | 51.285 | 0.040% | 0.030% | 60.000x |
| 4 | 109,830 | 43.981 | 0.079% | 0.129% | 60.000x |
| 5 | 214,756 | 43.942 | 0.119% | 0.099% | 60.000x |

해석:

- CV가 매우 큰 app들은 대부분 sparse burst pattern을 가진다.
- 전체 invocation volume은 크지 않지만 특정 순간에 매우 큰 spike가 발생한다.
- 이들은 burst-aware policy의 edge case를 보여주는 데 유용하다.
- 반면 main latency/cost 평가는 99% coverage subset 전체를 기준으로 보는 것이 더 안정적이다.

## 6. Baseline Warm Policy 구현

이번 baseline은 application-level workload 기준으로 구현했다.

각 application-minute에 대해 invocation count를 보고, warm capacity와 cold served request를 계산한다.

공통 모델:

```text
warm_capacity = warm_instances * capacity_per_instance
warm_served = min(invocations, warm_capacity)
cold_served = max(0, invocations - warm_capacity)
cold_start_rate = cold_served / invocations

avg_latency_ms =
  (warm_served * execution_ms
   + cold_served * (execution_ms + cold_start_penalty_ms))
  / invocations

resource_cost = warm_instances
```

기본 parameter:

| parameter | 값 |
|---|---:|
| capacity per instance | 20 invocations/min |
| warm execution latency | 100 ms |
| cold start penalty | 800 ms |

이번 문서에서는 latency 해석 시 `weighted_avg_latency_ms`를 중요하게 본다. 이는 app-minute 평균이 아니라 invocation 수로 가중한 request-level 평균 latency다.

### 6.1 Reactive

정책:

```text
warm_instances = 0
```

의미:

- 사전 warm instance가 없다.
- 모든 요청은 cold served로 계산된다.
- resource cost는 0이지만 latency는 최악이다.

### 6.2 Static Warm

정책:

```text
warm_instances = fixed N
```

실험한 값:

```text
N = 0, 1, 5, 10, 20, 50, 100, 500, 1000
```

의미:

- 각 application에 항상 N개의 warm instance를 유지한다.
- Azure Premium의 always-ready instance, AWS Lambda Provisioned Concurrency, Google Cloud Functions min instances와 유사한 provider-style baseline으로 해석할 수 있다.
- workload 변화에 반응하지 않기 때문에 burst 대응 효율은 낮을 수 있다.

### 6.3 Local Predictive

정책:

```text
predicted_invocations = shift(1).rolling(60).mean()
warm_instances = ceil(predicted_invocations / capacity_per_instance)
warm_instances = max(warm_instances, 1)
```

의미:

- 직전 60분 평균 invocation을 이용해 다음 minute의 workload를 예측한다.
- 복잡한 ML 모델이 아니라 lightweight history-based forecasting이다.
- 선행연구의 idle-time/history 기반 prewarming 및 predictive scaling baseline과 연결할 수 있다.

## 7. App-level Baseline 결과

99% invocation coverage app subset 기준 결과:

| policy | cold served ratio | weighted avg latency | weighted p95 latency | weighted p99 latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|---:|---:|
| reactive | 100.000% | 900.00 ms | 900.00 ms | 900.00 ms | 0.00 | 0 |
| static-1 | 96.421% | 871.37 ms | 899.84 ms | 899.87 ms | 1.00 | 45,924,480 |
| static-5 | 90.042% | 820.33 ms | 899.21 ms | 899.37 ms | 5.00 | 229,622,400 |
| static-10 | 85.409% | 783.27 ms | 898.42 ms | 898.74 ms | 10.00 | 459,244,800 |
| static-20 | 79.382% | 735.06 ms | 896.84 ms | 897.48 ms | 20.00 | 918,489,600 |
| static-50 | 69.106% | 652.85 ms | 892.10 ms | 893.69 ms | 50.00 | 2,296,224,000 |
| static-100 | 60.453% | 583.62 ms | 884.20 ms | 887.38 ms | 100.00 | 4,592,448,000 |
| static-500 | 36.721% | 393.77 ms | 820.99 ms | 836.92 ms | 500.00 | 22,962,240,000 |
| static-1000 | 26.564% | 312.52 ms | 741.97 ms | 773.84 ms | 1000.00 | 45,924,480,000 |
| local predictive | 9.860% | 178.88 ms | 637.39 ms | 842.04 ms | 14.11 | 648,016,253 |

해석:

- reactive는 모든 요청이 cold start를 겪는다.
- static warm을 크게 늘리면 cold served ratio와 latency가 줄어든다.
- 하지만 static-1000도 cold served ratio가 26.6% 남고, resource cost가 매우 크다.
- local predictive는 avg warm instance가 14.11개 수준인데도 cold served ratio를 9.86%까지 낮춘다.
- local predictive의 total resource cost는 static-20보다 낮고, 성능은 static-1000보다 좋다.

핵심 비교:

| 비교 | 결론 |
|---|---|
| static-1 vs reactive | static-1은 거의 충분하지 않다 |
| static-1000 vs local predictive | local predictive가 훨씬 적은 warm instance로 더 낮은 cold served ratio를 달성 |
| static warm 전체 | 단순히 많이 켜두는 방식은 cost 효율이 낮다 |
| local predictive | workload-aware baseline으로 매우 강한 비교 대상 |

## 8. 연구 관점 결론

이번 app-level 실험에서 얻은 결론은 다음과 같다.

1. 전체 24,274개 app 중 2,278개 app이 전체 invocation의 99.0006%를 담당한다.
2. 따라서 main experiment를 99% invocation coverage app subset으로 두는 것은 임의의 top-k가 아니라 coverage-based selection이다.
3. app-level workload에서도 3σ burst ratio는 1.245%로, 정규분포 이론값보다 훨씬 높다.
4. 1σ는 너무 느슨하고, 3σ는 강한 burst 기준으로 적합하다.
5. prewarming trigger 후보로는 2σ도 함께 실험할 가치가 있다.
6. static warm policy는 provider-style baseline으로 필요하지만, high-volume workload에서는 cost 효율이 낮다.
7. local predictive는 static-1000보다 훨씬 낮은 resource cost로 더 좋은 cold start/latency 결과를 보인다.
8. 따라서 제안 기법은 local predictive를 강한 baseline으로 두고, 그보다 resource cost를 줄이거나 tail latency를 낮추는 방향으로 설계해야 한다.

## 9. 교수님께 설명할 연구 설계 문장

다음 문장으로 설명하면 가장 명확하다.

> 본 연구는 Azure Functions 2019 trace에서 application 단위 workload를 구성하고, 전체 invocation의 99% 이상을 설명하는 최소 application 집합을 main evaluation subset으로 사용한다. 이 기준은 임의의 top-k 선택이 아니라 cumulative workload coverage 기반 selection이며, 실제 system demand의 대부분을 반영하면서 long-tail application으로 인한 계산 비용과 noise를 줄이기 위한 실험 설계다.

그리고 한계는 이렇게 같이 말하는 것이 좋다.

> 다만 low-volume long-tail application의 cold start 특성은 main subset에서 약하게 반영될 수 있으므로, 전체 trace characterization과 burstiness 상위 application 분석을 별도로 제시한다.

## 10. 다음 실험 제안

| 실험 | 목적 |
|---|---|
| z=2 vs z=3 trigger 기반 prewarming | aggressive/conservative burst trigger 비교 |
| 30/60/120분 window sensitivity | burst detector의 안정성 확인 |
| app-level histogram keep-alive baseline | Azure `Serverless in the Wild`와 더 가까운 baseline |
| day 1-10 train, day 11-14 test | predictive policy의 temporal generalization 평가 |
| HTTP/queue/timer trigger별 app 분석 | trigger type에 따른 burst 특성 분리 |
| long-tail app subset 분석 | 99% coverage 밖 app의 cold start 특성 확인 |

## 11. 참고 선행연구 및 문서

| 구분 | 내용 |
|---|---|
| Azure Public Dataset | Azure Functions 2019/2021 trace의 공식 데이터 출처 |
| Serverless in the Wild, USENIX ATC 2020 | Azure Functions production workload 분석, fixed keep-alive와 hybrid histogram policy 제안 |
| FGCS 2024 TCN cold-start management | Azure Functions Trace 2019를 목적에 맞게 필터링/가공해 predictive cold-start management에 사용 |
| Azure Functions Premium Plan | always-ready/prewarmed instance 개념 제공 |
| AWS Lambda Provisioned Concurrency | 지정한 concurrency만큼 initialized environment 유지 |
| Google Cloud Functions min instances | minimum instance 유지로 cold start 완화 |
| FaasCache, ASPLOS 2021 | keep-alive를 cache/resource trade-off 문제로 해석 |
| Empirical rule | 1σ/2σ/3σ의 일반적 통계 해석 근거 |

참고 링크:

- https://github.com/Azure/AzurePublicDataset
- https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md
- https://www.microsoft.com/en-us/research/publication/serverless-in-the-wild-characterizing-and-optimizing-the-serverless-workload-at-a-large-cloud-provider/
- https://www.microsoft.com/en-us/research/wp-content/uploads/2020/05/serverless-ATC20.pdf
- https://www.sciencedirect.com/science/article/abs/pii/S0167739X2300465X
- https://learn.microsoft.com/azure/azure-functions/functions-premium-plan
- https://aws.amazon.com/about-aws/whats-new/2019/12/aws-lambda-announces-provisioned-concurrency/
- https://cloud.google.com/blog/products/serverless/cloud-functions-supports-min-instances
- https://dl.acm.org/doi/10.1145/3445814.3446757
