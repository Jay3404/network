# Week 1. Azure Functions Trace 기반 App-level Burst 분석 및 Warm Policy Baseline

## 핵심 요약

Week 1의 목적은 Azure Functions Trace 2019를 이용해 application-level workload를 구성하고, cold start 완화를 위한 baseline warm policy를 재현 가능한 형태로 정리하는 것이다.

이번 주차의 최종 실험 설정은 다음과 같다.

| 항목 | 최종 설정 |
|---|---|
| 데이터 | Azure Functions Trace 2019, 14일, minute-level invocation |
| 분석 단위 | application, `HashApp` |
| main subset | 전체 invocation의 99% 이상을 커버하는 최소 app 집합 |
| selected apps | 2,278 apps |
| achieved coverage | 99.0006% |
| burst 기준 | 최근 60분 rolling mean + 3σ |
| warm policy baseline | reactive, static warm, local predictive |
| 추가 forecast baseline | TCN, LSTM, LightGBM rolling forecast |
| ML 평가 방식 | chronological 70/30 holdout + expanding-window rolling-origin |

핵심 결과는 다음과 같다.

| 질문 | 결론 |
|---|---|
| 99% coverage subset이 맞는가? | 맞다. 24,274개 app 중 2,278개가 전체 invocation의 99.0006%를 설명한다. |
| app-level workload는 bursty한가? | 맞다. 60분 window, 3σ 기준 burst ratio가 1.245%로 정규분포 기대치보다 훨씬 높다. |
| 단순 static warm은 충분한가? | 충분하지 않다. static-1은 cold served ratio가 96.421%이고, static-1000도 26.564% 수준이다. |
| Local Predictive 기본 MA60은 어떤가? | cold served ratio 9.860%, weighted latency 178.88 ms로 static보다 훨씬 효율적이다. |
| 더 강한 local-only baseline은? | `local_mean_std_60_z1`이 성능/비용 균형이 가장 좋다. full 14-day sweep에서 cold served ratio 4.138%, weighted latency 133.11 ms다. |
| ML rolling forecast는 local baseline을 이겼는가? | 현재 설정에서는 아니다. 70/30 및 rolling-origin 평가 모두 `local_mean_std_60_z1`이 가장 안정적이다. |
| ML 중 가장 현실적인 baseline은? | LightGBM이다. TCN/LSTM보다 안정적이고, rolling-origin에서 cold served ratio 4.794%, weighted latency 138.35 ms다. |

Word 개요서에는 Local Predictive의 구체 수식이 명시되어 있지는 않다. 대신 초기 Burst Predictor를 TCN, LSTM, LightGBM 기반 rolling forecast로 시작한다고 되어 있다. 따라서 본 문서에서는 Local Predictive를 “각 app의 local history만 이용해 다음 minute demand를 예측하는 baseline 계열”로 정의하고, 가장 단순한 MA60부터 EWMA, mean+std, p95, TCN/LSTM/LightGBM까지 확장해 비교했다.

Week 1의 최종 baseline 추천은 다음과 같다.

| 용도 | 추천 baseline |
|---|---|
| 기본 local-only 비교군 | `local_mean_std_60_z1` |
| cost를 거의 늘리지 않는 local 개선안 | `local_ewma_a0p3` |
| local-only 성능 상한 참고 | `local_p95_60` |
| ML rolling forecast 비교군 | `lightgbm` |
| provider-style 단순 비교군 | static warm sensitivity |

따라서 이후 제안 기법은 단순 static warm이 아니라 `local_mean_std_60_z1` 및 LightGBM rolling forecast보다 resource cost를 줄이거나 tail latency를 낮추는 방향으로 설계해야 한다.

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

| policy | cold served ratio | weighted avg latency | p95 app-minute latency | p99 app-minute latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|---:|---:|
| reactive | 100.000% | 900.00 ms | 900.00 ms | 900.00 ms | 0.00 | 0 |
| static-1 | 96.421% | 871.37 ms | 882.14 ms | 897.13 ms | 1.00 | 45,924,480 |
| static-5 | 90.042% | 820.33 ms | 810.71 ms | 885.64 ms | 5.00 | 229,622,400 |
| static-10 | 85.409% | 783.27 ms | 721.43 ms | 871.28 ms | 10.00 | 459,244,800 |
| static-20 | 79.382% | 735.06 ms | 542.86 ms | 842.56 ms | 20.00 | 918,489,600 |
| static-50 | 69.106% | 652.85 ms | 100.00 ms | 756.40 ms | 50.00 | 2,296,224,000 |
| static-100 | 60.453% | 583.62 ms | 100.00 ms | 612.80 ms | 100.00 | 4,592,448,000 |
| static-500 | 36.721% | 393.77 ms | 100.00 ms | 100.00 ms | 500.00 | 22,962,240,000 |
| static-1000 | 26.564% | 312.52 ms | 100.00 ms | 100.00 ms | 1000.00 | 45,924,480,000 |
| local predictive | 9.860% | 178.88 ms | 343.48 ms | 685.88 ms | 14.11 | 648,016,253 |

해석:

- reactive는 모든 요청이 cold start를 겪는다.
- static warm을 크게 늘리면 cold served ratio와 latency가 줄어든다.
- 하지만 static-1000도 cold served ratio가 26.6% 남고, resource cost가 매우 크다.
- local predictive는 avg warm instance가 14.11개 수준인데도 cold served ratio를 9.86%까지 낮춘다.
- local predictive의 total resource cost는 static-20보다 낮고, 성능은 static-1000보다 좋다.

### 7.1 Local Predictive Variant Sweep

`local predictive`는 선행연구의 특정 고정 알고리즘 이름이라기보다, 각 application의 local history만 이용해 다음 workload를 예측하는 baseline 범주로 보는 것이 정확하다.

따라서 단순 moving average 외에 몇 가지 local-only predictor를 추가로 비교했다.

실행 명령:

```bash
python code/azure_trace_week1.py --mode predictive_sweep_2019 --coverage-threshold 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
```

비교한 predictor:

| predictor | 의미 |
|---|---|
| `local_ma_15` | 직전 15분 평균 |
| `local_ma_60` | 직전 60분 평균, 기존 baseline |
| `local_ma_180` | 직전 180분 평균 |
| `local_ewma_a0p3` | 최근 값에 더 큰 가중치를 두는 EWMA |
| `local_mean_std_60_z1` | 직전 60분 평균 + 1σ safety buffer |
| `local_p95_60` | 직전 60분 95th percentile |
| `local_max_ma_5_60` | 직전 5분 평균과 60분 평균 중 큰 값 |
| `local_max_ma_60_seasonal_1440` | 60분 평균과 전일 동일 minute 값 중 큰 값 |

결과:

| predictor | cold served ratio | weighted avg latency | p95 app-minute latency | p99 app-minute latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|---:|---:|
| `local_ma_15` | 8.407% | 167.26 ms | 307.41 ms | 670.33 ms | 14.12 | 648,534,448 |
| `local_ma_60` | 9.860% | 178.88 ms | 343.48 ms | 685.88 ms | 14.11 | 648,016,253 |
| `local_ma_180` | 11.865% | 194.92 ms | 395.50 ms | 711.76 ms | 14.10 | 647,324,609 |
| `local_ewma_a0p3` | 7.202% | 157.62 ms | 288.63 ms | 671.43 ms | 14.13 | 648,879,478 |
| `local_mean_std_60_z1` | 4.138% | 133.11 ms | 128.11 ms | 449.30 ms | 17.73 | 814,420,820 |
| `local_p95_60` | 2.046% | 116.37 ms | 100.00 ms | 256.51 ms | 20.45 | 939,154,630 |
| `local_max_ma_5_60` | 6.008% | 148.06 ms | 235.46 ms | 628.81 ms | 15.10 | 693,414,753 |
| `local_max_ma_60_seasonal_1440` | 5.792% | 146.34 ms | 204.35 ms | 563.16 ms | 16.23 | 745,574,782 |

해석:

- 단순 moving average 중에서는 15분 window가 60분보다 낫다. Azure workload에서는 너무 긴 평균이 burst 대응을 둔하게 만든다.
- `local_ewma_a0p3`는 resource cost가 `local_ma_60`과 거의 같으면서 cold served ratio와 weighted latency를 모두 개선한다.
- `local_mean_std_60_z1`은 burst-aware safety buffer를 붙인 형태라 성능과 비용의 균형이 가장 좋다.
- `local_p95_60`은 가장 공격적인 local-only predictor이며 성능은 가장 좋지만 resource cost가 가장 높다.
- `local_max_ma_60_seasonal_1440`은 전일 동일 minute 패턴을 반영하므로 daily periodic workload에 강하다.

이번 단계의 추천 baseline:

| 용도 | 추천 |
|---|---|
| cost를 거의 늘리지 않는 개선 baseline | `local_ewma_a0p3` |
| 성능과 비용 균형 baseline | `local_mean_std_60_z1` |
| local-only 성능 상한에 가까운 baseline | `local_p95_60` |

따라서 이후 제안 기법과 비교할 강한 local-only baseline은 `local_mean_std_60_z1`을 기본으로 두고, 보조적으로 `local_ewma_a0p3`와 `local_p95_60`을 함께 제시하는 것이 적절하다.

### 7.2 TCN/LSTM/LightGBM Rolling Forecast Baseline

Word 개요서에는 Local Predictive의 구체 수식이 아니라, 초기 Burst Predictor를 `TCN, LSTM, LightGBM 기반 rolling forecast`로 시작한다고 적혀 있다. 따라서 이번 실험은 반반 split 대신 time-series 평가에서 더 자연스러운 chronological 70/30 holdout을 main으로 두고, 추가로 expanding-window rolling-origin evaluation을 수행했다.

설정:

| 항목 | 값 |
|---|---:|
| app subset | 99% invocation coverage, 2,278 apps |
| holdout train period | first 70%, 14,112 minutes = 9.8 days |
| holdout eval period | last 30%, 6,048 minutes = 4.2 days |
| rolling-origin | initial train 9.8 days, 1-day eval step, expanding train |
| rolling folds | 5 folds, 4 full-day folds + final 288-minute fold |
| input sequence | 직전 60분 |
| train samples | 300,000 sampled windows |
| TCN/LSTM epochs | 3 |
| target transform | `log1p(invocations)` |
| warm safety buffer | train residual 90th percentile |

TCN/LSTM은 직전 60분 `log1p(invocations)` sequence를 입력으로 다음 minute invocation을 예측한다. LightGBM은 lag/statistical feature를 사용한다.

LightGBM feature:

```text
last_1,
mean_5, mean_15, mean_60,
std_60, p95_60, max_60,
seasonal_lag_1440,
minute_of_day_sin, minute_of_day_cos
```

각 ML 모델은 point forecast만 쓰면 burst를 과소예측하기 쉬우므로, 학습 구간 residual의 90th percentile을 log-scale safety buffer로 더했다. 이는 평가 구간을 보지 않고 학습 구간에서만 계산한 calibration이다.

이번 실험에서 둔 주요 가정은 다음과 같다.

| 항목 | 값 | 이유 |
|---|---:|---|
| capacity per warm instance | 20 invocations/min | trace에 실제 instance capacity가 없으므로 warm capacity를 비교하기 위한 simulation proxy |
| cold start penalty | 800 ms | cold start가 latency에 주는 영향을 반영하기 위한 단순화된 penalty |
| warm execution time | 100 ms | per-invocation latency trace가 없으므로 warm path의 기본 latency를 고정 |
| resource cost | warm instance app-minute 합 | 실제 비용 데이터가 없으므로 prewarmed resource 유지량을 proxy cost로 사용 |
| minimum warm instance | 1 | predictive policy가 app별 최소 prewarm을 유지하는 보수적 가정 |

실행 명령:

```bash
pip install torch lightgbm scikit-learn

python code/azure_trace_week1.py \
  --mode ml_forecast_2019 \
  --coverage-threshold 0.99 \
  --capacity 20 \
  --cold-start-penalty 800 \
  --execution-ms 100 \
  --ml-models local_mean_std,tcn,lstm,lightgbm \
  --ml-train-ratio 0.7 \
  --ml-eval-mode both \
  --ml-train-samples 300000 \
  --ml-epochs 3 \
  --ml-residual-quantile 0.9 \
  --ml-rolling-eval-days 1 \
  --ml-rolling-step-days 1
```

Chronological 70/30 holdout 결과:

| predictor | cold served ratio | weighted avg latency | p95 app-minute latency | p99 app-minute latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|---:|---:|
| `local_mean_std_60_z1` | 3.893% | 131.14 ms | 124.10 ms | 442.86 ms | 17.85 | 245,989,082 |
| `tcn` | 33.115% | 364.92 ms | 297.85 ms | 659.40 ms | 10.40 | 143,250,342 |
| `lstm` | 8.878% | 171.02 ms | 233.33 ms | 766.67 ms | 17.80 | 245,194,255 |
| `lightgbm` | 4.766% | 138.13 ms | 150.00 ms | 660.30 ms | 19.43 | 267,656,469 |

Expanding-window rolling-origin 결과:

| predictor | cold served ratio | weighted avg latency | p95 app-minute latency | p99 app-minute latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|---:|---:|
| `local_mean_std_60_z1` | 3.893% | 131.14 ms | 124.10 ms | 442.86 ms | 17.85 | 245,989,082 |
| `tcn` | 12.591% | 200.73 ms | 196.48 ms | 644.68 ms | 36.77 | 506,547,337 |
| `lstm` | 9.270% | 174.16 ms | 233.33 ms | 766.67 ms | 17.43 | 240,118,791 |
| `lightgbm` | 4.794% | 138.35 ms | 153.89 ms | 662.46 ms | 19.29 | 265,803,325 |

해석:

- `local_mean_std_60_z1`은 단순하지만 매우 강한 baseline이다. cold served ratio와 weighted latency 기준으로 ML baseline들과 비교해도 가장 균형이 좋다.
- `tcn`은 single holdout에서는 final 30% 구간을 under-prewarm해 cold served ratio가 크게 높아진다. rolling-origin으로 매 fold 재학습하면 성능은 개선되지만, resource cost가 크게 증가한다.
- `lightgbm`은 holdout과 rolling-origin 모두에서 가장 안정적인 ML baseline이다. 다만 현재 feature/학습 설정에서는 `local_mean_std_60_z1`을 넘지는 못했다.
- `lstm`은 resource cost는 낮지만 under-prewarming 경향이 강해 cold served ratio와 tail latency가 높다.
- 따라서 week-1 기준의 주 local baseline은 `local_mean_std_60_z1`으로 두고, ML 계열은 TCN/LSTM/LightGBM rolling forecast baseline으로 별도 비교하는 것이 적절하다. ML 계열 중에서는 `lightgbm`이 가장 현실적인 시작점이다.

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
8. TCN/LSTM/LightGBM rolling forecast baseline도 구현했지만, week-1 설정에서는 단순한 `local_mean_std_60_z1`이 가장 강한 성능/비용 균형을 보였다.
9. 따라서 제안 기법은 `local_mean_std_60_z1`을 강한 local-only baseline으로 두고, 그보다 resource cost를 줄이거나 tail latency를 낮추는 방향으로 설계해야 한다.

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
| capacity/cold penalty sensitivity | simulation assumption이 결론에 미치는 영향 확인 |
| residual quantile q80/q90/q95 | ML predictor의 under/over-prewarming trade-off 확인 |
| HTTP/queue/timer trigger별 app 분석 | trigger type에 따른 burst 특성 분리 |
| long-tail app subset 분석 | 99% coverage 밖 app의 cold start 특성 확인 |

재현 명령은 99% coverage subset을 직접 선택하도록 다음 형태를 사용한다.

```bash
python 01_jay/azure_trace_week1.py --mode app_2019 --coverage 0.99 --window 60 --z 3
python 01_jay/azure_trace_week1.py --mode baseline_2019 --baseline-level app --coverage 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
```

## 11. Temporal Holdout 검증

논문 실험으로 이어가려면 예측 정책이 같은 기간에서만 좋아 보이는지, 미래 구간에서도 유지되는지 확인해야 한다. 따라서 Day 1-10에서 99% coverage app subset과 정책 파라미터를 정하고, Day 11-14에서 평가했다.

실행 명령:

```bash
python 01_jay/azure_trace_week1.py \
  --mode holdout_2019 \
  --baseline-level app \
  --coverage 0.99 \
  --train-days 10 \
  --capacity 20 \
  --cold-start-penalty 800 \
  --execution-ms 100 \
  --static-warm-values 0,1,5,10,20,50,100,500,1000 \
  --prediction-window-values 30,60,120 \
  --threshold-window-values 30,60,120 \
  --threshold-z-values 2,3
```

Train 기준 선택:

| 항목 | 값 |
|---|---:|
| train days | 1-10 |
| test days | 11-14 |
| train unique apps | 23,159 |
| train 99% coverage selected apps | 2,257 |
| train actual coverage | 99.0002% |

Train에서 각 family별 best config를 고른 뒤 test에서 평가한 결과:

| policy | cold served ratio | weighted avg latency | weighted p95 latency | weighted p99 latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|---:|---:|
| z2_trigger_w30 | 6.4056% | 151.24 ms | 432.03 ms | 740.97 ms | 14.71 | 191,235,396 |
| z3_trigger_w30 | 7.2257% | 157.81 ms | 489.12 ms | 773.02 ms | 14.46 | 188,025,712 |
| local_predictive_w30 | 8.3813% | 167.05 ms | 584.15 ms | 851.02 ms | 14.19 | 184,474,709 |
| static_1000 | 30.8428% | 346.74 ms | 763.45 ms | 786.62 ms | 1000.00 | 13,000,320,000 |
| reactive | 100.0000% | 900.00 ms | 900.00 ms | 900.00 ms | 0.00 | 0 |

해석:

1. local predictive는 holdout test에서도 static_1000보다 훨씬 적은 resource cost로 더 낮은 latency와 cold served ratio를 보인다.
2. z-trigger는 단순 local predictive보다 한 단계 더 강하다. z2_trigger_w30은 local_predictive_w30 대비 cold served ratio를 8.3813%에서 6.4056%로 낮추고, weighted avg latency를 167.05 ms에서 151.24 ms로 낮췄다.
3. z2가 z3보다 test에서 더 좋다. 즉, 이 workload에서는 3σ가 너무 보수적이고, prewarming trigger로는 2σ 계열이 더 유리하다.
4. static_1000은 tail p99는 낮지만 resource cost가 압도적으로 크고 cold served ratio도 높다. 따라서 static warm은 강한 baseline이라기보다 provider-style cost-inefficient baseline으로 보는 것이 맞다.

## 12. Cold-start / Resource Sensitivity 검증

단일 cold-start penalty와 단일 capacity에서만 결론을 내면 논문 근거가 약하다. 따라서 Day 11-14 test split에서 cold-start penalty와 instance capacity를 바꿔 sensitivity를 확인했다.

실행 명령:

```bash
python 01_jay/azure_trace_week1.py \
  --mode sensitivity_2019 \
  --baseline-level app \
  --coverage 0.99 \
  --train-days 10 \
  --prediction-window 60 \
  --window 60 \
  --threshold-z-values 2,3 \
  --static-warm-values 0,1,5,10,20,50,100,500,1000 \
  --sensitivity-cold-start-penalties 200,500,800,1500 \
  --sensitivity-capacities 10,20,50 \
  --execution-ms 100
```

기본 조합인 capacity=20, cold-start penalty=800ms에서의 test 결과:

| policy | cold served ratio | weighted avg latency | avg warm instances | total resource cost |
|---|---:|---:|---:|---:|
| reactive | 100.0000% | 900.00 ms | 0.00 | 0 |
| static_1 | 96.5257% | 872.21 ms | 1.00 | 13,000,320 |
| static_20 | 79.9477% | 739.58 ms | 20.00 | 260,006,400 |
| static_100 | 61.7288% | 593.83 ms | 100.00 | 1,300,032,000 |
| static_1000 | 30.8428% | 346.74 ms | 1000.00 | 13,000,320,000 |
| local_predictive_w60 | 9.0512% | 172.41 ms | 14.18 | 184,388,853 |
| z2_trigger_w60 | 6.7648% | 154.12 ms | 14.75 | 191,798,342 |
| z3_trigger_w60 | 7.7129% | 161.70 ms | 14.50 | 188,540,622 |

local predictive는 모든 sensitivity 조합에서 best static(static_1000)보다 낮은 latency와 낮은 cold served ratio를 보였다.

| capacity | cold penalty | local cold ratio | best static cold ratio | local weighted avg | best static weighted avg | local / static resource cost |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 200 ms | 9.2564% | 39.9035% | 118.51 ms | 179.81 ms | 0.0277 |
| 10 | 500 ms | 9.2564% | 39.9035% | 146.28 ms | 299.52 ms | 0.0277 |
| 10 | 800 ms | 9.2564% | 39.9035% | 174.05 ms | 419.23 ms | 0.0277 |
| 10 | 1500 ms | 9.2564% | 39.9035% | 238.85 ms | 698.55 ms | 0.0277 |
| 20 | 200 ms | 9.0512% | 30.8428% | 118.10 ms | 161.69 ms | 0.0142 |
| 20 | 500 ms | 9.0512% | 30.8428% | 145.26 ms | 254.21 ms | 0.0142 |
| 20 | 800 ms | 9.0512% | 30.8428% | 172.41 ms | 346.74 ms | 0.0142 |
| 20 | 1500 ms | 9.0512% | 30.8428% | 235.77 ms | 562.64 ms | 0.0142 |
| 50 | 200 ms | 8.6330% | 14.9341% | 117.27 ms | 129.87 ms | 0.0062 |
| 50 | 500 ms | 8.6330% | 14.9341% | 143.17 ms | 174.67 ms | 0.0062 |
| 50 | 800 ms | 8.6330% | 14.9341% | 169.06 ms | 219.47 ms | 0.0062 |
| 50 | 1500 ms | 8.6330% | 14.9341% | 229.50 ms | 324.01 ms | 0.0062 |

z2 trigger도 모든 sensitivity 조합에서 local predictive보다 더 낮은 weighted average latency를 보였다. resource cost 증가는 약 3.66-4.14% 수준이다.

| capacity | cold penalty | local cold ratio | z2 cold ratio | local weighted avg | z2 weighted avg | z2 / local resource cost |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 200 ms | 9.2564% | 6.9522% | 118.51 ms | 113.90 ms | 1.0414 |
| 10 | 800 ms | 9.2564% | 6.9522% | 174.05 ms | 155.62 ms | 1.0414 |
| 20 | 200 ms | 9.0512% | 6.7648% | 118.10 ms | 113.53 ms | 1.0402 |
| 20 | 800 ms | 9.0512% | 6.7648% | 172.41 ms | 154.12 ms | 1.0402 |
| 50 | 200 ms | 8.6330% | 6.3914% | 117.27 ms | 112.78 ms | 1.0366 |
| 50 | 800 ms | 8.6330% | 6.3914% | 169.06 ms | 151.13 ms | 1.0366 |

## 13. 검증 후 결론

이번 추가 검증으로 기존 baseline 결과가 단일 기간/단일 파라미터에만 의존한 우연은 아니라는 점을 확인했다.

1. Day 1-10 기반으로 선택한 app subset과 정책 파라미터가 Day 11-14에서도 유지된다.
2. local predictive는 static warm budget을 크게 늘린 정책보다 resource 효율과 latency가 모두 좋다.
3. z-trigger는 local predictive보다 약간 더 많은 warm instance를 쓰지만, cold served ratio와 latency를 더 낮춘다.
4. 따라서 다음 단계의 제안 기법은 `local_predictive`만 이기는 것으로는 부족하고, `z2_trigger`를 강한 baseline으로 포함해야 한다.

논문 관점에서 가장 중요한 변화:

```text
기존 strong baseline: local_predictive
검증 후 strong baseline: local_predictive + z2_trigger
```

즉, 5-6주차 collaborative prewarming은 z2_trigger 대비 추가 이득을 보여야 논문 기여로 설득력이 있다.

## 14. 참고 선행연구 및 문서

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
