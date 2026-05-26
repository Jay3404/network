# 1-2주차 발표용 정리

## Slide 1. 주제

### Azure Functions Trace 기반 Burst 분석 및 Prewarming Baseline

핵심 메시지:

- Azure Functions trace에서 serverless workload의 burst 특성을 분석했다.
- application 단위로 workload를 구성하고, 전체 invocation의 99%를 설명하는 subset을 main experiment 대상으로 정했다.
- reactive, static warm, local predictive, z-trigger baseline을 구현하고 검증했다.

## Slide 2. 연구 배경

AIoT/serverless edge 환경에서는 이벤트가 특정 시간에 몰리는 burst가 자주 발생한다.

이때 컨테이너가 미리 warm 상태가 아니면 cold start가 발생하고, 평균 latency뿐 아니라 tail latency가 크게 증가한다.

따라서 핵심 질문은 다음과 같다.

```text
어떤 application을, 언제, 얼마나 미리 warm 상태로 유지해야
latency를 줄이면서 resource waste를 줄일 수 있는가?
```

## Slide 3. 데이터셋

| 항목 | 내용 |
|---|---|
| 데이터 | Azure Functions Trace 2019 |
| 기간 | 14일 |
| 해상도 | 1분 단위 invocation count |
| 분석 단위 | application, HashApp |
| 전체 app 수 | 24,274 |
| 전체 invocation | 12,495,810,846 |

application 단위 분석을 선택한 이유:

- 실제 platform의 resource allocation과 prewarming decision은 app 단위 해석이 가능하다.
- function-level noise를 줄이고 workload pattern을 안정적으로 볼 수 있다.

## Slide 4. Main Subset 선택

전체 app을 invocation 수 기준으로 정렬하고, 누적 invocation coverage가 99% 이상이 되는 최소 app 집합을 선택했다.

| 기준 | app 수 | invocation coverage |
|---|---:|---:|
| top-20 | 20 | 57.59% |
| top-100 | 100 | 78.45% |
| top-500 | 500 | 93.77% |
| 95% coverage | 627 | 95.003% |
| 99% coverage | 2,278 | 99.0006% |

핵심 해석:

- 전체 app의 약 9.4%가 전체 invocation의 99%를 담당한다.
- main experiment는 임의의 top-k가 아니라 workload coverage 기반이다.

## Slide 5. Burst Detection 방법

각 application의 minute-level invocation을 시계열로 두고, rolling statistics를 계산했다.

```text
x_t = minute t의 invocation count
burst if x_t > rolling_mean + z * rolling_std
```

| 항목 | 설정 |
|---|---|
| 기본 window | 60분 |
| 기본 threshold | mean + 3σ |
| sensitivity | z=1,2,3 / window=30,60,120분 |

## Slide 6. Burst 분석 결과

99% coverage app subset, 60분 rolling window 기준 결과:

| threshold | burst ratio |
|---|---:|
| mean + 1σ | 11.181% |
| mean + 2σ | 3.527% |
| mean + 3σ | 1.245% |

해석:

- 1σ는 너무 넓어서 strong burst 기준으로는 느슨하다.
- 2σ는 prewarming trigger 후보로 적절하다.
- 3σ는 강한 burst event 분석 기준으로 적절하다.
- 정규분포의 3σ upper-tail보다 실제 workload의 burst ratio가 훨씬 높다.

## Slide 7. Baseline Policy

비교한 warm policy:

| policy | 의미 |
|---|---|
| reactive | warm instance 없음. 모든 요청이 cold served |
| static warm | app마다 고정 개수의 warm instance 유지 |
| local predictive | 직전 window 평균 invocation으로 다음 minute 예측 |
| z-trigger | 최근 workload가 burst threshold를 넘으면 더 적극적으로 warm |

공통 latency model:

```text
warm request latency = execution_ms
cold request latency = execution_ms + cold_start_penalty_ms
```

기본 설정:

| parameter | 값 |
|---|---:|
| capacity | 20 invocations/min |
| execution latency | 100 ms |
| cold-start penalty | 800 ms |

## Slide 8. 기본 Baseline 결과

99% coverage subset 전체 기간 기준:

| policy | cold served ratio | weighted avg latency | avg warm instances |
|---|---:|---:|---:|
| reactive | 100.000% | 900.00 ms | 0.00 |
| static-20 | 79.382% | 735.06 ms | 20.00 |
| static-100 | 60.453% | 583.62 ms | 100.00 |
| static-1000 | 26.564% | 312.52 ms | 1000.00 |
| local predictive | 9.860% | 178.88 ms | 14.11 |

핵심 해석:

- static warm은 많이 켜둘수록 좋아지지만 cost가 매우 크다.
- local predictive는 static-1000보다 훨씬 적은 warm instance로 더 좋은 성능을 보인다.
- 따라서 local predictive는 강한 baseline으로 봐야 한다.

## Slide 9. Temporal Holdout 검증

예측 정책의 일반화를 확인하기 위해 시간 분리 검증을 수행했다.

| 구분 | 설정 |
|---|---|
| train | Day 1-10 |
| test | Day 11-14 |
| train selected apps | 2,257 |
| train coverage | 99.0002% |

Train에서 policy family별 best config를 고른 뒤, test split에서 평가했다.

## Slide 10. Holdout Test 결과

| policy | cold served ratio | weighted avg latency | weighted p95 | avg warm instances |
|---|---:|---:|---:|---:|
| z2_trigger_w30 | 6.4056% | 151.24 ms | 432.03 ms | 14.71 |
| z3_trigger_w30 | 7.2257% | 157.81 ms | 489.12 ms | 14.46 |
| local_predictive_w30 | 8.3813% | 167.05 ms | 584.15 ms | 14.19 |
| static_1000 | 30.8428% | 346.74 ms | 763.45 ms | 1000.00 |
| reactive | 100.0000% | 900.00 ms | 900.00 ms | 0.00 |

핵심 해석:

- local predictive는 holdout에서도 static보다 훨씬 좋다.
- z2-trigger는 local predictive보다 더 낮은 cold ratio와 latency를 보인다.
- 이 workload에서는 3σ보다 2σ trigger가 prewarming에 더 적합하다.

## Slide 11. Sensitivity 검증

단일 parameter 결과에 의존하지 않기 위해 cold-start penalty와 capacity를 바꿔 실험했다.

| 항목 | 값 |
|---|---|
| cold-start penalty | 200, 500, 800, 1500 ms |
| capacity | 10, 20, 50 invocations/min |
| static warm budget | 0, 1, 5, 10, 20, 50, 100, 500, 1000 |

결과:

- local predictive는 모든 조합에서 best static보다 낮은 latency와 cold ratio를 보였다.
- z2-trigger는 모든 조합에서 local predictive보다 더 낮은 latency를 보였다.
- z2-trigger의 resource cost 증가는 local predictive 대비 약 3.7-4.1% 수준이었다.

## Slide 12. 1-2주차 결론

1-2주차에서 확인한 내용:

- Azure Functions workload는 명확한 burst-heavy 특성을 가진다.
- 99% invocation coverage app subset은 main experiment 대상으로 타당하다.
- static warm은 provider-style baseline으로 필요하지만 cost 효율이 낮다.
- local predictive는 강한 baseline이다.
- temporal holdout과 sensitivity 결과를 보면 z2-trigger가 현재 가장 강한 baseline이다.

논문 관점의 의미:

```text
이후 제안기법은 reactive/static만 이기는 것으로 부족하다.
local_predictive와 z2_trigger를 이겨야 연구 기여가 된다.
```

