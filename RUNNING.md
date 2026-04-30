# 실행 방법

이 문서는 현재 repository의 주요 분석 스크립트 실행 방법을 정리한다. 코드 디렉터리는 `code/`이다.

## 1. 기본 준비

Python 패키지:

```bash
pip install pandas numpy matplotlib
```

ML baseline까지 실행하려면 추가 패키지가 필요하다.

```bash
pip install torch lightgbm scikit-learn
```

대용량 데이터와 생성 결과는 git에 포함하지 않는다.

```text
data/     # downloaded datasets
outputs/  # generated summaries, plots, result CSVs
```

## 2. Azure Functions Trace 다운로드

Azure Functions Trace 2019/2021 데이터는 다음 명령으로 다운로드 및 압축 해제한다.

```bash
bash scripts/download_azure_traces.sh
```

macOS에서 2021 `.rar` 압축 해제가 실패하면 `unar`를 설치한 뒤 다시 실행한다.

```bash
brew install unar
```

## 3. Week 1: Azure Functions 분석

메인 스크립트:

```text
code/azure_trace_week1.py
```

### 3.1 데이터 구조 확인

```bash
python code/azure_trace_week1.py --mode inspect
```

### 3.2 Burst 분석

99% invocation coverage app subset 기준으로 app-level burst 분석을 실행한다.

```bash
python code/azure_trace_week1.py \
  --mode analysis_2019 \
  --coverage-threshold 0.99 \
  --window 60 \
  --z 3
```

주요 출력:

```text
outputs/week1/analysis_2019/
```

### 3.3 Reactive / Static / Local Predictive Baseline

```bash
python code/azure_trace_week1.py \
  --mode baseline_2019 \
  --coverage-threshold 0.99 \
  --capacity 20 \
  --cold-start-penalty 800 \
  --execution-ms 100 \
  --static-warm 1 \
  --prediction-window 60
```

주요 출력:

```text
outputs/week1/baseline_2019/baseline_policy_summary.csv
outputs/week1/baseline_2019/static_warm_sensitivity.csv
```

주의:

- `capacity=20`은 Azure 공식값이 아니라 보수적인 simulation parameter다.
- `execution_ms=100`, `cold_start_penalty=800`도 lightweight function scenario 가정이다.

### 3.4 Local Predictive Variant Sweep

```bash
python code/azure_trace_week1.py \
  --mode predictive_sweep_2019 \
  --coverage-threshold 0.99 \
  --capacity 20 \
  --cold-start-penalty 800 \
  --execution-ms 100 \
  --static-warm 1 \
  --prediction-window 60
```

주요 출력:

```text
outputs/week1/predictive_sweep_2019/predictive_variant_summary.csv
```

기본 비교 predictor:

```text
local_ma_15
local_ma_60
local_ma_180
local_ewma_a0p3
local_mean_std_60_z1
local_p95_60
local_max_ma_5_60
local_max_ma_60_seasonal_1440
```

### 3.5 TCN / LSTM / LightGBM Rolling Forecast Baseline

Chronological 70/30 holdout과 expanding-window rolling-origin을 함께 실행한다.

```bash
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

주요 출력:

```text
outputs/week1/ml_forecast_2019/ml_forecast_summary.csv
outputs/week1/ml_forecast_2019/ml_forecast_rolling_folds.csv
outputs/week1/ml_forecast_2019/ml_forecast_rolling_summary.csv
```

빠른 smoke test는 모델과 샘플 수를 줄여서 실행한다.

```bash
python code/azure_trace_week1.py \
  --mode ml_forecast_2019 \
  --top-k 5 \
  --ml-models local_mean_std,lightgbm \
  --ml-train-ratio 0.7 \
  --ml-eval-mode both \
  --ml-train-samples 1000 \
  --lightgbm-estimators 10
```

## 4. Week 2: Alibaba Microservices Trace

메인 스크립트:

```text
code/alibaba_trace_week2.py
```

### 4.1 로컬 데이터 manifest 확인

```bash
python code/alibaba_trace_week2.py --mode inspect
```

주요 출력:

```text
outputs/week2/alibaba_microservices_2021/dataset_manifest.csv
```

### 4.2 Alibaba 다운로드 manifest 확인

전체 데이터는 수십 GiB 규모이므로 먼저 manifest만 확인한다.

```bash
bash scripts/preprocess/download_alibaba_microservices_2021.sh --manifest-only
```

### 4.3 부분 다운로드 smoke test

각 테이블 1개 파일만 다운로드해 schema와 summary 생성이 되는지 확인한다.

```bash
bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --callgraph-only \
  --callgraph-limit 1 \
  --extract

bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --resource-only \
  --resource-limit 1 \
  --extract

bash scripts/preprocess/download_alibaba_microservices_2021.sh \
  --rtqps-only \
  --rtqps-limit 1 \
  --extract
```

### 4.4 Alibaba summary 및 graph prior 생성

sample file 기준:

```bash
python code/alibaba_trace_week2.py \
  --mode all \
  --max-files 1 \
  --chunksize 500000 \
  --rt-scale-ms 100
```

전체 다운로드가 끝난 뒤 full run:

```bash
python code/alibaba_trace_week2.py \
  --mode all \
  --chunksize 500000 \
  --rt-scale-ms 100
```

주요 출력:

```text
outputs/week2/alibaba_microservices_2021/callgraph_edge_summary.csv
outputs/week2/alibaba_microservices_2021/callgraph_service_summary.csv
outputs/week2/alibaba_microservices_2021/service_resource_summary.csv
outputs/week2/alibaba_microservices_2021/service_rtqps_summary.csv
outputs/week2/alibaba_microservices_2021/graph_prior_edges.csv
outputs/week2/alibaba_microservices_2021/graph_prior_services.csv
```

## 5. Simulator 환경

### 5.1 faas-sim

Week 2의 primary simulator 후보는 `faas-sim`이다.

```bash
bash scripts/sim/setup_faas_sim.sh
```

이 명령은 `tools/faas-sim`에 repository를 clone하고 `.venv/faas-sim`에 Python 환경을 만든다. 첫 실행은 network access가 필요하다.

### 5.2 EdgeCloudSim

EdgeCloudSim은 edge/cloud topology simulation이 필요할 때 secondary option으로 둔다.

```bash
bash scripts/sim/setup_edgecloudsim.sh
```

Java가 필요하다. macOS에서 Java가 없으면 다음처럼 설치할 수 있다.

```bash
brew install openjdk@21
```

## 6. 검증 명령

코드 문법 확인:

```bash
python3 -m py_compile code/azure_trace_week1.py
python3 -m py_compile code/alibaba_trace_week2.py
```

Shell script 문법 확인:

```bash
bash -n scripts/download_azure_traces.sh
bash -n scripts/preprocess/download_alibaba_microservices_2021.sh
bash -n scripts/sim/setup_faas_sim.sh
bash -n scripts/sim/setup_edgecloudsim.sh
```

Git 상태 확인:

```bash
git status --short
git diff --check
```
