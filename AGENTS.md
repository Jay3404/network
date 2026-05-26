# Repository Instructions for Codex

## Project Context

This repository contains week-1 research code and notes for Azure Functions trace analysis and baseline warm policy simulation.

The current main experiment is app-level:

- Dataset: Azure Functions Trace 2019
- Unit: application (`HashApp`)
- Main subset: minimum number of apps covering at least 99% of total invocations
- Current selected subset size: 2,278 apps
- Main metrics: burst ratio, cold served ratio, weighted average latency, resource cost

## Data Policy

Do not commit downloaded datasets or generated outputs.

Ignored local directories:

- `data/`
- `outputs/`

The dataset should be recreated locally with:

```bash
bash scripts/download_azure_traces.sh
bash scripts/download_alibaba_microservices.sh
```

## Important Files

- `01_jay/azure_trace_week1.py`: Python analysis and baseline policy script
- `01_jay/alibaba_microservices_week3.py`: Alibaba graph/resource/topology prior preprocessing
- `scripts/download_azure_traces.sh`: dataset download/extract helper
- `scripts/download_alibaba_microservices.sh`: Alibaba microservices trace download/extract helper
- `scripts/setup_faas_sim.sh`: local faas-sim setup helper under `data/tools`
- `week1.md`: research summary intended for sharing in Notion or with collaborators
- `week3_4.md`: graph/resource prior and simulator preparation summary

## Reproducibility Notes

Required Python packages:

```bash
pip install -r requirements.txt
```

faas-sim uses modernized dependencies in this repo because upstream pins are not
installable on current Python:

```bash
pip install -r requirements-faas-sim-modern.txt
```

Useful commands:

```bash
python 01_jay/azure_trace_week1.py --mode inspect
python 01_jay/azure_trace_week1.py --mode app_2019 --coverage 0.99 --window 60 --z 3
python 01_jay/azure_trace_week1.py --mode baseline_2019 --baseline-level app --coverage 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
python 01_jay/alibaba_microservices_week3.py --mode inspect
python 01_jay/alibaba_microservices_week3.py --mode graph_prior --min-call-count 2 --top-edges 100000
python 01_jay/alibaba_microservices_week3.py --mode resource_prior --candidate-nodes-per-service 3
python 01_jay/alibaba_microservices_week3.py --mode topology --edge-nodes 32 --neighbor-degree 2
```

## Git Hygiene

Before committing, check that large local artifacts are not staged:

```bash
git status --short
git diff --cached --stat
```

Commit code and Markdown summaries only unless explicitly requested otherwise.
