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
```

## Important Files

- `code/azure_trace_week1.py`: Python analysis and baseline policy script
- `code/alibaba_trace_week2.py`: Alibaba microservices preprocessing and graph-prior script
- `scripts/download_azure_traces.sh`: dataset download/extract helper
- `scripts/preprocess/download_alibaba_microservices_2021.sh`: Alibaba microservices download helper
- `RUNNING.md`: reproducible command reference
- `week1.md`: research summary intended for sharing in Notion or with collaborators
- `week2.md`: Alibaba graph-prior and simulator setup summary

## Reproducibility Notes

Required Python packages:

```bash
pip install pandas numpy matplotlib
```

Useful commands:

```bash
python code/azure_trace_week1.py --mode inspect
python code/azure_trace_week1.py --mode analysis_2019 --top-k 20 --window 60 --z 3
python code/azure_trace_week1.py --mode baseline_2019 --top-k 20 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
```

## Git Hygiene

Before committing, check that large local artifacts are not staged:

```bash
git status --short
git diff --cached --stat
```

Commit code and Markdown summaries only unless explicitly requested otherwise.
