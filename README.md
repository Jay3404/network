# Network Research Notes

Azure Functions trace, Alibaba microservices trace, and synthetic edge topology
experiments for serverless/edge warm policy research.

## Main Documents

- `advise.txt`: research direction and week plan.
- `RUNNING.md`: local execution, dataset, NAS mount, and verification commands.
- `week1.md`: week 1-2 Azure trace/baseline summary.
- `week3_4.md`: week 3-4 Alibaba graph/resource prior and topology summary.

## Data Policy

Downloaded datasets and generated outputs are local artifacts and should not be
committed.

```text
data/
outputs/
```

## NAS Data Mount

Large Alibaba raw/extracted data can be read from the lab NAS through the
intermediate server. On the Mac, the expected mount point is:

```text
/Users/jay/mnt/dbi-nas
```

After mounting, the Alibaba microservices trace path is:

```text
/Users/jay/mnt/dbi-nas/data/alibaba_clusterdata/extracted/microservices_v2021
```

The full setup and recovery commands are documented in `RUNNING.md`.
