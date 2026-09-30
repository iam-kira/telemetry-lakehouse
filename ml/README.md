# Cold-chain breach prediction (ML)

A predictive-maintenance layer on the telemetry-lakehouse pipeline. The Flink job
*alerts on* a breach after it happens; this **predicts** that a freezer is trending
toward a breach in the next **H** minutes from its recent behaviour, and scores it
live on the Kafka stream.

## Why this design
- **Real data, no toy set.** Training data is the Iceberg history the pipeline already
  stores; live scoring reads the same `telemetry` Kafka topic. One system, end to end.
- **Feature parity.** `features.py` is pure and shared by batch training *and* live
  serving, so the two can't silently diverge — a classic production-ML failure mode.

## Pipeline

```
Iceberg history ──extract.py──▶ training table ──train.py──▶ model.pkl
                                                                 │
   telemetry (Kafka) ──serve.py + features.py──▶ predict ──▶ predictions (Kafka)
```

## Scaffolded vs yours

| Part | Status |
|---|---|
| `features.py` windowed-stat utilities + `test_features.py` | ✅ scaffolded (generic plumbing, tested) |
| `extract.py` / `train.py` / `serve.py` structure | ✅ skeletons with TODOs |
| **Feature selection** — which stats, which window, why | ⬜ **you** |
| **Label definition** — breach within horizon H | ⬜ **you** |
| **Model choice, training, tuning** | ⬜ **you** |
| **Evaluation** — time-based split, precision/recall/PR-AUC, overfitting | ⬜ **you** |

The scaffold is **not** the project. The model you build and can *explain* is — that's
the part an interviewer probes and the part that earns a line on your resume.

## Run

```bash
python ml/test_features.py     # green, needs no services
# later, in lakehouse mode, once you've filled the TODOs:
# python ml/extract.py         # build the training table from Iceberg
# python ml/train.py           # train + evaluate, save model.pkl
```

## Interview-defensible checklist (fill as you go)
- [ ] Why predict-ahead beats threshold-alerting for cold-chain (business framing)
- [ ] Label + horizon H, and the class imbalance it creates
- [ ] Why a time-based split, not random
- [ ] Metrics chosen and why (not accuracy)
- [ ] Overfitting check + top features and whether they make physical sense
