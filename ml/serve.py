"""Score the live telemetry stream and emit predictions.  SKELETON — plumbing outline.

Mode: flink/api (needs kafka). Consumes `telemetry`, keeps a short per-device history,
computes the SAME features as training (via ml/features.py), predicts, and emits to a
`predictions` topic. Reusing features.py is what keeps train and serve from drifting.
"""
from __future__ import annotations

# TODO(you): confluent_kafka Consumer on `telemetry` (see api/ or bridge/ for the client pattern).
# TODO(you): keep the last N readings per device; reuse ml/features.py to build the feature vector.
# TODO(you): load model.pkl; on each reading, predict; produce the result to `predictions`.
# TODO(you): write one design note in README on why train/serve feature-parity matters.

if __name__ == "__main__":
    raise SystemExit("serve.py is a skeleton — implement the TODOs (see ml/README.md).")
