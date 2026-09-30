"""Train + evaluate the breach predictor.  SKELETON — the model and evaluation are YOURS.

This is the part an interviewer probes, so it has to be your work and your numbers.
The harness (load, split, save) lives here; the decisions do not.
"""
from __future__ import annotations

# TODO(you): load the table extract.py produced.
# TODO(you): a TIME-BASED train/test split. A random split leaks the future — be ready to say why.
# TODO(you): choose a model. Start simple (logistic regression, then gradient-boosted trees).
# TODO(you): evaluate on the held-out period with precision / recall / PR-AUC — NOT accuracy.
#            Breaches are rare, so accuracy is a trap; know that cold.
# TODO(you): check overfitting (train vs test gap), inspect feature importance, save model.pkl.

if __name__ == "__main__":
    raise SystemExit("train.py is a skeleton — implement the TODOs (see ml/README.md).")
