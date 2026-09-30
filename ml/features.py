"""Windowed feature helpers for cold-chain breach prediction.

PURE functions only — no Kafka, no Iceberg — so they are unit-testable in isolation
and shared by BOTH train.py (batch, over Iceberg history) and serve.py (live stream).
Sharing this module is what stops training and serving features from silently drifting.

These are generic aggregation utilities. WHICH features go into the model, over WHAT
window, and WHY — that is your modeling decision (see ml/README.md), not plumbing.
"""
from __future__ import annotations

import statistics
from typing import Iterator, Sequence


def rolling_windows(values: Sequence[float], size: int) -> Iterator[list[float]]:
    """Yield consecutive windows of length `size`, oldest-first.

    Fewer than `size` points yields nothing (no partial windows)."""
    if size <= 0:
        raise ValueError("size must be positive")
    for i in range(size, len(values) + 1):
        yield list(values[i - size:i])


def aggregate(window: Sequence[float]) -> dict[str, float]:
    """Summary stats for one window. Population stdev; 0.0 for a single point."""
    if not window:
        raise ValueError("empty window")
    return {
        "mean": statistics.fmean(window),
        "min": min(window),
        "max": max(window),
        "range": max(window) - min(window),
        "stdev": statistics.pstdev(window) if len(window) > 1 else 0.0,
    }


def slope(values: Sequence[float]) -> float:
    """Least-squares slope over index 0..n-1 (units per step).

    A warming freezer trends positive. 0.0 for fewer than two points."""
    n = len(values)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2
    mean_y = statistics.fmean(values)
    num = sum((x - mean_x) * (y - mean_y) for x, y in enumerate(values))
    den = sum((x - mean_x) ** 2 for x in range(n))
    return num / den if den else 0.0
