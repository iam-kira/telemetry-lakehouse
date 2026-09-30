"""Runnable checks for ml/features.py — no infra needed.

    python ml/test_features.py
"""
from features import aggregate, rolling_windows, slope


def test_rolling_windows():
    assert list(rolling_windows([1, 2, 3, 4], 2)) == [[1, 2], [2, 3], [3, 4]]
    assert list(rolling_windows([1, 2], 3)) == []          # too few points -> nothing
    assert list(rolling_windows([], 2)) == []
    try:
        list(rolling_windows([1, 2], 0))                   # invalid size
    except ValueError:
        pass
    else:
        raise AssertionError("size<=0 should raise")


def test_aggregate():
    a = aggregate([-20.0, -18.0, -16.0])
    assert a["min"] == -20.0 and a["max"] == -16.0
    assert a["range"] == 4.0
    assert abs(a["mean"] - (-18.0)) < 1e-9
    assert aggregate([5.0])["stdev"] == 0.0                # single point -> 0 stdev


def test_slope():
    assert abs(slope([0, 1, 2, 3]) - 1.0) < 1e-9           # rising 1 per step
    assert abs(slope([3, 3, 3]) - 0.0) < 1e-9              # flat
    assert slope([10]) == 0.0                              # degenerate


if __name__ == "__main__":
    for _name, _fn in sorted(list(globals().items())):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print(f"ok  {_name}")
    print("all feature tests passed")
