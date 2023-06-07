#!/usr/bin/env python

from pandas import Interval, Timedelta, Timestamp
from numpy import ceil, array
from numpy.typing import NDArray
from pandas import interval_range as pd_interval_range


def overlap_percent(a : Interval, b : Interval) -> float:
    if not a.overlaps(b):
        return 0
    else :
        dt = (min(a.right, b.right) - max(a.left, b.left)).total_seconds()
        return dt / a.length.total_seconds()


def interval_range(start: Timestamp, end: Timestamp, freq: str | Timedelta) -> NDArray:
    nt = ceil((end - start) / Timedelta(freq))
    intervals = array(pd_interval_range(start, freq=freq, periods=nt))
    intervals[-1] = Interval(intervals[-1].left, end)
    return intervals
