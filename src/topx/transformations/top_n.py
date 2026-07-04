"""Process dataset to obtain the top n values per key in a dataset.
Processes the data in `key, (id, score)` pairs.
"""

import heapq
from typing import Iterable, List, Tuple, TypeVar

from pyspark import RDD

K = TypeVar("K")
M = TypeVar("M")


class _ReverseOrder:
    """Class to handle comparison nuance.
    Ensure that a > b means that a.value < b.value
    Needed for numerical + alphabetical priority to align
    """

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __lt__(self, other: "_ReverseOrder") -> bool:
        return self.value > other.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _ReverseOrder) and self.value == other.value

    def __repr__(self) -> str:
        return f"_ReverseOrder({self.value!r})"


# Define heap structure
Heap = List[Tuple[int, _ReverseOrder]]


def _push_bounded(heap: Heap, entry: Tuple[int, M], n: int) -> Heap:
    """Push `entry` onto a min-heap, keeping only the n largest scores."""
    if len(heap) < n:
        heapq.heappush(heap, entry)
    elif entry > heap[0]:
        heapq.heapreplace(heap, entry)
    return heap


def _merge_bounded(a: Heap, b: Heap, n: int) -> Heap:
    """Helper for bounded heap"""
    merged = a
    for entry in b:
        merged = _push_bounded(merged, entry, n)
    return merged


def top_n_per_key(pair_rdd: "RDD[Tuple[K, Tuple[M, int]]]",
                  n: int) -> "RDD[Tuple[K, List[Tuple[M, int]]]]":
    """Get the top n pairs per key
    May return less than n if there are fewer distinct keys
    """
    if n <= 0:
        raise ValueError("n must be a positive integer")

    def seq_func(heap: Heap, member_score: Tuple[M, int]) -> Heap:
        member, score = member_score
        # heap entries are (score, _ReverseOrder(member)); score sorts
        # ascending as normal, _ReverseOrder makes the alphabetically
        # LAST member compare smallest on a tie so it is the one evited.
        return _push_bounded(heap, (score, _ReverseOrder(member)), n)

    def comb_func(a: Heap, b: Heap) -> Heap:
        return _merge_bounded(a, b, n)

    # use aggregateByKey to insert pairs into heap.
    aggregated = pair_rdd.aggregateByKey([], seq_func, comb_func)

    def finalize(heap: Heap) -> List[Tuple[M, int]]:
        # Sort descending by score, then ascending by member
        ordered = sorted(heap, key=lambda entry: (-entry[0], entry[1].value))
        return [(entry[1].value, entry[0]) for entry in ordered]

    return aggregated.mapValues(finalize)


def flatten_ranked(ranked_rdd: "RDD[Tuple[K, List[Tuple[M, int]]]]"
                   ) -> "RDD[Tuple[K, int, M]]":
    """Flatten nested tuple into tuple of 3
    based on rank for member score.
    """

    def to_rows(pair: Tuple[K, List[Tuple[M, int]]]
                ) -> Iterable[Tuple[K, int, M]]:
        key, ranked_members = pair
        for rank, (member, _score) in enumerate(ranked_members, start=1):
            yield (key, rank, member)

    return ranked_rdd.flatMap(to_rows)
