"""
CLean input dataset from duplicates to handle Dataset A issues.

Take lowest timestamp as source of truth.
"""

from typing import Callable, TypeVar
from pyspark import RDD

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def dedup_key(rdd: "RDD[T]", key_fn: Callable[[T], K],
              order_fn: Callable[[T], V]) -> "RDD[T]":

    def keep_lowest(left: T, right: T) -> T:
        return left if order_fn(left) <= order_fn(right) else right

    keyed = rdd.map(lambda row: (key_fn(row), row))
    deduped = keyed.reduceByKey(keep_lowest)
    return deduped.values()
