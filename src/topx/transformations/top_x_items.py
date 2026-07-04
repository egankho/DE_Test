"""Combine transformations into task - identify Top X detected
items per location.
"""

from typing import Set, Tuple

from pyspark import RDD, SparkContext

from topx.transformations.dedup import dedup_key
from topx.transformations.top_n import flatten_ranked, top_n_per_key


def build_detection_counts(dataset_a_rdd: "RDD"
                           ) -> "RDD[Tuple[Tuple[int, str], int]]":
    """Dedup Dataset A's detection_oid, then count detections per
    key ``(geographical_location_oid, item_name)``.

    Returns RDD of ``((geographical_location_oid, item_name), count)``.
    """
    deduped = dedup_key(dataset_a_rdd, key_fn=lambda row: row.detection_oid,
                        order_fn=lambda row: row.timestamp_detected)
    pairs = deduped.map(lambda row: ((row.geographical_location_oid,
                                      row.item_name), 1))
    return pairs.reduceByKey(lambda a, b: a + b)


def filter_to_known_locations(counts_rdd: "RDD[Tuple[Tuple[int, str], int]]",
                              sc: SparkContext,
                              known_location_oids: Set[int],
                              ) -> "RDD[Tuple[Tuple[int, str], int]]":
    """Drop rows for any location that does not exist in Dataset B,
    using a broadcast variable instead of .join.

    Returns RDD of ``((geographical_location_oid, item_name), count)``.
    """
    broadcast_locations = sc.broadcast(known_location_oids)

    def is_known(pair: Tuple[Tuple[int, str], int]) -> bool:
        (location_oid, _item_name), _count = pair
        return location_oid in broadcast_locations.value

    return counts_rdd.filter(is_known)


def compute_top_x_items(dataset_a_rdd: "RDD", dataset_b_rdd: "RDD",
                        sc: SparkContext, top_x: int,
                        ) -> "RDD[Tuple[int, int, str]]":
    """End-to-end transformation of Dataset A and B to produce
    Dataset C, the location, item rank, and item name.

    Returns RDD of ``(geographical_location_oid, item_rank, item_name)``
    """
    counts = build_detection_counts(dataset_a_rdd)

    known_locations: Set[int] = set(
        dataset_b_rdd
        .map(lambda row: row.geographical_location_oid)
        .distinct()
        .collect()
    )
    counts = filter_to_known_locations(counts, sc, known_locations)

    # Reshape to (geo_oid, (item_name, count))
    per_location_items = counts.map(
        lambda pair: (pair[0][0], (pair[0][1], pair[1]))
    )

    ranked = top_n_per_key(per_location_items, n=top_x)
    return flatten_ranked(ranked)
