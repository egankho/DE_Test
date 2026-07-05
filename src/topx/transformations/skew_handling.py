"""Handle skew in location id in Dataset A.
A skew in the location id results in unevenly distributed work
across the partitions created during the reduce task in top_x_items.

To handle this, we can salt the skewed key, creating sub-keys.
These sub-keys are then used pre-reduction to break apart the work
into multiple partitions.

Determination of the skewed key and number of sub-partitions can be driven by
prior runs of the data pipeline and looking at the durations. It can also
be driven by a ``countByKey`` sample of the data taken at the start.
For number of sub-partitions, selection will depends on the degree of the skew
as well as the available compute.

Alternatively, we can use Spark's built-in parameters to automatically
re-balance the partitions
https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join
"""

import random
from typing import Set, Tuple

from pyspark import RDD

from topx.transformations.dedup import dedup_key


# Number of sub-keys to be generated for a skewed key.
# Set as a multiple of compute cores to ensure that partitions
# actually split out the dispropriationately heavy key.
# Value should be adjusted based on the data ingested.
DEFAULT_NUM_SALT_SPLIT = 12


def build_detection_counts_skew(dataset_a_rdd: "RDD",
                                skewed_ids: Set[int],
                                num_splits: DEFAULT_NUM_SALT_SPLIT
                                ) -> "RDD[Tuple[Tuple[int, str], int]]":
    """Outputs the same as the non-skew-ready build_detection_counts.

    Returns RDD of ``((geographical_location_oid, item_name), count)``
    """
    deduped = dedup_key(dataset_a_rdd, key_fn=lambda row: row.detection_oid,
                        order_fn=lambda row: row.timestamp_detected)

    def salt_key(row) -> Tuple[Tuple[int, str, int], int]:
        base_key = (row.geographical_location_oid, row.item_name)
        if (row.geographical_location_oid in skewed_ids):
            salt = random.randint(0, num_splits-1)
        else:
            salt = 0
        return ((base_key[0], base_key[1], salt), 1)

    salted_counts = deduped.map(salt_key).reduceByKey(lambda a, b: a + b)

    def clean_salt(pair: Tuple[Tuple[int, str, int], int]
                   ) -> Tuple[Tuple[int, str], int]:
        (location_oid, item_name, _salt), count = pair
        return ((location_oid, item_name), count)

    return salted_counts.map(clean_salt).reduceByKey(lambda a, b: a + b)
