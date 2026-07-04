from collections import namedtuple

from topx.transformations.top_x_items import (
    build_detection_counts,
    compute_top_x_items,
    filter_to_known_locations,
)

DetectionRow = namedtuple(
    "DetectionRow",
    [
        "geographical_location_oid",
        "video_camera_oid",
        "detection_oid",
        "item_name",
        "timestamp_detected",
    ],
)
LocationRow = namedtuple("LocationRow", ["geographical_location_oid",
                                         "geographical_location"])


def test_build_detection_counts_dedupes_and_counts(spark):
    rows = [
        DetectionRow(1, 10, 100, "car", 1000),
        DetectionRow(1, 10, 100, "car", 1000),
        DetectionRow(1, 11, 101, "car", 1001),
        DetectionRow(1, 12, 102, "bicycle", 1002),
        DetectionRow(2, 20, 200, "dog", 2000),
    ]
    rdd = spark.sparkContext.parallelize(rows)

    counts = dict(build_detection_counts(rdd).collect())

    assert counts[(1, "car")] == 2
    assert counts[(1, "bicycle")] == 1
    assert counts[(2, "dog")] == 1


def test_filter_to_known_locations_drops_unknown_geo_oid(spark):
    counts = spark.sparkContext.parallelize(
        [((1, "car"), 5), ((999, "ghost_item"), 1)]
    )

    result = filter_to_known_locations(
        counts, spark.sparkContext, known_location_oids={1, 2}
    ).collect()

    assert result == [((1, "car"), 5)]


def test_compute_top_x_items_end_to_end_1(spark):
    detections = [
        DetectionRow(1, 10, 1, "car", 1000),
        DetectionRow(1, 10, 1, "car", 1000),
        DetectionRow(1, 10, 2, "car", 1001),
        DetectionRow(1, 10, 3, "bicycle", 1002),
        DetectionRow(1, 10, 4, "dog", 1003),
        DetectionRow(2, 20, 5, "cat", 2000),
        DetectionRow(999, 20, 6, "ghost_item", 3000),
    ]
    locations = [LocationRow(1, "Changi Airport"), LocationRow(2, "One-North")]

    rdd_a = spark.sparkContext.parallelize(detections)
    rdd_b = spark.sparkContext.parallelize(locations)

    result = sorted(compute_top_x_items(rdd_a,
                                        rdd_b,
                                        spark.sparkContext,
                                        top_x=2).collect())

    assert result == [
        (1, 1, "car"),
        (1, 2, "bicycle"),
        (2, 1, "cat"),
    ]
    assert all(row[0] != 999 for row in result)


def test_compute_top_x_items_end_to_end_2(spark):
    detections = [
        DetectionRow(1, 10, 1, "car", 1000),
        DetectionRow(1, 10, 1, "car", 1000),
        DetectionRow(1, 10, 2, "car", 1001),
        DetectionRow(1, 10, 3, "bicycle", 1002),
        DetectionRow(1, 10, 4, "dog", 1003),
        DetectionRow(2, 20, 5, "cat", 2000),
        DetectionRow(2, 25, 6, "car", 3000),
        DetectionRow(2, 25, 7, "car", 3002),
        DetectionRow(2, 20, 8, "cat", 3005),
        DetectionRow(999, 20, 6, "ghost_item", 5000),

    ]
    locations = [LocationRow(1, "Changi Airport"), LocationRow(2, "One-North")]

    rdd_a = spark.sparkContext.parallelize(detections)
    rdd_b = spark.sparkContext.parallelize(locations)

    result = sorted(compute_top_x_items(rdd_a,
                                        rdd_b,
                                        spark.sparkContext,
                                        top_x=3).collect())

    assert result == [
        (1, 1, "car"),
        (1, 2, "bicycle"),
        (1, 3, "dog"),
        (2, 1, "car"),
        (2, 2, "cat"),
    ]
    assert all(row[0] != 999 for row in result)
