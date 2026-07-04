from collections import namedtuple

from topx.transformations.dedup import dedup_key

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


def test_dedup_removes_duplicate_detection_oids1(spark):
    rows = [
        DetectionRow(1, 10, 100, "car", 1000),
        DetectionRow(1, 10, 100, "car", 1001),
        DetectionRow(1, 10, 100, "car", 1002),
        DetectionRow(1, 11, 101, "bicycle", 1003),
        DetectionRow(2, 12, 102, "dog", 1004),
    ]
    rdd = spark.sparkContext.parallelize(rows)

    result = dedup_key(rdd, lambda row: row.detection_oid,
                       lambda row: row.timestamp_detected).collect()

    assert len(result) == 3
    detection_oids = sorted(row.detection_oid for row in result)
    assert detection_oids == [100, 101, 102]


def test_dedup_removes_duplicate_detection_oids2(spark):
    rows = [
        DetectionRow(1, 10, 100, "car", 1000),
        DetectionRow(1, 10, 100, "car", 1001),
        DetectionRow(1, 10, 100, "car", 1001),
        DetectionRow(1, 10, 100, "car", 1005),
        DetectionRow(1, 11, 101, "bicycle", 1003),
        DetectionRow(2, 12, 102, "dog", 1004),
        DetectionRow(2, 12, 102, "dog", 1005),
        DetectionRow(2, 12, 102, "dog", 1002),
    ]
    rdd = spark.sparkContext.parallelize(rows)

    result = dedup_key(rdd, lambda row: row.detection_oid,
                       lambda row: row.timestamp_detected).collect()

    assert len(result) == 3
    detection_oids = sorted(row.detection_oid for row in result)
    assert detection_oids == [100, 101, 102]


def test_dedup_noop_when_no_duplicates(spark):
    rows = [
        DetectionRow(1, 10, 1, "car", 1000),
        DetectionRow(1, 10, 2, "bicycle", 1001),
    ]
    rdd = spark.sparkContext.parallelize(rows)

    result = dedup_key(rdd, key_fn=lambda row: row.detection_oid,
                       order_fn=lambda row: row.timestamp_detected).collect()

    assert len(result) == 2


def test_dedup_handles_empty_rdd(spark):
    rdd = spark.sparkContext.parallelize([], numSlices=2)

    result = dedup_key(rdd, key_fn=lambda row: row,
                       order_fn=lambda row: row).collect()

    assert result == []
