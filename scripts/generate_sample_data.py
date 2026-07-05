"""Generate sample Parquet files for Dataset A and Dataset B
to test pipeline end-to-end

python scripts/generate_sample_data.py \\
    --output-dir data/sample --num-detections 2000 --num-locations 20
"""

import argparse
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

ITEM_NAMES = [
    "car", "bicycle", "pedestrian", "traffic_light", "dog",
    "motorbike", "bus", "truck", "cat", "stroller",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate sample Dataset A / B Parquet files.")
    parser.add_argument("--output-dir", default="data/sample")
    parser.add_argument("--num-locations", type=int, default=20)
    parser.add_argument("--num-cameras", type=int, default=50)
    parser.add_argument("--num-detections", type=int, default=2000)
    parser.add_argument("--duplicate-rate", type=float, default=0.1,
                        help="Fraction of detection_oids that are duplicated")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def build_dataset_b_rows(num_locations: int):
    return [
        (loc_oid, f"Location-{loc_oid}")
        for loc_oid in range(1, num_locations + 1)
    ]


def build_dataset_a_rows(num_locations,
                         num_cameras,
                         num_detections,
                         duplicate_rate,
                         rng):
    rows = []
    detection_oid = 1
    base_ts = 1_700_000_000
    while len(rows) < num_detections:
        geo_oid = rng.randint(1, num_locations)
        cam_oid = rng.randint(1, num_cameras)
        item_name = rng.choice(ITEM_NAMES)
        ts = base_ts + detection_oid

        rows.append((geo_oid, cam_oid, detection_oid, item_name, ts))

        # Simulate the ingestion bug: sometimes emit the same
        # detection_oid again (2-3 total copies), identical payload.
        if rng.random() < duplicate_rate:
            extra_copies = rng.randint(1, 2)
            for _ in range(extra_copies):
                rows.append((geo_oid, cam_oid, detection_oid, item_name, ts))

        detection_oid += 1

    # A couple of detections referencing a geo_oid NOT present in
    # Dataset B, to exercise the "filter to known locations" path.
    rows.append((num_locations + 999,
                 1,
                 detection_oid,
                 "ghost_item",
                 base_ts + detection_oid))

    return rows


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    spark = SparkSession.builder.appName("generate-sample-data").master("local[*]").getOrCreate() # noqa

    schema_a = StructType([
        StructField("geographical_location_oid", LongType(), False),
        StructField("video_camera_oid", LongType(), False),
        StructField("detection_oid", LongType(), False),
        StructField("item_name", StringType(), False),
        StructField("timestamp_detected", LongType(), False),
    ])
    schema_b = StructType([
        StructField("geographical_location_oid", LongType(), False),
        StructField("geographical_location", StringType(), False),
    ])

    rows_a = build_dataset_a_rows(
        args.num_locations,
        args.num_cameras,
        args.num_detections,
        args.duplicate_rate,
        rng
    )
    rows_b = build_dataset_b_rows(args.num_locations)

    df_a = spark.createDataFrame(rows_a, schema=schema_a)
    df_b = spark.createDataFrame(rows_b, schema=schema_b)

    df_a.write.mode("overwrite").parquet(f"{args.output_dir}/dataset_a.parquet")  # noqa
    df_b.write.mode("overwrite").parquet(f"{args.output_dir}/dataset_b.parquet")  # noqa

    print(f"Wrote {df_a.count()} rows (with duplicates) to {args.output_dir}/dataset_a.parquet") # noqa
    print(f"Wrote {df_b.count()} rows to {args.output_dir}/dataset_b.parquet")

    spark.stop()


if __name__ == "__main__":
    main()
