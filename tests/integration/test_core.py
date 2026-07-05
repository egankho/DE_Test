"""Integration test of full pipeline:
Parquet in -> RDD transformations -> Parquet out
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType

from topx.core import run

SCHEMA_A = StructType(
    [
        StructField("geographical_location_oid", LongType(), False),
        StructField("video_camera_oid", LongType(), False),
        StructField("detection_oid", LongType(), False),
        StructField("item_name", StringType(), False),
        StructField("timestamp_detected", LongType(), False),
    ]
)
SCHEMA_B = StructType(
    [
        StructField("geographical_location_oid", LongType(), False),
        StructField("geographical_location", StringType(), False),
    ]
)


def test_job_runs_end_to_end_and_writes_expected_output(spark,
                                                        tmp_path,
                                                        tmp_output_dir):
    rows_a = [
        (1, 10, 1, "car", 1000),
        (1, 10, 1, "car", 1000),
        (1, 10, 2, "car", 1001),
        (1, 10, 3, "bicycle", 1002),
        (2, 20, 4, "dog", 2000),
        (2, 20, 5, "dog", 2001),
        (2, 20, 6, "cat", 2002),
        (999, 30, 7, "ghost_item", 3000),
    ]
    rows_b = [
        (1, "Changi Airport"),
        (2, "One-North"),
    ]

    in_path_a = str(tmp_path/"input"/"dataset_a.parquet")
    in_path_b = str(tmp_path/"input"/"dataset_b.parquet")

    spark.createDataFrame(rows_a, schema=SCHEMA_A).write.mode("overwrite").parquet(in_path_a) # noqa
    spark.createDataFrame(rows_b, schema=SCHEMA_B).write.mode("overwrite").parquet(in_path_b) # noqa

    run(spark, input_a=in_path_a, input_b=in_path_b,
        output=tmp_output_dir, top_x=1)

    result_df = spark.read.parquet(tmp_output_dir)
    result = sorted(result_df.collect(),
                    key=lambda row: row.geographical_location_oid)

    assert [tuple(row) for row in result] == [
        (1, 1, "car"),
        (2, 1, "dog"),
    ]
    assert set(result_df.columns) == {"geographical_location_oid",
                                      "item_rank", "item_name"}


def test_job_respects_top_x_configuration(spark, tmp_path, tmp_output_dir):
    rows_a = [
        (1, 10, 1, "car", 1000),
        (1, 10, 2, "bicycle", 1001),
        (1, 10, 3, "dog", 1002),
    ]
    rows_b = [(1, "Changi Airport")]

    in_path_a = str(tmp_path/"input"/"dataset_a.parquet")
    in_path_b = str(tmp_path/"input"/"dataset_b.parquet")

    spark.createDataFrame(rows_a, schema=SCHEMA_A).write.mode("overwrite").parquet(in_path_a) # noqa
    spark.createDataFrame(rows_b, schema=SCHEMA_B).write.mode("overwrite").parquet(in_path_b) # noqa

    run(spark, input_a=in_path_a, input_b=in_path_b,
        output=tmp_output_dir, top_x=3)

    result_df = spark.read.parquet(tmp_output_dir)
    assert result_df.count() == 3
    ranks = sorted(row.item_rank for row in result_df.collect())
    assert ranks == [1, 2, 3]
