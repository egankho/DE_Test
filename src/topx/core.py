"""Core
Reads Dataset A and B as Parquet files,
then compute and writes the result as Parquet

spark-submit src/topx/main.py \\
    --input-a /path/to/dataset_a.parquet \\
    --input-b /path/to/dataset_b.parquet \\
    --output /path/to/output.parquet \\
    --top-x 10
"""

import logging

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType
from pyspark.sql.types import StructField, StructType

from topx.transformations.top_x_items import compute_top_x_items


logger = logging.getLogger(__name__)

OUTPUT_SCHEMA = StructType(
    [
        StructField("geographical_location_oid", LongType(), nullable=False),
        StructField("item_rank", IntegerType(), nullable=False),
        StructField("item_name", StringType(), nullable=False),
    ]
)


def run(spark: SparkSession,
        input_a: str,
        input_b: str,
        output: str,
        top_x: int
        ) -> None:
    """Execute the job using pre-constructed SparkSession.
    """
    sc = spark.sparkContext

    # DataFrame only for reading Parquet
    df_a = spark.read.parquet(input_a)
    df_b = spark.read.parquet(input_b)

    logger.info("Loaded Dataset A (%d partitions) and B (%d partitions)",
                df_a.rdd.getNumPartitions(), df_b.rdd.getNumPartitions())

    rdd_a = df_a.rdd
    rdd_b = df_b.rdd

    result_rdd = compute_top_x_items(rdd_a, rdd_b, sc, top_x=top_x)

    result_rows = result_rdd.map(
        lambda row: Row(
            geographical_location_oid=row[0],
            item_rank=row[1],
            item_name=row[2],
        )
    )

    result_df = spark.createDataFrame(result_rows, schema=OUTPUT_SCHEMA)
    result_df.write.mode("overwrite").parquet(output)
    logger.info("Wrote top-%d items per location to %s", top_x, output)


if __name__ == "__main__":
    run()
