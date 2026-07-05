"""
Use customized SparkSession object for testing.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "top-x",
                      master: str = "local[*]",
                      shuffle_partitions: int = 8) -> SparkSession:

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return builder.getOrCreate()
