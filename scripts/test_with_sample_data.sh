#!/bin/bash

python scripts/generate_sample_data.py --output-dir data/sample \
    --num-locations 20 --num-detections 2000

python -m topx \
    --input-a data/sample/dataset_a.parquet \
    --input-b data/sample/dataset_b.parquet \
    --output data/output/output.parquet \
    --top-x 5

# Inspect the result with a quick pyspark shell:

python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()
spark.read.parquet('data/output/output.parquet').orderBy(
    'geographical_location_oid', 'item_rank'
).show(50, truncate=False)
"
