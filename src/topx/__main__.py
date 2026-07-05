"""Main
enables python -m topx ... call
"""


import argparse
import logging

from topx.utils.spark_session import get_spark_session

from topx.core import run


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute the top X detected items per location."
    )
    parser.add_argument(
        "--input-a",
        required=True,
        help="Input path for Dataset A",
    )
    parser.add_argument(
        "--input-b",
        required=True,
        help="Input path for Dataset B",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for processed data",
    )
    parser.add_argument(
        "--top-x",
        type=int,
        required=True,
        help="How many top items to return per location.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    spark = get_spark_session(app_name="top-x")
    try:
        run(spark, args.input_a, args.input_b, args.output, args.top_x)
    finally:
        spark.stop()


main()
