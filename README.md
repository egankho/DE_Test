# DE_Test
Technical Test Questions for Data Engineer

## Structure
```
├── docs
├── scripts
├── src/topx
│       ├── transformations
│       └── utils
└── tests
    ├── integration
    └── unit
```

## Setup & Run
1. Run `setup.sh` to verify and install the necessary dependencies for pipeline to run.
1. Run `./scripts/test_with_sample_data.sh` in the root directory to test with sample data.
1. To run against Parquet test files, kindly run `python -m topx --input-a <PATH to Dataset A> --input-b <PATH to Dataset B> --output <PATH to Output> --top-x <X Configuration>`.

## Assumptions Made
1. The input in Dataset A has well-defined `item_names`. For example, if Item A is detected, it will always be named "Item A" and not "A" or "ItemA" or "Item 1".
1. Item A detected in Location 1 and Item A detected in Location 2 are both reported
1. Counting each `detection_oid` only once entails that if there are multiple rows with the same `detection_oid`, the row with the smaller `timestamp_detected` will be considered. If an older timestamp is read, it will be disregarded. If a smaller timestamp is read, then the previously read item has its count reduced and the timestamp is noted.
1. When producing a "Top X", the resulting list will only include locations detailed in Dataset B. If there are locations in Dataset A that do not appear in B, they are disregarded.
1. Written components of the assessment are in `/docs`.

<br><br>

# Background

## Prompt

1. As a Data Engineer, you are tasked to compute the top X items identified by video cameras in different geographical locations in the Town of Utopia. Your boss wants to know what are the most popular few items detected through object detection algorithms by your Data Scientist Coworkers.
1. Prepare unit test / integration test cases based on the data given. The integration test shall prove that the entire PySpark job can be run on a local spark test dev environment.
1. Suppose there is data skew in one of the geographical locations in Dataset A. Provide another code snippet on how to re-implement part of the program to speed up the computation. Refer to `skew_handling.py`
1. Explain the different sorting strategies in Spark and which strategy to adopt when joining Parquet File 1 and 2 if implementing the code in Spark Dataframe.


## Requirements
1. Python
1. Spark-based transformation code, only using RDD
1. Style-check with [Flake8](https://github.com/PyCQA/flake8).
1. In the PySpark job main class, you should provide the flexibility to change the following:
    1. Input path for Parquet File 1
    1. Input path for Parquet File 2
    1. Output path for Parquet File 3
    1. Top X configuration (For example, during runtime, we can specify 10, in order to return top 10 items to be considered.)
1. Only when reading/output the parquet file, you can choose to use Spark DataFrame API to read/ write it in/ out
1. Clean code guidelines
1. Use design patterns
1. Consider optimality of the time/space complexity of the code and the shuffle stages
    1. Bonus if "join" is performed without explicit use of `.join`
1. Document considerations when designing code and spark configurations.

## Data Models

### Dataset A
Parquet file, ~1 million rows

| Column Name | Column Type | Comment |
|-------------|-------------|---------|
| `geographical_location_oid` | bigint| A unique bigint identifier for the geographical location |
| `ideo_camera_oid` | bigint | A unique bigint identifier for the video camera that the item was detected from |
| `detection_oid` | bigint | A unique bigint identifier for each detection event |
| `item_name` | varchar(5000) | Item name |
| `timestamp_detected` | bigint | timestamp for a given timestamp detected |

_Note: duplicate `detection_oid` is possible; disregard duplicates, e.g. if multiple rows of the same `detection_oid` are in the table, only add 1 to the `item_count`._

### Dataset B
Parquet file, 10,000 rows

| Column Name | Column Type | Comment |
|-------------|-------------|---------|
| `geographical_location_oid` | bigint| A unique bigint identifier for the geographical location |
| `geographical_location` | varchar(500) | Geographical Location |

### Output
Parquet file, ~ <= 1 million rows
| Column Name | Column Type | Comment |
|-------------|-------------|---------|
| `geographical_location_oid` | bigint| A unique bigint identifier for the geographical location |
| `item_rank` | int | `item_rank = 1` corresponds to the most popular item detected in `geographical_location` |
| `item_name` | varchar(5000) | Item name |
