# Design Considerations and Justifications
_Note: I opted for markdown rather than rtf or plaintext due to the ease of readibility in most IDEs_

## 1. Overall Structure
Requirement for design patterns directed me to bootstrap and then build the pipeline in a data-blind transformation structure. By removing business logic from the core functions, `dedup` and `top_n` are highly reusable for specifications that require solving of the same foundational problems. They simply operate on RDDs and functions. From there, the definition of `top_x_items` introduced the business logic, while `core` and `__main__` ties the code together.

## 2. Testing
### 2.1. Unit Tests
Once I completed implementation of a low-level function, I promptly defined unit tests that covered the expected behavior and edge cases of each component. These build together to provide basic testing across the 3 unit tests:
1. test_dedup
1. test_top_n
1. test_top_x_items

### 2.2. Integration Tests
The integration test defined in `test_core` serves as a basic check of end-to-end functionality of the logic. Using temporary, small Parquet to ensure that core functionality is operational. The completion of this serves as a green light for the final test/dataset.

### 2.3. Sample Data
Using python to generate random data of an expected structure and then using a bash script to trigger the module in a manner similar to an end user allowed me to verify that the pipeline behaves as expected.

## 3. Code Considerations

### 3.1. dedup
I chose `reduceByKey` over `groupByKey` to perform the dedup to optimise for memory. The behavior of map-side combining and the comparitive size of Dataset A's rows to possible (location, item) keys means that using `reduceByKey` results in less data shuffles.

Combined with the logic of selecting the lowest timestamp detected, this results in a consistent reduction every time.

### 3.2. top_n
The use of a bounded heap with `aggregateByKey` over a combination of `groupByKey` and sorted() was immediately obvious. Firstly, similar for dedup, `groupByKey` is incredibly memory intensive. By using `aggregateByKey`, I was able to cap the size of the heap at O(n * distinct_keys), which is significantly smaller than the side of all total rows in Dataset A.

### 3.3. top_x_items
Here, the introduction of business logic comes into play. Thanks to the explicit callout of "bonus marks for not using .join explicitly", I looked to find a way to repeatedly reference Dataset B from within Dataset A without issue. Beyond the bonus callout, it can also be assumed that Dataset B is comparatively static - it is unlikely for location ids and names to be updated frequently. So, a perusal of the documentation landed on **broadcast**. This solves 2 issues. The first is the bonus marks. The second is the performance penalty of using `.join` on two large datasets. By using broadcast, the full set of valid location ids are collected once and made available to every partition. This avoids the excessive shuffling and co-partitioning that `.join` would cause. It further saves memory as the filtering step is local, in-memory, and zero shuffle lookup, making it the fastest and objectivelu best option given the size asymmerty.

Secondly, I made the design decision to treat location ids in Dataset A that did not have a matching entry in Dataset B as orphaned/bad data. In doing so, I programmed the pipeline to discard any entries during the filter step. A potential mapping to business scenario is if we receive data from a location id we should not be reporting on.

## 4. Spark configurations
1. spark.sql.shuffle.partitions = 8 \
    > Default of 200 is tuned for large clusters. On a local instance, scheduling overhead exceeds partitioning value.Setting to 8 to roughly match available cores for local runs.

1. spark.serializer = org.apache.spark.serializer.KryoSerializer
    > Faster and more compact serialization than Java default. Improves performance for broadcast variable and shuffle payloads.

1. spark.sql.autoBroadcastJoinThreshold = 10MB (10485760 bytes)
    > Ensures that the optimization decision of broadcast for combining Dataset A and B is automated if a DataFrame-API join were used instead of the RDD broadcast-variable technique. Size was chosen to approximate the size of Dataset B tables.

1. spark.ui.showConsoleProgress = false
    > Cosmetic -- keeps local test/CI logs readable.

<br>

# End

<br>

Below is brain-vomit retained for transparency.

Want top X items per location.
Dataset A is a set of detected items with associated location.

Possible outputs looks like:

location | item_rank | item_name
1 | 1 | Item1
1 | 2 | Item2
2 | 1 | ItemA
2 | 2 | ItemB
3 | 1 | ItemX

---

Filtering flow is Location > Item Count

Build sample Dataset A & Dataset B.

Dataset A
- duplicates in detection_oid
- multiple locations
- multiple cameras
- various item names; consider how to process item names for consistency.
    - assume item names are standardized by the Data Scientist team.

---

Build an "interim" dataset that reads through Dataset A and builds a dataset for item_count of item_name of location if we want to no-op the original set.
Better to just reduce the RDD that reads in the Parquet file.
Optimize by parallelization of the processing, but need to be careful for dupe detection_oid.
Can keep a list of detection_oid, and if it's already seen then disregard every other time it is read. Then when merging if a detection_oid is true for both, disregard 1 entry.
    As per Assumption 2, process duplicates based on timestamp detected. Create working memory model of detection_oid | smallest_timestamp_so_far | location | item_name. A bit space-inefficient.
    Can be processed considering O(2n) by doing a filter through and removing all but the smallest timestamp first. Which is fine if not doing concurrency because it it preserves linear time and linear space. However with concurrency we could get faster time.
    Use reduce

location | item_name | item_count
1 | Item1 | 3
1 | Item2 | 1
2 | ItemA | 2
2 | ItemB | 1
3 | ItemX | 5

Leverage built-in data model to sort by loc ID > Item_count. Need to reverse the 2nd one cuz usually z > a.
