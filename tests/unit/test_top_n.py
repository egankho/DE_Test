import pytest

from topx.transformations.top_n import flatten_ranked, top_n_per_key


def test_top_n_per_key_basic_ranking(spark):
    data = [
        (1, ("car", 50)),
        (1, ("bicycle", 30)),
        (1, ("dog", 10)),
        (1, ("bus", 40)),
        (2, ("cat", 5)),
        (2, ("truck", 7)),
    ]
    rdd = spark.sparkContext.parallelize(data)

    result = dict(top_n_per_key(rdd, n=2).collect())

    assert result[1] == [("car", 50), ("bus", 40)]
    assert result[2] == [("truck", 7), ("cat", 5)]


def test_top_n_per_key_when_fewer_members_than_n(spark):
    data = [(1, ("car", 5))]
    rdd = spark.sparkContext.parallelize(data)

    result = dict(top_n_per_key(rdd, n=10).collect())

    assert result[1] == [("car", 5)]


def test_top_n_per_key_breaks_ties_deterministically(spark):
    data = [
        (1, ("zebra", 10)),
        (1, ("apple", 10)),
    ]
    rdd = spark.sparkContext.parallelize(data)

    result = dict(top_n_per_key(rdd, n=2).collect())

    assert result[1] == [("apple", 10), ("zebra", 10)]


def test_top_n_per_key_breaks_ties_deterministically_2(spark):
    data = [
        (1, ("bicycle", 10)),
        (1, ("dog", 10)),
    ]
    rdd = spark.sparkContext.parallelize(data)

    result = dict(top_n_per_key(rdd, n=1).collect())

    assert result[1] == [("bicycle", 10)]


def test_top_n_per_key_evicts_alphabetically_last_member_on_tie(spark):
    data = [
        (1, ("bicycle", 1)),
        (1, ("dog", 1)),
        (1, ("car", 2)),
    ]
    rdd = spark.sparkContext.parallelize(data)

    result = dict(top_n_per_key(rdd, n=2).collect())

    assert result[1] == [("car", 2), ("bicycle", 1)]


def test_top_n_per_key_eviction_is_order_independent(spark):
    forward = spark.sparkContext.parallelize(
        [(1, ("bicycle", 1)),
         (1, ("dog", 1)),
         (1, ("car", 2))]
    )
    reversed_order = spark.sparkContext.parallelize(
        [(1, ("car", 2)),
         (1, ("dog", 1)),
         (1, ("bicycle", 1))]
    )

    result_forward = dict(top_n_per_key(forward, n=2).collect())
    result_reversed = dict(top_n_per_key(reversed_order, n=2).collect())

    assert result_forward == result_reversed == {1: [("car", 2),
                                                     ("bicycle", 1)]}


def test_top_n_per_key_rejects_non_positive_n(spark):
    rdd = spark.sparkContext.parallelize([(1, ("car", 1))])

    with pytest.raises(ValueError):
        top_n_per_key(rdd, n=0)


def test_flatten_ranked_produces_one_row_per_member_with_rank(spark):
    ranked = spark.sparkContext.parallelize(
        [(1, [("car", 50), ("bus", 40)]),
         (2, [("truck", 7)])]
    )

    rows = sorted(flatten_ranked(ranked).collect())

    assert rows == [
        (1, 1, "car"),
        (1, 2, "bus"),
        (2, 1, "truck"),
    ]
