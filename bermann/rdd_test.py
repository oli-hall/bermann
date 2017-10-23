import unittest

from bermann import RDD


class TestRDD(unittest.TestCase):

    def test_cache_is_noop(self):
        rdd = RDD([1, 2, 3])

        cached = rdd.cache()

        self.assertEqual(rdd, cached)

    def test_collect_empty_rdd_returns_empty_list(self):
        rdd = RDD()

        self.assertEqual([], rdd.collect())

    def test_collect_non_empty_rdd_returns_contents(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(rdd.contents, rdd.collect())

    def test_count_empty_rdd_returns_zero(self):
        rdd = RDD()

        self.assertEqual(0, rdd.count())

    def test_collect_non_empty_rdd_returns_length(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(3, rdd.count())

    def test_countbykey_empty_rdd_returns_empty_dict(self):
        rdd = RDD()

        self.assertEqual({}, rdd.countByKey())

    def test_countbykey_non_empty_rdd_returns_dict_of_counts(self):
        rdd = RDD([('a', 1), ('b', 2), ('a', 3)])

        self.assertEqual({'a': 2, 'b': 1}, rdd.countByKey())

    def test_countbyvalue_rdd_returns_dict_of_counts(self):
        rdd = RDD([1, 2, 3, 1, 2, 1])

        self.assertEqual({1: 3, 2: 2, 3: 1}, rdd.countByValue())

    def test_distinct_rdd_returns_unique_list(self):
        rdd = RDD([1, 2, 3, 1, 2, 1])

        self.assertEqual([1, 2, 3], rdd.distinct().collect())

    def test_distinct_on_unique_list_is_noop(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([1, 2, 3], rdd.distinct().collect())

    def test_filter_rdd_by_identity_returns_input(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([1, 2, 3], rdd.filter(lambda x: True).collect())

    def test_filter_rdd_returns_filters_by_input_func(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([2, 3], rdd.filter(lambda x: x > 1).collect())

    def test_first_empty_rdd_raises_valueerror(self):
        rdd = RDD()

        with self.assertRaises(ValueError) as e:
            rdd.first()
        self.assertEqual(ValueError, type(e.exception))

    def test_first_non_empty_rdd_returns_first_elem(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(1, rdd.first())

    # flatMap

    # flatMapValues

    # foreach

    # groupBy

    # groupByKey

    def test_isempty_returns_false_for_non_empty_rdd(self):
        rdd = RDD([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertFalse(rdd.isEmpty())

    def test_isempty_returns_true_for_empty_rdd(self):
        rdd = RDD()

        self.assertTrue(rdd.isEmpty())

    # keyBy

    def test_keys_returns_keys_of_kv_rdd(self):
        rdd = RDD([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertEqual(['k1', 'k1', 'k2'], rdd.keys().collect())

    def test_keys_returns_first_elem_of_non_tuple_rdd(self):
        rdd = RDD(['a', 'b', 'c'])

        self.assertEqual(['a', 'b', 'c'], rdd.keys().collect())

    # map

    # mapValues

    # max

    # min

    def test_get_name_sets_name(self):
        name = 'my RDD'
        rdd = RDD([], name=name)

        self.assertEqual(name, rdd.name)


    # reduce

    def test_set_name_sets_name(self):
        rdd = RDD()

        self.assertIsNone(rdd.name)

        name = 'my_RDD'
        rdd.setName(name)

        self.assertEqual(name, rdd.name)

    def test_sum_empty_rdd_returns_zero(self):
        self.assertEqual(0, RDD([]).sum())

    def test_sum_non_empty_rdd_returns_sum(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(6, rdd.sum())

    def test_take_empty_rdd_returns_empty_list(self):
        self.assertEqual([], RDD([]).take(10))

    def test_take_short_rdd_returns_full_rdd(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(rdd.contents, rdd.take(10))

    def test_take_long_rdd_returns_full_rdd(self):
        rdd = RDD(list(range(20)))

        self.assertEqual(rdd.contents[:10], rdd.take(10))

    def test_union_of_empty_rdds_returns_empty_rdd(self):
        self.assertEqual([], RDD([]).union(RDD([])).collect())

    def test_union_of_rdds_returns_all_elements(self):
        x = RDD([1, 2, 3])
        y = RDD(['a', 'b', 'c', 'd'])

        self.assertEqual([1, 2, 3, 'a', 'b', 'c', 'd'], x.union(y).collect())

    def test_values_returns_value_of_kv_rdd(self):
        rdd = RDD([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertEqual(['v1', 'v2', 'v3'], rdd.values().collect())

    def test_values_returns_second_elem_of_non_tuple_rdd(self):
        rdd = RDD(['abc', 'def'])

        self.assertEqual(['b', 'e'], rdd.values().collect())

    # zip

    # zipWithIndex


if __name__ == '__main__':
    unittest.main()
