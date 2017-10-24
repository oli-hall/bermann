import unittest

from bermann import RDD
from py4j.protocol import Py4JJavaError
from operator import add


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

    def test_flatmap_on_rdd_with_identity_func_returns_rdd(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(rdd.collect(), rdd.flatMap(lambda x: [x]).collect())

    def test_flatmap_on_rdd_with_expanding_func_returns_rdd_of_expanded_elems(self):
        rdd = RDD(['a b c', 'd e f'])

        self.assertEqual(['a', 'b', 'c', 'd', 'e', 'f'], rdd.flatMap(lambda x: x.split()).collect())

    def test_flatmapvalues_on_rdd_with_identity_func_returns_rdd(self):
        rdd = RDD([('a', 1), ('b', 2), ('c', 3)])

        self.assertEqual(rdd.collect(), rdd.flatMapValues(lambda x: [x]).collect())

    def test_flatmapvalues_on_rdd_with_expanding_func_returns_rdd_of_expanded_elems(self):
        rdd = RDD([('a', 'a b c'), ('b', 'd e f')])

        expected = [('a', 'a'), ('a', 'b'), ('a', 'c'), ('b', 'd'), ('b', 'e'), ('b', 'f')]
        self.assertEqual(expected, rdd.flatMapValues(lambda x: x.split()).collect())

    def test_foreach_on_rdd_runs_function_but_doesnt_affect_rdd(self):
        items = []
        add_to_items = lambda x: items.append(x) or x * x

        rdd = RDD([1, 2, 3])

        rdd.foreach(add_to_items)

        self.assertEqual([1, 2, 3], items)
        self.assertEqual([1, 2, 3], rdd.collect())

    def test_groupby_on_rdd_returns_rdd_grouped_by_function(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([(0, [2]), (1, [1, 3])], rdd.groupBy(lambda x: x % 2).collect())

    def test_groupbykey_on_rdd_returns_rdd_grouped_by_key(self):
        rdd = RDD([('k1', 1), ('k1', 2), ('k2', 3)])

        self.assertEqual([('k2', [3]), ('k1', [1, 2])], rdd.groupByKey().collect())

    def test_isempty_returns_false_for_non_empty_rdd(self):
        rdd = RDD([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertFalse(rdd.isEmpty())

    def test_isempty_returns_true_for_empty_rdd(self):
        rdd = RDD()

        self.assertTrue(rdd.isEmpty())

    def test_keyby_returns_rdd_with_keys(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([('1', 1), ('2', 2), ('3', 3)], rdd.keyBy(lambda x: str(x)).collect())

    def test_join_returns_shared_keys_with_joined_vals(self):
        x = RDD([('a', 11), ('b', 12)])
        y = RDD([('b', 21), ('c', 22)])

        self.assertEqual([('b', (12, 21))], x.join(y).collect())

    def test_keys_returns_keys_of_kv_rdd(self):
        rdd = RDD([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertEqual(['k1', 'k1', 'k2'], rdd.keys().collect())

    def test_keys_returns_first_elem_of_non_tuple_rdd(self):
        rdd = RDD(['a', 'b', 'c'])

        self.assertEqual(['a', 'b', 'c'], rdd.keys().collect())

    def test_map_on_rdd_with_identity_func_returns_rdd(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(rdd.collect(), rdd.map(lambda x: x).collect())

    def test_map_on_rdd_with_func_returns_rdd_of_mapped_elems(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([1, 4, 9], rdd.map(lambda x: x * x).collect())

    def test_mapvalues_on_rdd_with_func_returns_rdd_of_mapped_elems(self):
        rdd = RDD([('a', 1), ('b', 2), ('c', 3)])

        self.assertEqual([('a', 1), ('b', 4), ('c', 9)], rdd.mapValues(lambda x: x * x).collect())

    def test_mapvalues_on_rdd_of_3tuples_returns_2tuple_rdd(self):
        rdd = RDD([('a', 1, 'v1'), ('b', 2, 'v2'), ('c', 3, 'v3')])

        self.assertEqual([('a', 1), ('b', 2), ('c', 3)], rdd.mapValues(lambda x: x).collect())

    def test_max_rdd_returns_max_value(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(3, rdd.max())

    def test_min_rdd_returns_min_value(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(1, rdd.min())

    def test_get_name_sets_name(self):
        name = 'my RDD'
        rdd = RDD([], name=name)

        self.assertEqual(name, rdd.name)

    def test_filter_on_rdd_with_identity_func_returns_rdd(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual(rdd.collect(), rdd.filter(lambda x: True).collect())

    def test_filter_on_rdd_with_func_returns_filtered_rdd(self):
        rdd = RDD([1, 2, 3])

        self.assertEqual([2, 3], rdd.filter(lambda x: x > 1).collect())

    def test_reducebykey_on_rdd_returns_rdd_reduced_by_key(self):
        rdd = RDD([('k1', 1), ('k1', 2), ('k2', 3)])

        self.assertEqual([('k2', 3), ('k1', 3)], rdd.reduceByKey(add).collect())

    def test_set_name_sets_name(self):
        rdd = RDD()

        self.assertIsNone(rdd.name)

        name = 'my_RDD'
        rdd.setName(name)

        self.assertEqual(name, rdd.name)

    def test_subtractbykey_on_rdd_returns_keys_not_in_other_rdd(self):
        rdd = RDD([('k1', 1), ('k2', 2), ('k3', 3)])
        other = RDD([('k2', 5), ('k4', 7)])

        self.assertEqual([('k1', 1), ('k3', 3)], rdd.subtractByKey(other).collect())

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

    def test_zip_of_rdds_returns_zipped_rdd(self):
        x = RDD([1, 2, 3])
        y = RDD(['a', 'b', 'c'])

        self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], x.zip(y).collect())

    def test_zip_of_differing_length_rdds_raises_exception(self):
        x = RDD([1, 2, 3])
        y = RDD([1, 2, 3, 4])

        with self.assertRaises(Py4JJavaError) as e:
            x.zip(y)
        self.assertEqual(Py4JJavaError, type(e.exception))

    def test_zipwithindex_of_rdds_returns_content_zipped_with_index(self):
        x = RDD(['a', 'b', 'c'])

        self.assertEqual([('a', 0), ('b', 1), ('c', 2)], x.zipWithIndex().collect())


if __name__ == '__main__':
    unittest.main()
