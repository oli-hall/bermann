import unittest

from bermann.spark_context import SparkContext
from py4j.protocol import Py4JJavaError
from operator import add


class TestRDD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext()

    def test_cache_is_noop(self):
        rdd = self.sc.parallelize([1, 2, 3])

        cached = rdd.cache()

        self.assertEqual(rdd, cached)

    def test_coalesce_is_noop(self):
        rdd = self.sc.parallelize([1, 2, 3])

        coalesced = rdd.coalesce(100)

        self.assertEqual(rdd, coalesced)

    def test_cogroup_returns_right_keys_with_joined_vals(self):
        y = self.sc.parallelize([('a', 21), ('b', 22), ('c', 2323)])

        x = self.sc.parallelize([('a', 1), ('b', 2), ('b', 3), ('d', 3242)])

        expected = [
            ('a', ([1], [21])),
            ('c', ([], [2323])),
            ('b', ([2, 3], [22])),
            ('d', ([3242], []))
        ]

        self.assertEqual(sorted(expected), x.cogroup(y).collect())

    def test_collect_empty_rdd_returns_empty_list(self):
        rdd = self.sc.emptyRDD()

        self.assertEqual([], rdd.collect())

    def test_collect_non_empty_rdd_returns_contents(self):
        input = [1, 2, 3]
        rdd = self.sc.parallelize(input)

        self.assertEqual(input, rdd.collect())

    def test_combinebykey_rdd_returns_combined_contents(self):
        input = [('a', 1), ('b', 1), ('a', 2)]
        rdd = self.sc.parallelize(input)

        def to_list(a):
            return [a]

        def append(a, b):
            a.append(b)
            return a

        def extend(a, b):
            a.extend(b)
            return a

        expected = [
            ('a', [1, 2]),
            ('b', [1])
        ]

        self.assertEqual(expected, rdd.combineByKey(to_list, append, extend).collect())

    def test_count_empty_rdd_returns_zero(self):
        rdd = self.sc.emptyRDD()

        self.assertEqual(0, rdd.count())

    def test_collect_non_empty_rdd_returns_length(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(3, rdd.count())

    def test_countbykey_empty_rdd_returns_empty_dict(self):
        rdd = self.sc.emptyRDD()

        self.assertEqual({}, rdd.countByKey())

    def test_countbykey_non_empty_rdd_returns_dict_of_counts(self):
        rdd = self.sc.parallelize([('a', 1), ('b', 2), ('a', 3)])

        self.assertEqual({'a': 2, 'b': 1}, rdd.countByKey())

    def test_countbyvalue_rdd_returns_dict_of_counts(self):
        rdd = self.sc.parallelize([1, 2, 3, 1, 2, 1])

        self.assertEqual({1: 3, 2: 2, 3: 1}, rdd.countByValue())

    def test_distinct_rdd_returns_unique_list(self):
        rdd = self.sc.parallelize([1, 2, 3, 1, 2, 1])

        self.assertEqual([1, 2, 3], rdd.distinct().collect())

    def test_distinct_on_unique_list_is_noop(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([1, 2, 3], rdd.distinct().collect())

    def test_filter_rdd_by_identity_returns_input(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([1, 2, 3], rdd.filter(lambda x: True).collect())

    def test_filter_rdd_returns_filters_by_input_func(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([2, 3], rdd.filter(lambda x: x > 1).collect())

    def test_first_empty_rdd_raises_valueerror(self):
        rdd = self.sc.emptyRDD()

        with self.assertRaises(ValueError) as e:
            rdd.first()
        self.assertEqual(ValueError, type(e.exception))

    def test_first_non_empty_rdd_returns_first_elem(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(1, rdd.first())

    def test_flatmap_on_rdd_with_identity_func_returns_rdd(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(rdd.collect(), rdd.flatMap(lambda x: [x]).collect())

    def test_flatmap_on_rdd_with_expanding_func_returns_rdd_of_expanded_elems(self):
        rdd = self.sc.parallelize(['a b c', 'd e f'])

        expected = ['a', 'b', 'c', 'd', 'e', 'f']
        actual = rdd.flatMap(lambda x: x.split()).collect()
        self.assertEqual(sorted(expected), sorted(actual))

    def test_flatmapvalues_on_rdd_with_identity_func_returns_rdd(self):
        rdd = self.sc.parallelize([('a', 1), ('b', 2), ('c', 3)])

        self.assertEqual(rdd.collect(), rdd.flatMapValues(lambda x: [x]).collect())

    def test_flatmapvalues_on_rdd_with_expanding_func_returns_rdd_of_expanded_elems(self):
        rdd = self.sc.parallelize([('a', 'a b c'), ('b', 'd e f')])

        expected = [('a', 'a'), ('a', 'b'), ('a', 'c'), ('b', 'd'), ('b', 'e'), ('b', 'f')]
        actual = rdd.flatMapValues(lambda x: x.split()).collect()
        self.assertEqual(sorted(expected), actual)

    def test_foreach_on_rdd_runs_function_but_doesnt_affect_rdd(self):
        items = []
        add_to_items = lambda x: items.append(x) or x * x

        rdd = self.sc.parallelize([1, 2, 3])

        rdd.foreach(add_to_items)

        self.assertEqual([1, 2, 3], items)
        self.assertEqual([1, 2, 3], rdd.collect())

    def test_fullouterjoin_returns_shared_keys_with_joined_vals_even_for_repeated_keys(self):
        x = self.sc.parallelize([('a', 1), ('b', 2), ('b', 3), ('d', 4)])
        y = self.sc.parallelize([('a', 21), ('a', 31), ('b', 22), ('c', 23)])

        expected = [
            ('a', (1, 21)),
            ('a', (1, 31)),
            ('b', (2, 22)),
            ('b', (3, 22)),
            ('c', (None, 23)),
            ('d', (4, None))
        ]
        actual = x.fullOuterJoin(y).collect()
        self.assertEqual(sorted(expected), actual)

    def test_groupby_on_rdd_returns_rdd_grouped_by_function(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([(0, [2]), (1, [1, 3])], rdd.groupBy(lambda x: x % 2).collect())

    def test_groupbykey_on_rdd_returns_rdd_grouped_by_key(self):
        rdd = self.sc.parallelize([('k1', 1), ('k1', 2), ('k2', 3)])

        self.assertEqual([('k1', [1, 2]), ('k2', [3])], rdd.groupByKey().collect())

    def test_isempty_returns_false_for_non_empty_rdd(self):
        rdd = self.sc.parallelize([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertFalse(rdd.isEmpty())

    def test_isempty_returns_true_for_empty_rdd(self):
        rdd = self.sc.emptyRDD()

        self.assertTrue(rdd.isEmpty())

    def test_keyby_returns_rdd_with_keys(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([('1', 1), ('2', 2), ('3', 3)], rdd.keyBy(lambda x: str(x)).collect())

    def test_join_returns_shared_keys_with_joined_vals(self):
        x = self.sc.parallelize([('a', 11), ('b', 12)])
        y = self.sc.parallelize([('b', 21), ('c', 22)])

        self.assertEqual([('b', (12, 21))], x.join(y).collect())

    def test_join_returns_shared_keys_with_joined_vals_even_for_repeated_keys(self):
        x = self.sc.parallelize([('a', 1), ('b', 2), ('b', 3), ('d', 4)])
        y = self.sc.parallelize([('a', 21), ('a', 31), ('b', 22), ('c', 23)])

        expected = [
            ('a', (1, 21)),
            ('a', (1, 31)),
            ('b', (2, 22)),
            ('b', (3, 22))
        ]
        self.assertEqual(expected, x.join(y).collect())

    def test_keys_returns_keys_of_kv_rdd(self):
        rdd = self.sc.parallelize([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertEqual(['k1', 'k1', 'k2'], rdd.keys().collect())

    def test_keys_returns_first_elem_of_non_tuple_rdd(self):
        rdd = self.sc.parallelize(['a', 'b', 'c'])

        self.assertEqual(['a', 'b', 'c'], rdd.keys().collect())

    def test_leftouterjoin_returns_left_keys_with_joined_vals(self):
        x = self.sc.parallelize([('a', 11), ('b', 12)])
        y = self.sc.parallelize([('b', 21), ('c', 22)])

        self.assertEqual([('a', (11, None)), ('b', (12, 21))], x.leftOuterJoin(y).collect())

    def test_leftouterjoin_returns_shared_keys_with_joined_vals_even_for_repeated_keys(self):
        x = self.sc.parallelize([('a', 1), ('b', 2), ('b', 3), ('d', 4)])
        y = self.sc.parallelize([('a', 21), ('a', 31), ('b', 22), ('c', 23)])

        expected = [
            ('a', (1, 21)),
            ('a', (1, 31)),
            ('b', (2, 22)),
            ('b', (3, 22)),
            ('d', (4, None))
        ]
        actual = x.leftOuterJoin(y).collect()
        self.assertEqual(sorted(expected), actual)

    def test_map_on_rdd_with_identity_func_returns_rdd(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(rdd.collect(), rdd.map(lambda x: x).collect())

    def test_map_on_rdd_with_func_returns_rdd_of_mapped_elems(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([1, 4, 9], rdd.map(lambda x: x * x).collect())

    def test_map_on_rdd_doesnt_affect_original_rdd(self):
        # uses dicts to test that references aren't copied
        input = [{'a': 1}, {'a': 2}, {'a': 3}]
        rdd = self.sc.parallelize(input)

        def square(rec):
            rec['a'] = rec['a'] ** 2
            return rec

        mapped = rdd.map(square)

        self.assertEqual([{'a': 1}, {'a': 4}, {'a': 9}], mapped.collect())
        self.assertEqual([{'a': 1}, {'a': 2}, {'a': 3}], rdd.collect())


    def test_map_partitions_maps_function_across_each_partition(self):
        rdd = self.sc.parallelize([1, 2, 3, 4, 5, 6])

        def f(iterable): yield sum(iterable)

        expected = [sum(p) for p in rdd.glom().collect()]
        self.assertEqual(sorted(expected), rdd.mapPartitions(f).collect())

    def test_mappartitionswithindex_maps_function_across_each_partition_with_partition_index(self):
        rdd = self.sc.parallelize([1, 2, 3, 4, 5, 6])

        def f(p_idx, iterable): yield p_idx, sum(iterable)

        expected = [(idx, sum(p)) for idx, p in enumerate(rdd.glom().collect())]
        self.assertEqual(sorted(expected), rdd.mapPartitionsWithIndex(f).collect())

    def test_mapvalues_on_rdd_with_func_returns_rdd_of_mapped_elems(self):
        rdd = self.sc.parallelize([('a', 1), ('b', 2), ('c', 3)])

        self.assertEqual([('a', 1), ('b', 4), ('c', 9)], rdd.mapValues(lambda x: x * x).collect())

    def test_mapvalues_on_rdd_of_3tuples_returns_2tuple_rdd(self):
        rdd = self.sc.parallelize([('a', 1, 'v1'), ('b', 2, 'v2'), ('c', 3, 'v3')])

        self.assertEqual([('a', 1), ('b', 2), ('c', 3)], rdd.mapValues(lambda x: x).collect())

    def test_max_rdd_returns_max_value(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(3, rdd.max())

    def test_min_rdd_returns_min_value(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(1, rdd.min())

    def test_get_name_sets_name(self):
        name = 'my RDD'
        rdd = self.sc.emptyRDD()
        rdd.setName(name)

        self.assertEqual(name, rdd.name)

    def test_filter_on_rdd_with_identity_func_returns_rdd(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(rdd.collect(), rdd.filter(lambda x: True).collect())

    def test_filter_on_rdd_with_func_returns_filtered_rdd(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual([2, 3], rdd.filter(lambda x: x > 1).collect())

    def test_partitionby_updates_partitions(self):
        rdd = self.sc.parallelize([1, 2, 3, 4])

        self.assertEqual(4, rdd.getNumPartitions())

        repartitioned = rdd.partitionBy(6)

        self.assertEqual(6, repartitioned.getNumPartitions())

    def test_reduce_on_rdd_returns_rdd_reduced_to_single_elem(self):
        rdd = self.sc.parallelize([0, 1, 2, 3])

        self.assertEqual(6, rdd.reduce(add))

    def test_reducebykey_on_rdd_returns_rdd_reduced_by_key(self):
        rdd = self.sc.parallelize([('k1', 1), ('k1', 2), ('k2', 3)])

        expected = [('k2', 3), ('k1', 3)]
        actual = rdd.reduceByKey(add).collect()
        self.assertEqual(sorted(expected), actual)

    def test_repartition_updates_partitions(self):
        rdd = self.sc.parallelize([1, 2, 3, 4])

        self.assertEqual(4, rdd.getNumPartitions())

        repartitioned = rdd.repartition(6)

        self.assertEqual(6, repartitioned.getNumPartitions())

    def test_rightouterjoin_returns_right_keys_with_joined_vals(self):
        x = self.sc.parallelize([('a', 11), ('b', 12)])
        y = self.sc.parallelize([('b', 21), ('c', 22)])

        self.assertEqual([('b', (12, 21)), ('c', (None, 22))], x.rightOuterJoin(y).collect())

    def test_rightouterjoin_returns_shared_keys_with_joined_vals_even_for_repeated_keys(self):
        x = self.sc.parallelize([('a', 1), ('b', 2), ('b', 3), ('d', 4)])
        y = self.sc.parallelize([('a', 21), ('a', 31), ('b', 22), ('c', 23)])

        expected = [
            ('a', (1, 21)),
            ('a', (1, 31)),
            ('c', (None, 23)),
            ('b', (2, 22)),
            ('b', (3, 22))
        ]
        # TODO tmp fix until python version issues sorted
        actual = x.rightOuterJoin(y).collect()
        self.assertEqual(len(expected), len(actual))
        for exp in expected:
            self.assertTrue(exp in actual)

    def test_set_name_sets_name(self):
        rdd = self.sc.emptyRDD()

        self.assertIsNone(rdd.name)

        name = 'my_RDD'
        rdd.setName(name)

        self.assertEqual(name, rdd.name)

    def test_sortbykey_returns_rdd_sorted_by_key(self):
        rdd = self.sc.parallelize([
            (1, 1),
            (4, 4),
            (3, 3),
            (1, 1)
        ])

        expected = [
            (1, 1),
            (1, 1),
            (3, 3),
            (4, 4)
        ]
        actual = [i for p in rdd.sortByKey().partitions for i in p]
        self.assertEqual(expected, actual)

    def test_sortbykey_descending_returns_rdd_sorted_by_key_reversed(self):
        rdd = self.sc.parallelize([
            (1, 1),
            (4, 4),
            (3, 3),
            (1, 1)
        ])

        expected = [
            (4, 4),
            (3, 3),
            (1, 1),
            (1, 1)
        ]

        actual = [i for p in rdd.sortByKey(ascending=False).partitions for i in p]
        self.assertEqual(expected, actual)

    def test_sortby_returns_rdd_sorted_by_keyfunc(self):
        rdd = self.sc.parallelize([1, 4, 3, 1])

        expected = [1, 1, 3, 4]
        actual = [i for p in rdd.sortBy(lambda x: x).partitions for i in p]
        self.assertEqual(expected, actual)

    def test_sortby_descending_returns_rdd_in_reverse_order(self):
        rdd = self.sc.parallelize([1, 4, 3, 1])

        expected = [4, 3, 1, 1]

        actual = [i for p in rdd.sortBy(lambda x: x, ascending=False).partitions for i in p]
        self.assertEqual(expected, actual)

    def test_subtractbykey_on_rdd_returns_keys_not_in_other_rdd(self):
        rdd = self.sc.parallelize([('k1', 1), ('k2', 2), ('k3', 3)])
        other = self.sc.parallelize([('k2', 5), ('k4', 7)])

        self.assertEqual([('k1', 1), ('k3', 3)], rdd.subtractByKey(other).collect())

    def test_sum_empty_rdd_returns_zero(self):
        self.assertEqual(0, self.sc.emptyRDD().sum())

    def test_sum_non_empty_rdd_returns_sum(self):
        rdd = self.sc.parallelize([1, 2, 3])

        self.assertEqual(6, rdd.sum())

    def test_take_empty_rdd_returns_empty_list(self):
        self.assertEqual([], self.sc.emptyRDD().take(10))

    def test_take_short_rdd_returns_full_rdd(self):
        input = [1, 2, 3]
        rdd = self.sc.parallelize(input)

        self.assertEqual(input, rdd.take(10))

    def test_take_long_rdd_returns_subset(self):
        rdd = self.sc.parallelize(list(range(20)))

        self.assertTrue(len(rdd.take(10)))

    def test_takesample_small_rdd_returns_all_if_not_with_replacement(self):
        rdd = self.sc.parallelize(list(range(20)))


        expected = list(range(20))
        self.assertEqual(sorted(expected), rdd.takeSample(False, 30))

    def test_takesample_small_rdd_returns_sample(self):
        input = list(range(20))
        rdd = self.sc.parallelize(input)

        sample = rdd.takeSample(True, 30)
        self.assertEqual(30, len(sample))
        for s in sample:
            self.assertTrue(s in input)

    def test_takesample_small_sample_returns_sample(self):
        input = list(range(20))
        rdd = self.sc.parallelize(input)

        sample = rdd.takeSample(True, 10)
        self.assertEqual(10, len(sample))
        for s in sample:
            self.assertTrue(s in input)

    def test_union_of_empty_rdds_returns_empty_rdd(self):
        self.assertEqual([], self.sc.emptyRDD().union(self.sc.emptyRDD()).collect())

    def test_union_of_rdds_returns_all_elements(self):
        x = self.sc.parallelize([1, 2, 3])
        y = self.sc.parallelize(['a', 'b', 'c', 'd'])

        expected = [1, 2, 3, 'a', 'b', 'c', 'd']
        actual = x.union(y).collect()

        self.assertEqual(sorted(expected), actual)

    def test_unpersist_is_noop(self):
        rdd = self.sc.parallelize([1, 2, 3])

        unpersisted = rdd.unpersist()

        self.assertEqual(rdd, unpersisted)

    def test_values_returns_value_of_kv_rdd(self):
        rdd = self.sc.parallelize([('k1', 'v1'), ('k1', 'v2'), ('k2', 'v3')])

        self.assertEqual(['v1', 'v2', 'v3'], rdd.values().collect())

    def test_values_returns_second_elem_of_non_tuple_rdd(self):
        rdd = self.sc.parallelize(['abc', 'def'])

        self.assertEqual(['b', 'e'], rdd.values().collect())

    def test_zip_of_rdds_returns_zipped_rdd(self):
        x = self.sc.parallelize([1, 2, 3])
        y = self.sc.parallelize(['a', 'b', 'c'])

        self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], x.zip(y).collect())

    def test_zip_of_differing_length_rdds_raises_exception(self):
        x = self.sc.parallelize([1, 2, 3])
        y = self.sc.parallelize([1, 2, 3, 4])

        with self.assertRaises(Py4JJavaError) as e:
            x.zip(y)
        self.assertEqual(Py4JJavaError, type(e.exception))

    def test_zipwithindex_of_rdds_returns_content_zipped_with_index(self):
        x = self.sc.parallelize(['a', 'b', 'c'])

        self.assertEqual([('a', 0), ('b', 1), ('c', 2)], x.zipWithIndex().collect())

    def test_zipwithuniqueid_of_rdds_returns_content_zipped_with_id(self):
        x = self.sc.parallelize(['a', 'b', 'c', 'd', 'e', 'f'])

        expected = [
            ('a', 0),
            ('b', 4),
            ('c', 1),
            ('d', 5),
            ('e', 2),
            ('f', 3)
        ]
        self.assertEqual(expected, x.zipWithUniqueId().collect())

    def test_equality_of_rdd_with_non_rdd_fails(self):
        x = self.sc.parallelize(['a', 'b', 'c'])

        self.assertNotEqual(123, x)
        self.assertNotEqual('abc', x)

    def test_equality_of_rdd_with_non_equal_rdd_fails(self):
        x = self.sc.parallelize(['a', 'b', 'c'])
        y = self.sc.parallelize(['d', 'e', 'f'])
        z = self.sc.parallelize(['a', 'b', 'c'])
        z.setName("Z")

        self.assertNotEqual(x, y)
        self.assertNotEqual(x, z)

    def test_equality_of_rdd_with_equal_rdd_suceeds(self):
        x = self.sc.parallelize(['a', 'b', 'c'])
        y = self.sc.parallelize(['a', 'b', 'c'])

        self.assertEqual(x, y)


if __name__ == '__main__':
    unittest.main()
