import unittest

from bermann.spark_context import SparkContext
import bermann.rdd

class TestSparkContext(unittest.TestCase):

    def test_parallelize_with_list_input(self):
        sc = SparkContext()

        self.assertEqual([1, 2, 3], sc.parallelize([1, 2, 3]).collect())

    def test_parallelize_with_generator_input(self):
        sc = SparkContext()

        def gen_range(i):
            for i in range(i):
                yield i

        self.assertEqual([0, 1, 2, 3], sc.parallelize(gen_range(4)).collect())

    def test_parallelize_with_slices_sets_num_partitions(self):
        sc = SparkContext()

        rdd = sc.parallelize([1, 2, 3], 6)

        self.assertEqual(6, rdd.numPartitions)

    def test_empty_rdd_returns_empty_rdd(self):
        sc = SparkContext()

        empty = sc.emptyRDD()
        self.assertTrue(isinstance(empty, bermann.rdd.RDD))
        self.assertEqual(0, empty.count())

    def test_union_empty_list_raises_value_error(self):
        sc = SparkContext()

        with self.assertRaises(ValueError) as e:
            sc.union([])
        self.assertEqual(ValueError, type(e.exception))

    def test_union_single_rdd_list_returns_rdd(self):
        sc = SparkContext()

        rdd = sc.parallelize([1, 2, 3])

        self.assertEqual(rdd.collect(), sc.union([rdd]).collect())

    def test_union_multiple_rdds_returns_contents_unioned_as_new_rdd(self):
        sc = SparkContext()

        x = sc.parallelize([1, 2, 3])
        y = sc.parallelize([4, 5, 6])

        expected = [1, 2, 3, 4, 5, 6]
        self.assertEqual(sorted(expected), sc.union([x, y]).collect())


