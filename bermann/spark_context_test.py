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

    def test_empty_rdd_returns_empty_rdd(self):
        sc = SparkContext()

        empty = sc.emptyRDD()
        self.assertTrue(isinstance(empty, bermann.rdd.RDD))
        self.assertEqual(0, empty.count())
