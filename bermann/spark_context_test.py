import unittest

from bermann.spark_context import SparkContext


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
