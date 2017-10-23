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

    # countByKey

    # conntByValue

    # distinct

    # filter

    # first

    # flatMap

    # flatMapValues

    # foreach

    # groupBy

    # groupByKey

    # isEmpty

    # keyBy

    # keys

    # map

    # mapValues

    # max

    # min

    # name


if __name__ == '__main__':
    unittest.main()
