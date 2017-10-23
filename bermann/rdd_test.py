import unittest

from bermann import RDD


class TestRDD(unittest.TestCase):

    def test_cache_is_noop(self):
        rdd = RDD([1, 2, 3])

        cached = rdd.cache()

        self.assertEqual(rdd, cached)

    # collect

    # count

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
