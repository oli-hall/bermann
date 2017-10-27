import unittest

from bermann.row import Row
from bermann.rdd import RDD
from bermann.dataframe import DataFrame


class TestDataFrame(unittest.TestCase):

    def test_creation_from_list_of_dicts(self):
        df = DataFrame([
            {'a': 'a', 'b': 123},
            {'a': 'aa', 'b': 456}
        ])

        self.assertEqual(df.count(), 2)

    def test_creation_from_rdd_of_rows(self):
        rdd = RDD(
            Row(a='a', b=123),
            Row(a='aa', b=456)
        )

        df = DataFrame(rdd)

        self.assertEqual(df.count(), 2)
