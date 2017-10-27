import unittest

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from bermann.dataframe import DataFrame
from bermann.rdd import RDD
from bermann.row import Row


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

    # TODO test tuples with no schema
    def test_creation_from_rdd_of_tuples(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = DataFrame(input, schema)

        self.assertEqual(df.count(), 2)

    def test_creation_from_rdd_of_tuples_no_schema_raises_exception(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        with self.assertRaises(Exception) as e:
            df = DataFrame(input)
        self.assertEqual(Exception, type(e.exception))

    def test_creation_from_dataframe(self):
        input = [
            {'a': 'a', 'b': 123},
            {'a': 'aa', 'b': 456}
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = DataFrame(input, schema)

        df_2 = DataFrame(df)

        self.assertEqual(df_2.count(), 2)

    def test_schema_attr_returns_pyspark_schema(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = DataFrame(input, schema)

        self.assertEqual(df.schema, schema)
