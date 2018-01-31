import unittest

import pyspark

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from bermann.row import Row
from bermann.spark_context import SparkContext
from bermann.sql import SQLContext


class TestDataFrame(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sc = SparkContext()
        cls.sql = SQLContext(cls.sc)

    def test_creation_from_list_of_dicts(self):
        df = self.sql.createDataFrame([
            {'a': 'a', 'b': 123},
            {'a': 'aa', 'b': 456}
        ])

        self.assertEqual(df.count(), 2)

    def test_creation_from_rdd_of_rows(self):
        rdd = self.sc.parallelize([
            Row(a='a', b=123),
            Row(a='aa', b=456)
        ])

        df = self.sql.createDataFrame(rdd)

        self.assertEqual(df.count(), 2)

    def test_creation_from_rdd_of_pyspark_rows(self):
        rdd = self.sc.parallelize([
            pyspark.sql.Row(a='a', b=123),
            pyspark.sql.Row(a='aa', b=456)
        ])

        df = self.sql.createDataFrame(rdd)

        self.assertEqual(df.count(), 2)

    def test_creation_from_rdd_of_tuples(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        self.assertEqual(df.count(), 2)

    def test_creation_from_rdd_of_tuples_no_schema_raises_exception(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        with self.assertRaises(Exception) as e:
            df = self.sql.createDataFrame(input)

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

        df = self.sql.createDataFrame(input, schema)

        df_2 = self.sql.createDataFrame(df)

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

        df = self.sql.createDataFrame(input, schema)

        self.assertEqual(df.schema, schema)

    def test_schema_returns_pyspark_schema(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        df_schema = df.schema

        self.assertEqual(schema, df_schema)

    #TODO agg

    #TODO alias

    #TODO approxQuantile

    def test_cache_is_noop(self):
        input = [
            ('a', 123),
            ('aa', 456)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        cached = df.cache()

        self.assertEqual(df, cached)

    #TODO checkpoint

    #TODO coalesce
    def test_coalesce_is_noop(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        self.assertEqual(df.collect(), df.coalesce(100).collect())

    def test_collect_returns_list_of_rows(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        self.assertEqual([Row(**{'a': 'aaa', 'b': 123})], df.collect())

    def test_columns_returns_column_list(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        self.assertEqual(['a', 'b'], df.columns())

    #TODO corr

    def test_count_returns_row_count(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        self.assertEqual(1, df.count())

    #TODO cov

    #TODO createGlobalTempView

    #TODO createOrReplaceGlobalTempView

    #TODO createOrReplaceTempView

    #TODO createTempView

    #TODO crossJoin

    #TODO crosstab

    #TODO cube

    #TODO describe

    #TODO distinct

    #TODO drop

    #TODO dropDuplicates

    #TODO drop_duplicates

    #TODO dropna

    #TODO explain

    #TODO fillna

    #TODO filter

    #TODO first

    #TODO foreach

    #TODO foreachPartition

    #TODO freqItems

    #TODO groupBy

    #TODO groupby

    #TODO head

    #TODO hint

    #TODO intersect

    #TODO isLocal

    #TODO isStreaming

    #TODO join

    #TODO limit

    #TODO na

    #TODO orderBy

    #TODO persist

    #TODO printSchema

    #TODO randomSplit

    #TODO rdd

    #TODO registerTempTable

    #TODO repartition

    #TODO replace

    #TODO rollup

    #TODO sample

    #TODO sampleBy

    def test_select_with_subset_returns_subset_of_cols(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        selected = df.select('a')

        self.assertEqual([Row(a='aaa')], selected.rdd.collect())

    def test_select_with_superset_returns_cols(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        selected = df.select('a', 'b', 'c')

        self.assertEqual([Row(a='aaa', b=123)], selected.rdd.collect())

    #TODO selectExpr

    #TODO show

    #TODO sort

    #TODO sortWithinPartitions

    #TODO stat

    #TODO storageLevel

    #TODO subtract

    #TODO take

    #TODO toDF

    #TODO toJSON

    #TODO toLocalIterator

    #TODO toPandas

    #TODO union

    #TODO unionAll

    #TODO unpersist

    #TODO where

    #TODO withColumn

    def test_withcolumnrenamed_renames_column_if_exists(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        renamed = df.withColumnRenamed('a', 'new_a')

        self.assertEqual([Row(new_a='aaa', b=123)], renamed.rdd.collect())

    def test_withcolumnrenamed_noop_if_no_such_column(self):
        input = [
            ('aaa', 123)
        ]

        schema = StructType([
            StructField('a', StringType()),
            StructField('b', IntegerType())
        ])

        df = self.sql.createDataFrame(input, schema)

        renamed = df.withColumnRenamed('c', 'new_c')

        self.assertEqual([Row(a='aaa', b=123)], renamed.rdd.collect())


    #TODO withWatermark

    #TODO write

    #TODO writeStream
