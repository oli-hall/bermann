import pyspark
from pyspark.sql import types
from pyspark.sql.types import StructField, StructType
from pyspark.storagelevel import StorageLevel

from bermann.rdd import RDD
from bermann.row import Row


class DataFrame(object):

    @staticmethod
    def create(data, sc, schema=None):
        if isinstance(data, list):
            return DataFrame._from_list(data, sc, schema)
        elif isinstance(data, RDD):
            return DataFrame._from_rdd(data, sc, schema)
        elif isinstance(data, DataFrame):
            return DataFrame._from_dataframe(data, sc)
        raise Exception("Unsupported data type {}, should be list, RDD or DataFrame".format(type(data)))

    @staticmethod
    def _from_list(input_lst, sc, schema=None):
        rdd = RDD.from_list(input_lst, sc)
        return DataFrame._from_rdd(rdd, sc, schema=schema)

    @staticmethod
    def _from_rdd(rdd, sc, schema=None):
        if schema:
            if isinstance(schema, StructType):
                first = rdd.first()
                if isinstance(first, dict):
                    return DataFrame(rdd.map(lambda i: Row(**i)), schema=schema)
                elif isinstance(first, Row):
                    return DataFrame(rdd, schema=schema)
                else:
                    cols = [f.name for f in schema.fields]
                    return DataFrame(rdd.map(lambda i: Row(**dict(zip(cols, i)))), schema=schema)

            elif isinstance(schema, list):
                # TODO check type of schema items
                # TODO use more than first row to infer types
                first = rdd.first()
                if isinstance(first, dict):
                    assert sorted(first.keys()) == sorted(schema)
                    return DataFrame(rdd.map(lambda i: Row(**i)), schema=types._infer_schema(first))

                # TODO namedtuple

                elif isinstance(first, Row):
                    assert sorted(first.fields.keys()) == sorted(schema)
                    return DataFrame(rdd, schema=types._infer_schema(first.asDict()))

                elif isinstance(first, pyspark.sql.Row):
                    assert sorted(first.asDict().keys()) == sorted(schema)
                    return DataFrame(
                        rdd.map(lambda r: Row(**r.asDict())),
                        schema=types._infer_schema(first.asDict())
                    )

                elif isinstance(first, basestring) or isinstance(first, int):
                    raise Exception("Unexpected datatype encountered - should be Row, dict, list or tuple")

                else:
                    # data should be list/tuple
                    assert len(schema) == len(first)

                    return DataFrame(
                        rdd.map(lambda i: Row(**dict(zip(schema, i)))),
                        schema=types._infer_schema(dict(zip(schema, first)))
                    )
            else:
                raise Exception("Schema must either be PySpark StructType or list of column names")
        else:
            # parse schema (column names and types) from data, which should be
            # RDD of Row, namedtuple, or dict
            first = rdd.first()

            if isinstance(first, dict):
                return DataFrame(rdd.map(lambda i: Row(**i)), schema=types._infer_schema(first))

            # TODO namedtuple

            elif isinstance(first, Row):
                return DataFrame(rdd, schema=types._infer_schema(first.asDict()))
            elif isinstance(first, pyspark.sql.Row):
                return DataFrame(rdd.map(lambda r: Row(**r.asDict())), schema=types._infer_schema(first.asDict()))
            else:
                raise Exception("Unexpected datatype encountered - should be Row or dict if no schema provided")

    @staticmethod
    def _from_dataframe(df, sc):
        return DataFrame(df.rdd, schema=df.schema)

    def __init__(self, rdd=None, schema=None):
        """
        Creates a Bermann DataFrame object, given some input, specified
        as an RDD of Rows, and a Spark schema

        :param rdd: RDD of Rows
        :param schema: StrucType, a PySpark DataFrame schema
        """
        assert rdd
        assert schema

        self.rdd = rdd
        self._schema = schema

    def __getattr__(self, item):
        try:
            return self.schema[item]
        except KeyError:
            raise AttributeError(item)

    @property
    def schema(self):
        return self._schema

    def agg(self, *exprs):
        raise NotImplementedError()

    def alias(self, alias):
        raise NotImplementedError()

    def approxQuantile(self, col, probabilities, relativeError):
        raise NotImplementedError()

    def cache(self):
        return self

    def checkpoint(self, eager=True):
        raise NotImplementedError()

    def coalesce(self, numPartitions):
        return self

    def collect(self):
        return self.rdd.collect()

    def columns(self):
        return [c.name for c in self._schema.fields]

    def corr(self, col1, col2, method=None):
        raise NotImplementedError()

    def count(self):
        return self.rdd.count()

    def cov(self, col1, col2):
        raise NotImplementedError()

    def createGlobalTempView(self, name):
        raise NotImplementedError()

    def createOrReplaceGlobalTempView(self, name):
        raise NotImplementedError()

    def createOrReplaceTempView(self, name):
        raise NotImplementedError()

    def createTempView(self, name):
        raise NotImplementedError()

    def crossJoin(self, other):
        raise NotImplementedError()

    def crosstab(self, col1, col2):
        raise NotImplementedError()

    def cube(self, *cols):
        raise NotImplementedError()

    def describe(self, *cols):
        raise NotImplementedError()

    def distinct(self):
        raise NotImplementedError()

    def drop(self, *cols):
        raise NotImplementedError()

    def dropDuplicates(self, subset=None):
        raise NotImplementedError()

    def drop_duplicates(self, subset=None):
        raise NotImplementedError()

    def dropna(self, how='any', thresh=None, subset=None):
        raise NotImplementedError()

    def explain(self, extended=False):
        raise NotImplementedError()

    def fillna(self, value, subset=None):
        raise NotImplementedError()

    def filter(self, condition):
        # TODO parse condition
        # TODO
        raise NotImplementedError()

    def first(self):
        return self.rdd.first()

    def foreach(self, f):
        raise NotImplementedError()

    def foreachPartition(self, f):
        raise NotImplementedError()

    def freqItems(self, cols, support=None):
        raise NotImplementedError()

    def groupBy(self, *cols):
        raise NotImplementedError()

    def groupby(self, *cols):
        return self.groupBy(*cols)

    def head(self, n=None):
        raise NotImplementedError()

    def hint(self, name, *parameters):
        raise NotImplementedError()

    def intersect(self, other):
        raise NotImplementedError()

    def isLocal(self):
        raise NotImplementedError()

    def isStreaming(self):
        raise NotImplementedError()

    def join(self, other, on=None, how=None):
        raise NotImplementedError()

    def limit(self, num):
        raise NotImplementedError()

    def na(self):
        raise NotImplementedError()

    def orderBy(self, *cols, **kwargs):
        raise NotImplementedError()

    def persist(self, storageLevel=StorageLevel(True, True, False, False, 1)):
        raise NotImplementedError()

    def printSchema(self):
        raise NotImplementedError()

    def randomSplit(self, weights, seed=None):
        raise NotImplementedError()

    def rdd(self):
        return self.rdd

    def registerTempTable(self, name):
        raise NotImplementedError()

    def repartition(self, numPartitions, *cols):
        raise NotImplementedError()

    def replace(self, to_replace, value=None, subset=None):
        raise NotImplementedError()

    def rollup(self, *cols):
        raise NotImplementedError()

    def sample(self, withReplacement, fraction, seed=None):
        raise NotImplementedError()

    def sampleBy(self, col, fractions, seed=None):
        raise NotImplementedError()

    def select(self, *cols):
        return DataFrame(
            self.rdd.map(lambda r: Row(**{c: t for c, t in r.asDict().items() if c in cols})),
            schema=StructType([f for f in self.schema.fields if f.name in cols])
        )

    def selectExpr(self, *expr):
        raise NotImplementedError()

    def show(self, n=20, truncate=True):
        raise NotImplementedError()

    def sort(self, *cols, **kwargs):
        raise NotImplementedError()

    def sortWithinPartitions(self, *cols, **kwargs):
        raise NotImplementedError()

    def stat(self):
        raise NotImplementedError()

    def storageLevel(self, ):
        raise NotImplementedError()

    def subtract(self, other):
        raise NotImplementedError()

    def take(self, num):
        raise NotImplementedError()

    def toDF(self, *cols):
        return self.select(cols)

    def toJSON(self, use_unicode=True):
        raise NotImplementedError()

    def toLocalIterator(self):
        raise NotImplementedError()

    def toPandas(self):
        raise NotImplementedError()

    def union(self, other):
        raise NotImplementedError()

    def unionAll(self, other):
        raise NotImplementedError()

    def unpersist(self, blocking=False):
        raise NotImplementedError()

    def where(self, condition):
        return self.filter(condition)

    def withColumn(self, colName, col):
        raise NotImplementedError()

    def withColumnRenamed(self, existing, new):
        if existing in self.columns():
            cols = []
            for c in self.schema.fields:
                if c.name == existing:
                    cols.append(StructField(new, c.dataType, c.nullable))
                else:
                    cols.append(c)

            schema = StructType(cols)

            return DataFrame(
                self.rdd.map(
                    lambda r: Row(**DataFrame._rename_dict_key(r.fields, existing, new))
                ),
                schema=schema
            )

        return self

    @staticmethod
    def _rename_dict_key(input, existing, new):
        output = {}
        for k, v in input.items():
            if k == existing:
                output[new] = v
            else:
                output[k] = v
        return output

    def withWatermark(self, eventTime, delayThreshold):
        raise NotImplementedError()

    def write(self):
        raise NotImplementedError()

    def writeStream(self):
        raise NotImplementedError()
