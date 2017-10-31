from pyspark.sql.types import StructType
from pyspark.storagelevel import StorageLevel

from bermann.rdd import RDD
from bermann.row import Row


class DataFrame(object):

    # TODO it would be good to accept RDDs of PySpark Rows as well as bermann Rows
    def __init__(self, input=[], schema=None):
        """
        Creates a Bermann DataFrame object, given some input, specified
        as dicts of col_name -> value, and a schema of col_name -> type.

        :param input: list of dicts of column_name -> value
        :param _parsed_schema: a StructType definition of the DataFrame's schema
        """
        if isinstance(input, list):
            self.rows, self.schema = self._parse_from_list(input, schema)
        elif isinstance(input, RDD):
            # TODO deal with RDDs of other types than Row
            self.rows = input.rows
            self.schema = schema
        elif isinstance(input, DataFrame):
            self.rows = input.rows
            self.schema = input.schema
        else:
            raise Exception("input should be of type list, RDD, or DataFrame")

    def _parse_from_list(self, input_rows, schema=None):
        # if schema:
        #     parsed_schema = self._parse_schema(schema)
        # else:
        #     first = input_rows[0]
        #     if not first:
        #         raise Exception('Cannot parse schema from blank data')
        #     if isinstance(first, dict):
        #         parsed_schema = self._infer_python_schema_from_dict(first)
        #     elif isinstance(first, Row):
        #         parsed_schema = self._infer_python_schema_from_row(first)
        #     else:
        #         raise Exception('Schema can only be parsed from dict or Row')

        rows = []

        for r in input_rows:
            if isinstance(r, dict):
                # assert len(r) == len(parsed_schema)
                # assert sorted(r.keys()) == sorted(parsed_schema.keys())
                # # TODO validate input types against schema?

                rows.append(Row(**r))
            elif isinstance(r, list) or isinstance(r, tuple):
                if not schema:
                    raise Exception("Schema required when creating DataFrame from list of list/tuple")
                # assert len(r) == len(parsed_schema)
                # # TODO validate input types against schema?
                # TODO this won't deal with nested Rows
                keys = [t.name for t in schema.fields]
                inputs = {}
                for idx, k in enumerate(keys):
                    inputs[k] = r[idx]
                rows.append(Row(**inputs))
            else:
                raise Exception("input rows must of type dict, list or tuple")

        return rows, schema

    def _parse_schema(self, schema):
        """
        Convert a Spark schema (StructType) to a dict
        of key -> Spark datatype

        :param schema: spark schema (StructType)
        :return: dict of key: value
        """
        parsed = {}
        for t in schema.fields:
            if isinstance(t.dataType, StructType):
                parsed[t.name] = self._parse_schema(t.dataType)
            else:
                parsed[t.name] = t.dataType
        return parsed

    def _infer_python_schema_from_dict(self, row):
        assert isinstance(row, dict)
        # TODO this is useless. it does nothing
        tmp = {}
        for k, v in row.items():
            tmp[k] = type(v)
        return tmp

    def _infer_python_schema_from_row(self, row):
        assert isinstance(row, Row)
        tmp = {}
        for k, v in row.fields():
            tmp[k] = type(v)
        return tmp

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
        raise NotImplementedError()

    def collect(self):
        raise NotImplementedError()

    def columns(self):
        return self.schema.keys()

    def corr(self, col1, col2, method=None):
        raise NotImplementedError()

    def count(self):
        return len(self.rows)

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
        raise NotImplementedError()

    def first(self):
        raise NotImplementedError()

    def foreach(self, f):
        raise NotImplementedError()

    def foreachPartition(self, f):
        raise NotImplementedError()

    def freqItems(self, cols, support=None):
        raise NotImplementedError()

    def groupBy(self, *cols):
        raise NotImplementedError()

    def groupby(self, *cols):
        raise NotImplementedError()

    def head(self, n=None):
        raise NotImplementedError()

    def hint(self, name, *parameters):
        raise NotImplementedError()

    def intersect(self, other):
        raise NotImplementedError()

    def isLocal(self, ):
        raise NotImplementedError()

    def isStreaming(self, ):
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

    def printSchema(self, ):
        raise NotImplementedError()

    def randomSplit(self, weights, seed=None):
        raise NotImplementedError()

    def rdd(self):
        return RDD([r.values() for r in self.rows])

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

    # TODO this is an attribute, not a method
    def schema(self):
        raise NotImplementedError()

    def select(self, *cols):
        return DataFrame(
            input=[self._select(r, cols) for r in self.rows],
            schema=self._select(self.schema, cols)
        )

    @staticmethod
    def _select(input, *cols):
        return {c: t for c, t in input if c in cols}

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

    def toLocalIterator(self, ):
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
        raise NotImplementedError()

    def withColumn(self, colName, col):
        raise NotImplementedError()

    def withColumnRenamed(self, existing, new):
        if existing in self.schema.keys():
            self.schema[new] = self.schema[existing]
            del self.schema[existing]

            for r in self.rows:
                r[new] = r[existing]
                del r[existing]

        return self

    def withWatermark(self, eventTime, delayThreshold):
        raise NotImplementedError()

    def write(self):
        raise NotImplementedError()

    def writeStream(self):
        raise NotImplementedError()
