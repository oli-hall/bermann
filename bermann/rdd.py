from collections import defaultdict
from functools import reduce

from py4j.protocol import Py4JJavaError
from pyspark.rdd import portable_hash
from pyspark.storagelevel import StorageLevel

import bermann.dataframe
import bermann.spark_context


class RDD(object):

    # TODO support creation from pre-partitioned rows
    # would prevent recreation of the partitioning every time
    def __init__(self, rows=[], sc=None, numPartitions=None):
        assert isinstance(sc, bermann.spark_context.SparkContext)

        self.name = None
        self.sc = sc
        if numPartitions:
           self.numPartitions = numPartitions
        else:
            self.numPartitions = sc.defaultParallelism

        self.rows = list(rows[i::self.numPartitions] for i in range(self.numPartitions))

    def _toRDD(self, rows, numPartitions=None):
        if numPartitions:
            return RDD(rows, self.sc, numPartitions)
        return RDD(rows, self.sc, self.numPartitions)

    def aggregate(self, zeroValue, seqOp, combOp):
        raise NotImplementedError()

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def cache(self):
        return self

    def cartesian(self, other):
        raise NotImplementedError()

    def checkpoint(self):
        raise NotImplementedError()

    def coalesce(self, numPartitions, shuffle=False):
        return self

    def cogroup(self, other, numPartitions=None):
        grouped = self.groupByKey()
        other_grouped = other.groupByKey()

        kv = {o[0]: o[1] for o in grouped.collect()}
        other_kv = {o[0]: o[1] for o in other_grouped.collect()}

        return self._toRDD([(k, (kv.get(k, []), other_kv.get(k, []))) for k in set(kv.keys() + other_kv.keys())])

    def collect(self):
        return sorted([i for p in self.rows for i in p])

    def collectAsMap(self):
        raise NotImplementedError()

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def count(self):
        return len(self.collect())

    def countApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def countApproxDistinct(self, relativeSD=0.05):
        raise NotImplementedError()

    def countByKey(self):
        counts = defaultdict(int)
        for p in self.rows:
            for i in p:
                counts[i[0]] += 1

        return counts

    def countByValue(self):
        counts = defaultdict(int)
        for p in self.rows:
            for i in p:
                counts[i] += 1

        return counts

    def distinct(self, numPartitions=None):
        return self._toRDD(list(set(self.collect())))

    def filter(self, f):
        return self._toRDD(filter(f, self.collect()))

    def first(self):
        if not self.isEmpty():
            return self.collect()[0]
        raise ValueError("RDD is empty")

    def flatMap(self, f, preservesPartitioning=False):
        return self._toRDD([v for p in self.rows for i in p for v in f(i)])

    def flatMapValues(self, f):
        return self._toRDD([(i[0], v) for p in self.rows for i in p for v in f(i[1])])

    def fold(self, zeroValue, op):
        raise NotImplementedError()

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def foreach(self, f):
        for p in self.rows:
            for i in p:
                f(i)

    def foreachPartition(self, f):
        raise NotImplementedError()

    def fullOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError()

    def getCheckpointFile(self):
        raise NotImplementedError()

    def getNumPartitions(self):
        return self.numPartitions

    def getStorageLevel(self):
        raise NotImplementedError()

    def glom(self):
        return self.rows

    def groupBy(self, f, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for p in self.rows:
            for i in p:
                tmp[f(i)].append(i)

        return self._toRDD([(k, v) for k, v in tmp.items()])

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for p in self.rows:
            for i in p:
                tmp[i[0]].append(i[1])

        return self._toRDD([(k, v) for k, v in tmp.items()])

    def groupWith(self, other, *others):
        raise NotImplementedError()

    def histogram(self, buckets):
        raise NotImplementedError()

    def intersection(self, other):
        raise NotImplementedError()

    def isCheckpointed(self):
        raise NotImplementedError()

    def isEmpty(self):
        return len(self.collect()) == 0

    def isLocallyCheckpointed(self):
        raise NotImplementedError()

    def join(self, other, numPartitions=None):
        other_kv = defaultdict(list)
        for o in other.collect():
            other_kv[o[0]].append(o[1])

        joined = []
        for p in self.rows:
            for r in p:
                if r[0] in other_kv:
                    for v in other_kv[r[0]]:
                        joined.append((r[0], (r[1], v)))

        return self._toRDD(joined)

    def keyBy(self, f):
        return self._toRDD([(f(i), i) for i in self.collect()])

    def keys(self):
        return self.map(lambda x: x[0])

    def leftOuterJoin(self, other, numPartitions=None):
        other_kv = defaultdict(list)
        for o in other.collect():
            other_kv[o[0]].append(o[1])

        joined = []
        for p in self.rows:
            for r in p:
                if r[0] in other_kv:
                    joined += [(r[0], (r[1], v)) for v in other_kv[r[0]]]
                else:
                    joined.append((r[0], (r[1], None)))

        return self._toRDD(joined)

    def localCheckpoint(self):
        raise NotImplementedError()

    def lookup(self, key):
        raise NotImplementedError()

    def map(self, f, preservesPartitioning=False):
        return self._toRDD(list(map(f, self.collect())))

    def mapPartitions(self, f, preservesPartitioning=False):
        return self._toRDD([mapped for p in self.rows for mapped in f(p)])

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithSplit(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapValues(self, f):
        return self._toRDD([(i[0], f(i[1])) for p in self.rows for i in p])

    def max(self, key=None):
        if key:
            return max(self.collect(), key=key)
        return max(self.collect())

    def mean(self):
        raise NotImplementedError()

    def meanApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def min(self, key=None):
        if key:
            return min(self.collect(), key=key)
        return min(self.collect())

    def name(self):
        return self.name

    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        raise NotImplementedError()

    def persist(self, storageLevel=StorageLevel(False, True, False, False, 1)):
        raise NotImplementedError()

    def pipe(self, command, env=None, checkCode=False):
        raise NotImplementedError()

    def randomSplit(self, weights, seed=None):
        raise NotImplementedError()

    def reduce(self, f):
        return reduce(f, self.collect())

    def reduceByKey(self, func, numPartitions=None, partitionFunc=portable_hash):
        grouped = self.groupByKey(numPartitions=numPartitions, partitionFunc=partitionFunc)
        return self._toRDD([(r[0], reduce(func, r[1])) for r in grouped.collect()])

    def reduceByKeyLocally(self, func):
        raise NotImplementedError()

    def repartition(self, numPartitions):
        return self._toRDD(self.collect(), numPartitions)

    def rightOuterJoin(self, other, numPartitions=None):
        kv = defaultdict(list)
        for o in self.collect():
            kv[o[0]].append(o[1])

        joined = []
        for p in other.rows:
            for r in p:
                if r[0] in kv:
                    for v in kv[r[0]]:
                        joined.append((r[0], (v, r[1])))
                else:
                    joined.append((r[0], (None, r[1])))

        return self._toRDD(joined)

    def sample(self, withReplacement, fraction, seed=None):
        raise NotImplementedError()

    def sampleByKey(self, withReplacement, fractions, seed=None):
        raise NotImplementedError()

    def sampleStdev(self):
        raise NotImplementedError()

    def sampleVariance(self):
        raise NotImplementedError()

    def saveAsHadoopDataset(self, conf, keyConverter=None, valueConverter=None):
        raise NotImplementedError()

    def saveAsHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None, compressionCodecClass=None):
        raise NotImplementedError()

    def saveAsNewAPIHadoopDataset(self, conf, keyConverter=None, valueConverter=None):
        raise NotImplementedError()

    def saveAsNewAPIHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None):
        raise NotImplementedError()

    def saveAsPickleFile(self, path, batchSize=10):
        raise NotImplementedError()

    def saveAsSequenceFile(self, path, compressionCodecClass=None):
        raise NotImplementedError()

    def saveAsTextFile(self, path, compressionCodecClass=None):
        raise NotImplementedError()

    def setName(self, name):
        self.name = name

    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        raise NotImplementedError()

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        raise NotImplementedError()

    def stats(self):
        raise NotImplementedError()

    def stdev(self):
        raise NotImplementedError()

    def subtract(self, other, numPartitions=None):
        raise NotImplementedError()

    def subtractByKey(self, other, numPartitions=None):
        other_keys = other.keys().collect()
        return self._toRDD([i for p in self.rows for i in p if i[0] not in other_keys])

    def sum(self):
        return sum(self.collect())

    def sumApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def take(self, num):
        return self.collect()[:num]

    def takeOrdered(self, num, key=None):
        raise NotImplementedError()

    def takeSample(self, withReplacement, num, seed=None):
        raise NotImplementedError()

    def toDebugString(self):
        raise NotImplementedError()

    def toDF(self, schema=None):
        """This isn't technically an RDD method, but is part of Spark implicits, and
        is available on RDDs"""
        return bermann.dataframe.DataFrame(self.collect(), schema=schema)

    def toLocalIterator(self):
        raise NotImplementedError()

    def top(self, num, key=None):
        raise NotImplementedError()

    def treeAggregate(self, zeroValue, seqOp, combOp, depth=2):
        raise NotImplementedError()

    def treeReduce(self, f, depth=2):
        raise NotImplementedError()

    def union(self, other):
        return self._toRDD((self.collect() + other.collect()))

    def unpersist(self):
        return self

    def values(self):
        return self.map(lambda x: x[1])

    def variance(self):
        raise NotImplementedError()

    def zip(self, other):
        if self.count() != other.count():
            raise Py4JJavaError("Can only zip RDDs with same number of elements in each partition", JavaException(''))
        return self._toRDD(zip(self.collect(), other.collect()))

    def zipWithIndex(self):
        return self._toRDD([(i, idx) for idx, i in enumerate(self.collect())])

    def zipWithUniqueId(self):
        raise NotImplementedError()

    def __eq__(self, other):
        if not isinstance(other, RDD):
            return False
        return self.name == other.name and self.rows == other.rows


class JavaException(Exception):
    """This class exists solely to allow the creation of Py4JErrors
    from the RDD.zip() method, as those require an argument with an
    attribute of _target_id"""

    _target_id = ''
