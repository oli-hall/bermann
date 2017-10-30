from collections import defaultdict
from functools import reduce

from py4j.protocol import Py4JJavaError
from pyspark.rdd import portable_hash
from pyspark.storagelevel import StorageLevel

import bermann.dataframe


class RDD(object):

    def __init__(self, *rows):
        self.rows = list(rows)
        self.name = None
        # TODO hold/update number of partitions

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

        kv = {o[0]: o[1] for o in grouped.rows}
        other_kv = {o[0]: o[1] for o in other_grouped.rows}

        return RDD(*[(k, (kv.get(k, []), other_kv.get(k, []))) for k in set(kv.keys() + other_kv.keys())])

    def collect(self):
        return self.rows

    def collectAsMap(self):
        raise NotImplementedError()

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def count(self):
        return len(self.rows)

    def countApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def countApproxDistinct(self, relativeSD=0.05):
        raise NotImplementedError()

    def countByKey(self):
        counts = defaultdict(int)
        for i in self.rows:
            counts[i[0]] += 1

        return counts

    def countByValue(self):
        counts = defaultdict(int)
        for i in self.rows:
            counts[i] += 1

        return counts

    def distinct(self, numPartitions=None):
        return RDD(*list(set(self.rows)))

    def filter(self, f):
        return RDD(*filter(f, self.rows))

    def first(self):
        if len(self.rows) > 0:
            return self.rows[0]
        raise ValueError("RDD is empty")

    def flatMap(self, f, preservesPartitioning=False):
        return RDD(*[v for i in self.rows for v in f(i)])

    def flatMapValues(self, f):
        return RDD(*[(i[0], v) for i in self.rows for v in f(i[1])])

    def fold(self, zeroValue, op):
        raise NotImplementedError()

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def foreach(self, f):
        for i in self.rows:
            f(i)

    def foreachPartition(self, f):
        raise NotImplementedError()

    def fullOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError()

    def getCheckpointFile(self):
        raise NotImplementedError()

    def getNumPartitions(self):
        raise NotImplementedError()

    def getStorageLevel(self):
        raise NotImplementedError()

    def glom(self):
        raise NotImplementedError()

    def groupBy(self, f, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for i in self.rows:
            tmp[f(i)].append(i)

        return RDD(*[(k, v) for k, v in tmp.items()])

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for i in self.rows:
            tmp[i[0]].append(i[1])

        return RDD(*[(k, v) for k, v in tmp.items()])

    def groupWith(self, other, *others):
        raise NotImplementedError()

    def histogram(self, buckets):
        raise NotImplementedError()

    def intersection(self, other):
        raise NotImplementedError()

    def isCheckpointed(self):
        raise NotImplementedError()

    def isEmpty(self):
        return len(self.rows) == 0

    def isLocallyCheckpointed(self):
        raise NotImplementedError()

    def join(self, other, numPartitions=None):
        other_kv = defaultdict(list)
        for o in other.rows:
            other_kv[o[0]].append(o[1])

        joined = []
        for r in self.rows:
            if r[0] in other_kv:
                for v in other_kv[r[0]]:
                    joined.append((r[0], (r[1], v)))

        return RDD(*joined)

    def keyBy(self, f):
        return RDD(*[(f(i), i) for i in self.rows])

    def keys(self):
        return self.map(lambda x: x[0])

    def leftOuterJoin(self, other, numPartitions=None):
        other_kv = defaultdict(list)
        for o in other.rows:
            other_kv[o[0]].append(o[1])

        joined = []
        for r in self.rows:
            if r[0] in other_kv:
                for v in other_kv[r[0]]:
                    joined.append((r[0], (r[1], v)))
            else:
                joined.append((r[0], (r[1], None)))

        return RDD(*joined)

    def localCheckpoint(self):
        raise NotImplementedError()

    def lookup(self, key):
        raise NotImplementedError()

    def map(self, f, preservesPartitioning=False):
        return RDD(*[f(i) for i in self.rows])

    def mapPartitions(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithSplit(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapValues(self, f):
        return RDD(*[(i[0], f(i[1])) for i in self.rows])

    def max(self, key=None):
        if key:
            return max(self.rows, key=key)
        return max(self.rows)

    def mean(self):
        raise NotImplementedError()

    def meanApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def min(self, key=None):
        if key:
            return min(self.rows, key=key)
        return min(self.rows)

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
        return reduce(f, self.rows)

    def reduceByKey(self, func, numPartitions=None, partitionFunc=portable_hash):
        grouped = self.groupByKey(numPartitions=numPartitions, partitionFunc=partitionFunc)
        return RDD(*[(r[0], reduce(func, r[1])) for r in grouped.rows])

    def reduceByKeyLocally(self, func):
        raise NotImplementedError()

    def repartition(self, numPartitions):
        raise NotImplementedError()

    def rightOuterJoin(self, other, numPartitions=None):
        kv = defaultdict(list)
        for o in self.rows:
            kv[o[0]].append(o[1])

        joined = []
        for r in other.rows:
            if r[0] in kv:
                for v in kv[r[0]]:
                    joined.append((r[0], (v, r[1])))
            else:
                joined.append((r[0], (None, r[1])))

        return RDD(*joined)

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
        return RDD(*[i for i in self.rows if i[0] not in other_keys])

    def sum(self):
        return sum(self.rows)

    def sumApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def take(self, num):
        return self.rows[:num]

    def takeOrdered(self, num, key=None):
        raise NotImplementedError()

    def takeSample(self, withReplacement, num, seed=None):
        raise NotImplementedError()

    def toDebugString(self):
        raise NotImplementedError()

    def toDF(self, schema=None):
        """This isn't technically an RDD method, but is part of Spark implicits, and
        is available on RDDs"""
        return bermann.dataframe.DataFrame(self.rows, schema=schema)

    def toLocalIterator(self):
        raise NotImplementedError()

    def top(self, num, key=None):
        raise NotImplementedError()

    def treeAggregate(self, zeroValue, seqOp, combOp, depth=2):
        raise NotImplementedError()

    def treeReduce(self, f, depth=2):
        raise NotImplementedError()

    def union(self, other):
        return RDD(*(self.rows + other.rows))

    def unpersist(self):
        return self

    def values(self):
        return self.map(lambda x: x[1])

    def variance(self):
        raise NotImplementedError()

    def zip(self, other):
        if len(self.rows) != len(other.rows):
            raise Py4JJavaError("Can only zip RDDs with same number of elements in each partition", JavaException(''))
        return RDD(*zip(self.rows, other.rows))

    def zipWithIndex(self):
        return RDD(*[(i, idx) for idx, i in enumerate(self.rows)])

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
