from collections import defaultdict
from functools import reduce

from py4j.protocol import Py4JJavaError
from pyspark.rdd import portable_hash
from pyspark.storagelevel import StorageLevel

import bermann.dataframe
import bermann.spark_context


class RDD(object):

    @staticmethod
    def from_list(input=[], sc=None, numPartitions=None):
        assert isinstance(sc, bermann.spark_context.SparkContext)

        if not numPartitions:
            numPartitions = sc.defaultParallelism

        rows = RDD._partition(input, numPartitions)

        return RDD(rows, sc, numPartitions)

    @staticmethod
    def _partition(input_lst=[], numPartitions=1):
        output = []
        from_idx, to_idx = 0, 0
        for i in range(numPartitions):
            partition_len = len(input_lst[i::numPartitions])
            to_idx += partition_len
            output.append(input_lst[from_idx:to_idx])
            from_idx += partition_len

        return output

    def __init__(self, rows=None, sc=None, numPartitions=None):
        """
        This is designed for internal use only, to create RDDs from partitioned data.
        If you create with a flat list of partitions things will break!

        :param rows: partitioned list of lists of data
        :param sc: SparkContext to create RDD with
        :param numPartitions: the number of partitions in this RDD
        """

        self.name = None
        self.context = sc
        self.numPartitions = numPartitions
        self.partitions = rows

    def _toRDD(self, rows):
        return RDD(rows, self.context, self.numPartitions)

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

        return RDD.from_list([(k, (kv.get(k, []), other_kv.get(k, []))) for k in set(kv.keys() + other_kv.keys())],
                             sc=self.context,
                             numPartitions=self.numPartitions)

    def collect(self):
        return sorted([i for p in self.partitions for i in p])

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
        for p in self.partitions:
            for i in p:
                counts[i[0]] += 1

        return counts

    def countByValue(self):
        counts = defaultdict(int)
        for p in self.partitions:
            for i in p:
                counts[i] += 1

        return counts

    def distinct(self, numPartitions=None):
        return RDD.from_list(list(set(self.collect())), sc=self.context, numPartitions=self.numPartitions)

    def filter(self, f):
        return self._toRDD([list(filter(f, p)) for p in self.partitions])

    def first(self):
        if not self.isEmpty():
            return self.collect()[0]
        raise ValueError("RDD is empty")

    def flatMap(self, f, preservesPartitioning=False):
        return self._toRDD([[v for i in p for v in f(i)] for p in self.partitions])

    def flatMapValues(self, f):
        return self._toRDD([[(i[0], v) for i in p for v in f(i[1])] for p in self.partitions])

    def fold(self, zeroValue, op):
        raise NotImplementedError()

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def foreach(self, f):
        for p in self.partitions:
            for i in p:
                f(i)

    def foreachPartition(self, f):
        raise NotImplementedError()

    def fullOuterJoin(self, other, numPartitions=None):
        kv = defaultdict(list)
        for r in self.collect():
            kv[r[0]].append(r[1])

        other_kv = defaultdict(list)
        for o in other.collect():
            other_kv[o[0]].append(o[1])

        joined = []

        for k in set(kv.keys() + other_kv.keys()):
            if k in kv:
                for v in kv[k]:
                    if k in other_kv:
                        for v_o in other_kv[k]:
                            joined.append((k, (v, v_o)))
                    else:
                        joined.append((k, (v, None)))
            else:
                for v_o in other_kv[k]:
                    joined.append((k, (None, v_o)))

        return self.from_list(joined,
                              sc=self.context,
                              numPartitions=self.numPartitions)

    def getCheckpointFile(self):
        raise NotImplementedError()

    def getNumPartitions(self):
        return self.numPartitions

    def getStorageLevel(self):
        raise NotImplementedError()

    def glom(self):
        return self._toRDD([[p] for p in self.partitions])

    def groupBy(self, f, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for p in self.partitions:
            for i in p:
                tmp[f(i)].append(i)

        return RDD.from_list([(k, v) for k, v in tmp.items()], self.context, self.numPartitions)

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for p in self.partitions:
            for i in p:
                tmp[i[0]].append(i[1])

        return self.from_list([(k, v) for k, v in tmp.items()], self.context, self.numPartitions)

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
        for p in self.partitions:
            joined_p = []
            for r in p:
                if r[0] in other_kv:
                    for v in other_kv[r[0]]:
                        joined_p.append((r[0], (r[1], v)))
            joined.append(joined_p)

        return self._toRDD(joined)

    def keyBy(self, f):
        return self._toRDD([[(f(i), i) for i in p] for p in self.partitions])

    def keys(self):
        return self.map(lambda x: x[0])

    def leftOuterJoin(self, other, numPartitions=None):
        other_kv = defaultdict(list)
        for o in other.collect():
            other_kv[o[0]].append(o[1])

        joined = []
        for p in self.partitions:
            joined_p = []
            for r in p:
                if r[0] in other_kv:
                    joined_p += [(r[0], (r[1], v)) for v in other_kv[r[0]]]
                else:
                    joined_p.append((r[0], (r[1], None)))
            joined.append(joined_p)

        return self._toRDD(joined)

    def localCheckpoint(self):
        raise NotImplementedError()

    def lookup(self, key):
        raise NotImplementedError()

    def map(self, f, preservesPartitioning=False):
        return self._toRDD([list(map(f, p)) for p in self.partitions])

    def mapPartitions(self, f, preservesPartitioning=False):
        return self._toRDD([[m for m in f(p)] for p in self.partitions])

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithSplit(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapValues(self, f):
        return self._toRDD([[(i[0], f(i[1])) for i in p] for p in self.partitions])

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
        # TODO use partitionFunc
        return self.repartition(numPartitions)

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
        return self._toRDD([[(r[0], reduce(func, r[1])) for r in p] for p in grouped.partitions])

    def reduceByKeyLocally(self, func):
        raise NotImplementedError()

    def repartition(self, numPartitions):
        if numPartitions == self.numPartitions:
            return self
        return RDD.from_list([i for p in self.partitions for i in p],
                             sc=self.context,
                             numPartitions=numPartitions)

    def rightOuterJoin(self, other, numPartitions=None):
        kv = defaultdict(list)
        for o in self.collect():
            kv[o[0]].append(o[1])

        joined = []
        for p in other.partitions:
            joined_p = []
            for r in p:
                if r[0] in kv:
                    for v in kv[r[0]]:
                        joined_p.append((r[0], (v, r[1])))
                else:
                    joined_p.append((r[0], (None, r[1])))
            joined.append(joined_p)

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
        kv = defaultdict(list)
        for p in self.partitions:
            for r in p:
                kv[keyfunc(r)].append(r)

        sorted_keys = sorted(kv, reverse=(not ascending))

        return RDD.from_list([v for k in sorted_keys for v in kv[k]],
                             sc=self.context,
                             numPartitions=self.numPartitions)

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        kv = defaultdict(list)
        for p in self.partitions:
            for r in p:
                kv[r[0]].append(r[1])

        keys = {keyfunc(k): k for k in kv.keys()}
        sorted_keys = sorted(keys, reverse=(not ascending))

        return RDD.from_list([(k, v) for k in sorted_keys for v in kv[keys[k]]],
                             sc=self.context,
                             numPartitions=self.numPartitions)

    def stats(self):
        raise NotImplementedError()

    def stdev(self):
        raise NotImplementedError()

    def subtract(self, other, numPartitions=None):
        raise NotImplementedError()

    def subtractByKey(self, other, numPartitions=None):
        other_keys = other.keys().collect()
        return self._toRDD([[i for i in p if i[0] not in other_keys] for p in self.partitions])

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
        # just add the two sets of partitions
        rdd = self._toRDD((self.partitions + other.partitions))
        rdd.numPartitions = self.numPartitions + other.numPartitions
        return rdd

    def unpersist(self):
        return self

    def values(self):
        return self.map(lambda x: x[1])

    def variance(self):
        raise NotImplementedError()

    def zip(self, other):
        if self.count() != other.count():
            raise Py4JJavaError("Can only zip RDDs with same number of elements in each partition", JavaException(''))

        other_flat = [o for p in other.partitions for o in p]

        zipped = []
        idx = 0
        for p in self.partitions:
            zipped_p = []
            for r in p:
                zipped_p.append((r, other_flat[idx]))
                idx += 1
            zipped.append(zipped_p)

        return self._toRDD(zipped)

    def zipWithIndex(self):
        zipped = []
        idx = 0
        for p in self.partitions:
            zipped_p = []
            for r in p:
                zipped_p.append((r, idx))
                idx += 1
            zipped.append(zipped_p)

        return self._toRDD(zipped)

    def zipWithUniqueId(self):
        zipped = []
        for k, p in enumerate(self.partitions):
            zipped.append([(r, (i * self.numPartitions) + k) for i, r in enumerate(p)])

        return self._toRDD(zipped)

    def __eq__(self, other):
        if not isinstance(other, RDD):
            return False
        return self.name == other.name \
               and self.partitions == other.partitions \
               and self.numPartitions == other.numPartitions \
               and self.context == other.context


class JavaException(Exception):
    """This class exists solely to allow the creation of Py4JErrors
    from the RDD.zip() method, as those require an argument with an
    attribute of _target_id"""

    _target_id = ''
