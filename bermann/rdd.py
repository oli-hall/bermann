from collections import defaultdict
from functools import reduce

from py4j.protocol import Py4JJavaError
from pyspark.rdd import portable_hash
from pyspark.storagelevel import StorageLevel


# TODO should these operations modify the existing RDD or return a new one with the updated contents?
class RDD(object):

    def __init__(self, content=[], name=None):
        assert isinstance(content, list)

        self.contents = content
        self.name = name

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
        raise NotImplementedError()

    def cogroup(self, other, numPartitions=None):
        raise NotImplementedError()

    def collect(self):
        return self.contents

    def collectAsMap(self):
        raise NotImplementedError()

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def count(self):
        return len(self.contents)

    def countApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def countApproxDistinct(self, relativeSD=0.05):
        raise NotImplementedError()

    def countByKey(self):
        counts = defaultdict(int)
        for i in self.contents:
            counts[i[0]] += 1

        return counts

    def countByValue(self):
        counts = defaultdict(int)
        for i in self.contents:
            counts[i] += 1

        return counts

    def distinct(self, numPartitions=None):
        self.contents = list(set(self.contents))
        return self

    def filter(self, f):
        self.contents = filter(f, self.contents)
        return self

    def first(self):
        if len(self.contents) > 0:
            return self.contents[0]
        raise ValueError("RDD is empty")

    def flatMap(self, f, preservesPartitioning=False):
        self.contents = [v for i in self.contents for v in f(i)]
        return self

    def flatMapValues(self, f):
        self.contents = [(i[0], v) for i in self.contents for v in f(i[1])]
        return self

    def fold(self, zeroValue, op):
        raise NotImplementedError()

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def foreach(self, f):
        for i in self.contents:
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
        for i in self.contents:
            tmp[f(i)].append(i)

        self.contents = [(k, v) for k, v in tmp.items()]
        return self

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash):
        tmp = defaultdict(list)
        for i in self.contents:
            tmp[i[0]].append(i[1])

        self.contents = [(k, v) for k, v in tmp.items()]
        return self

    def groupWith(self, other, *others):
        raise NotImplementedError()

    def histogram(self, buckets):
        raise NotImplementedError()

    def intersection(self, other):
        raise NotImplementedError()

    def isCheckpointed(self):
        raise NotImplementedError()

    def isEmpty(self):
        return len(self.contents) == 0

    def isLocallyCheckpointed(self):
        raise NotImplementedError()

    def join(self, other, numPartitions=None):
        raise NotImplementedError()

    def keyBy(self, f):
        self.contents = [(f(i), i) for i in self.contents]
        return self

    def keys(self):
        return self.map(lambda x: x[0])

    def leftOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError()

    def localCheckpoint(self):
        raise NotImplementedError()

    def lookup(self, key):
        raise NotImplementedError()

    def map(self, f, preservesPartitioning=False):
        self.contents = [f(i) for i in self.contents]
        return self

    def mapPartitions(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithSplit(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapValues(self, f):
        self.contents = [(i[0], f(i[1])) for i in self.contents]
        return self

    def max(self, key=None):
        if key:
            return max(self.contents, key=key)
        return max(self.contents)

    def mean(self):
        raise NotImplementedError()

    def meanApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def min(self, key=None):
        if key:
            return min(self.contents, key=key)
        return min(self.contents)

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
        self.contents = reduce(f, self.contents)
        return self

    def reduceByKey(self, func, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def reduceByKeyLocally(self, func):
        raise NotImplementedError()

    def repartition(self, numPartitions):
        raise NotImplementedError()

    def rightOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError()

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
        raise NotImplementedError()

    def sum(self):
        return sum(self.contents)

    def sumApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def take(self, num):
        return self.contents[:num]

    def takeOrdered(self, num, key=None):
        raise NotImplementedError()

    def takeSample(self, withReplacement, num, seed=None):
        raise NotImplementedError()

    def toDebugString(self):
        raise NotImplementedError()

    def toLocalIterator(self):
        raise NotImplementedError()

    def top(self, num, key=None):
        raise NotImplementedError()

    def treeAggregate(self, zeroValue, seqOp, combOp, depth=2):
        raise NotImplementedError()

    def treeReduce(self, f, depth=2):
        raise NotImplementedError()

    def union(self, other):
        self.contents = self.contents + other.contents
        return self

    def unpersist(self):
        raise NotImplementedError()

    def values(self):
        return self.map(lambda x: x[1])

    def variance(self):
        raise NotImplementedError()

    def zip(self, other):
        if len(self.contents) != len(other.contents):
            raise Py4JJavaError("Can only zip RDDs with same number of elements in each partition", JavaException(''))
        self.contents = zip(self.contents, other.contents)
        return self

    def zipWithIndex(self):
        self.contents = [(i, idx) for idx, i in enumerate(self.contents)]
        return self

    def zipWithUniqueId(self):
        raise NotImplementedError()

    def __eq__(self, other):
        return self.name == other.name and self.contents == other.contents


class JavaException(Exception):
    """This class exists solely to allow the creation of Py4JErrors
    from the RDD.zip() method, as those require an argument with an
    attribute of _target_id"""

    _target_id = ''
