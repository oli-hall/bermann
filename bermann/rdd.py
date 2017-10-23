from pyspark.rdd import portable_hash
from pyspark.storagelevel import StorageLevel


class RDD(object):

    def __init__(self, input):
        assert(input)

        self.input = input

    def aggregate(self, zeroValue, seqOp, combOp):
        raise NotImplementedError()

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def cache(self):
        raise NotImplementedError()

    def cartesian(self, other):
        raise NotImplementedError()

    def checkpoint(self):
        raise NotImplementedError()

    def coalesce(self, numPartitions, shuffle=False):
        raise NotImplementedError()

    def cogroup(self, other, numPartitions=None):
        raise NotImplementedError()

    def collect(self):
        raise NotImplementedError()

    def collectAsMap(self):
        raise NotImplementedError()

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def count(self):
        raise NotImplementedError()

    def countApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def countApproxDistinct(self, relativeSD=0.05):
        raise NotImplementedError()

    def countByKey(self):
        raise NotImplementedError()

    def countByValue(self):
        raise NotImplementedError()

    def distinct(self, numPartitions=None):
        raise NotImplementedError()

    def filter(self, f):
        raise NotImplementedError()

    def first(self):
        raise NotImplementedError()

    def flatMap(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def flatMapValues(self, f):
        raise NotImplementedError()

    def fold(self, zeroValue, op):
        raise NotImplementedError()

    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def foreach(self, f):
        raise NotImplementedError()

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
        raise NotImplementedError()

    def groupByKey(self, numPartitions=None, partitionFunc=portable_hash):
        raise NotImplementedError()

    def groupWith(self, other, *others):
        raise NotImplementedError()

    def histogram(self, buckets):
        raise NotImplementedError()

    def intersection(self, other):
        raise NotImplementedError()

    def isCheckpointed(self):
        raise NotImplementedError()

    def isEmpty(self):
        raise NotImplementedError()

    def isLocallyCheckpointed(self):
        raise NotImplementedError()

    def join(self, other, numPartitions=None):
        raise NotImplementedError()

    def keyBy(self, f):
        raise NotImplementedError()

    def keys(self):
        raise NotImplementedError()

    def leftOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError()

    def localCheckpoint(self):
        raise NotImplementedError()

    def lookup(self, key):
        raise NotImplementedError()

    def map(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitions(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapPartitionsWithSplit(self, f, preservesPartitioning=False):
        raise NotImplementedError()

    def mapValues(self, f):
        raise NotImplementedError()

    def max(self, key=None):
        raise NotImplementedError()

    def mean(self):
        raise NotImplementedError()

    def meanApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def min(self, key=None):
        raise NotImplementedError()

    def name(self):
        raise NotImplementedError()

    def partitionBy(self, numPartitions, partitionFunc=portable_hash):
        raise NotImplementedError()

    def persist(self, storageLevel=StorageLevel(False, True, False, False, 1)):
        raise NotImplementedError()

    def pipe(self, command, env=None, checkCode=False):
        raise NotImplementedError()

    def randomSplit(self, weights, seed=None):
        raise NotImplementedError()

    def reduce(self, f):
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def sumApprox(self, timeout, confidence=0.95):
        raise NotImplementedError()

    def take(self, num):
        raise NotImplementedError()

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
        raise NotImplementedError()

    def unpersist(self):
        raise NotImplementedError()

    def values(self):
        raise NotImplementedError()

    def variance(self):
        raise NotImplementedError()

    def zip(self, other):
        raise NotImplementedError()

    def zipWithIndex(self):
        raise NotImplementedError()

    def zipWithUniqueId(self):
        raise NotImplementedError()
