from types import GeneratorType

from bermann.accumulator import Accumulator
from bermann.broadcast import Broadcast
import bermann.rdd


class SparkContext(object):

    DEFAULT_PARTITIONS = 4

    def __init__(self, conf=None):

        self.conf = conf or {}
        self.applicationId = None
        self.defaultMinPartitions = None
        if "spark.default.parallelism" in self.conf:
            self.defaultParallelism = int(self.conf["spark.default.parallelism"])
        else:
            self.defaultParallelism = self.DEFAULT_PARTITIONS
        self.startTime = None

    def accumulator(self, value, accum_param=None):
        return Accumulator(value)

    def addFile(self, path, recursive=False):
        raise NotImplementedError()

    def addPyFile(self, path):
        raise NotImplementedError()

    def binaryFiles(self, path, minPartitions=None):
        raise NotImplementedError()

    def binaryRecords(self, path, recordLength):
        raise NotImplementedError()

    def broadcast(self, value):
        return Broadcast(value)

    def cancelAllJobs(self):
        raise NotImplementedError()

    def cancelJobGroup(self, groupId):
        raise NotImplementedError()

    def dump_profiles(self, path):
        raise NotImplementedError()

    def emptyRDD(self):
        return bermann.rdd.RDD.from_list([], sc=self)

    def getConf(self):
        return self.conf

    def getLocalProperty(self, key):
        raise NotImplementedError()

    @classmethod
    def getOrCreate(cls, conf=None):
        raise NotImplementedError()

    def hadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None,
               batchSize=0):
        raise NotImplementedError()

    def hadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None, batchSize=0):
        raise NotImplementedError()

    def newAPIHadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None,
                     batchSize=0):
        raise NotImplementedError()

    def newAPIHadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None,
                    batchSize=0):
        raise NotImplementedError()

    def parallelize(self, c, numSlices=None):
        if isinstance(c, GeneratorType):
            return bermann.rdd.RDD.from_list(list(c), sc=self, numPartitions=numSlices)
        return bermann.rdd.RDD.from_list(c, sc=self, numPartitions=numSlices)

    def pickleFile(self, name, minPartitions=None):
        raise NotImplementedError()

    def range(self, start, end=None, step=1, numSlices=None):
        raise NotImplementedError()

    def sequenceFile(self, path, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, minSplits=None,
                 batchSize=0):
        raise NotImplementedError()

    def setJobGroup(self, groupId, description, interruptOnCancel=False):
        raise NotImplementedError()

    def setLocalProperty(self, key, value):
        raise NotImplementedError()

    def setLogLevel(self, logLevel):
        raise NotImplementedError()

    @classmethod
    def setSystemProperty(cls, key, value):
        raise NotImplementedError()

    def show_profiles(self):
        raise NotImplementedError()

    def sparkUser(self):
        raise NotImplementedError()

    def statusTracker(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def textFile(self, name, minPartitions=None, use_unicode=True):
        raise NotImplementedError()

    def union(self, rdds):
        if not rdds or len(rdds) < 1:
            raise ValueError("Can only union a non-empty list of RDDs")
        return reduce(lambda x, y: x.union(y), rdds)

    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        raise NotImplementedError()
