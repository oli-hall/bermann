from pyspark.sql.types import StringType


class SQLContext(object):

    def __init__(self):
        self.read = None
        self.readStream = None
        self.streams = None

    def cacheTable(self, tableName):
        raise NotImplementedError()

    def clearCache(self):
        raise NotImplementedError()

    def createDataFrame(self, data, schema=None, samplingRatio=None, verifySchema=True):
        raise NotImplementedError()

    def createExternalTable(self, tableName, path=None, source=None, schema=None, **options):
        raise NotImplementedError()

    def dropTempTable(self, tableName):
        raise NotImplementedError()

    def getConf(self, key, defaultValue=None):
        raise NotImplementedError()

    def newSession(self):
        raise NotImplementedError()

    def range(self, start, end=None, step=1, numPartitions=None):
        raise NotImplementedError()

    def registerFunction(self, name, f, returnType=StringType):
        raise NotImplementedError()

    def registerJavaFunction(self, name, javaClassName, returnType=None):
        raise NotImplementedError()

    def setConf(self, key, value):
        raise NotImplementedError()

    def sql(self, sqlQuery):
        raise NotImplementedError()

    def table(self, tableName):
        raise NotImplementedError()

    def tableNames(self, dbName=None):
        raise NotImplementedError()

    def tables(self, dbName=None):
        raise NotImplementedError()

    def uncacheTable(self, tableName):
        raise NotImplementedError()
