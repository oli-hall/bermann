# bermann

A unit-testing library for PySpark.

Currently, Spark, and PySpark in particular, has little support for testing Spark job logic, particularly in a unit-test environment. If you want to test even the simplest RDD operations, you have to spin up a local Spark instance and run your code on that. This is overkill, and once you have a decent suite of Spark tests, really gets in the way of speedy tests.

### Where does Bermann come in?

Bermann essentially replicates Spark constructs, such as RDDs, DataFrames, etc, so that you can test your methods rapidly in pure Python, without needing to spin up an entire Spark cluster.

#### Setup

Clone the repo, create a virtualenv, install the requirements, and you're good to go!

```bash
virtualenv bin/env
source bin/env/bin/activate
python setup.py install
```

Setuptools should mean you can install directly from GitHub, by putting the following in your requirements file:

```bash
git+git://github.com/oli-hall/bermann.git@<release version>#egg=bermann
```

#### Requirements

This has been tested with Python 2.7, but should be Python 3 compatible. More thorough testing will follow. It uses the `pyspark` and `py4j` Python libs, but requires no external services to run (that'd be kinda contrary to the spirit of the library!).

### Usage 

Currently, the library consists of only RDD support, but more will be coming soon, never worry! 

#### RDD

To use the Bermann RDD, import the RDD class, and initialise it with the starting state (a list). Then apply RDD operations as per Spark:

```python
> from bermann import RDD
> 
> rdd = RDD([1, 2, 3])
> rdd.count()
3
``` 

This means if you have methods that take RDDs and modify them, you can now test them by creating Bermann RDDs in your tests, and pass those into the methods to be tested. Then, simply assert that the contents of the RDD are as expected at the end.

### Where does the name come from?

The library is named after Max Bermann, a Hungarian engineer who first discovered that [spark testing](https://en.wikipedia.org/wiki/Spark_testing) could reliably classify ferrous material. 
