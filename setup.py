from setuptools import setup

install_requires = [
    "pyspark==2.2.0.post0",
    "py4j==0.10.4"
]

test_requires = [
]

setup(
    name='Bermann',
    version='0.1.0',
    author='Oli Hall',
    author_email='',
    description="Unit testing library for PySpark",
    license='MIT',
    url='https://github.com/oli-hall/bermann',
    packages=['bermann'],
    setup_requires=[],
    install_requires=install_requires,
    tests_require=test_requires,
)
