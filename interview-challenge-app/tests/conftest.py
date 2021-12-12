import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    spark = SparkSession.builder \
        .appName("test_interview-challenge-app") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())
    return spark
