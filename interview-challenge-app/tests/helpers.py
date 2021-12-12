def get_test_data(spark_session, module, func, type):
    return spark_session.read.json(f"tests/data/test_{module}/{func}/{type}")
