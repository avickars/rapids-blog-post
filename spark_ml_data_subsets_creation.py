from pyspark.sql import SparkSession, types

NUM_FEATURES = 399 + 1

NUM_DSIZE_DOUBLINGS = 10


def main():
    schema = types.StructType([
        types.StructField(f"{i}", types.DoubleType()) for i in range(0, NUM_FEATURES)

    ])

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        print('Test:', i)

        X = spark.read.csv(
            f"ml_data/{i}/",
            schema=schema,
            header=True)

        X = X.repartition(16)

        X.write.csv(f"spark_ml_test_subsets/test_{i}", compression='gzip', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark etl').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
