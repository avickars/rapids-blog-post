from pyspark.sql import SparkSession, types

NUM_START_ROWS = 2500

NUM_EXECUTIONS_PER_TEST = 3

NUM_DSIZE_DOUBLINGS = 12


def main():
    air_schema = types.StructType([
        types.StructField('DATE_PST', types.StringType()),
        types.StructField('DATE', types.DateType()),
        types.StructField('TIME', types.StringType()),
        types.StructField('STATION_NAME', types.StringType()),
        types.StructField('STATION_NAME_FULL', types.StringType()),
        types.StructField('EMS_ID', types.StringType()),
        types.StructField('NAPS_ID', types.IntegerType()),
        types.StructField('RAW_VALUE', types.FloatType()),
        types.StructField('ROUNDED_VALUE', types.FloatType()),
        types.StructField('UNIT', types.StringType()),
        types.StructField('INSTRUMENT', types.StringType()),
        types.StructField('PARAMETER', types.StringType()),
        types.StructField('OWNER', types.StringType()),
        types.StructField('REGION', types.StringType()),

    ])

    co_df = spark.read.csv("1980-2008-CO.csv", header=True, schema=air_schema)

    numRows = NUM_START_ROWS

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        print('Test:', i)

        numRows = numRows * 2

        if numRows > co_df.count():
            co_subset = co_subset.union(co_subset)
        else:
            co_subset = co_df.limit(numRows)

        co_subset = co_subset.repartition(16)

        co_subset.write.csv(f"spark_etl_test_subsets/test_{i}", compression='gzip', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark etl').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
