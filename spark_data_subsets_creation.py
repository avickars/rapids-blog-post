from pyspark.sql import SparkSession, functions, types
import os

NUM_START_ROWS = 2500

NUM_EXECUTIONS_PER_TEST = 3

NUM_DSIZE_DOUBLINGS = 11


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

    station_schema = types.StructType([
        types.StructField('STATION_NAME_FULL', types.StringType()),
        types.StructField('STATION_NAME', types.StringType()),
        types.StructField('EMS_ID', types.StringType()),
        types.StructField('SERIAL', types.IntegerType()),
        types.StructField('ADDRESS', types.StringType()),
        types.StructField('CITY', types.StringType()),
        types.StructField('LAT', types.FloatType()),
        types.StructField('LONG', types.FloatType()),
        types.StructField('ELEVATION', types.IntegerType()),
        types.StructField('STATUS_DESCRIPTION', types.StringType()),
        types.StructField('OWNER', types.StringType()),
        types.StructField('REGION', types.StringType()),
        types.StructField('STATUS', types.StringType()),
        types.StructField('OPENED', types.TimestampType()),
        types.StructField('CLOSED', types.StringType()),
        types.StructField('NAPS_ID', types.IntegerType()),

    ])

    co_df = spark.read.csv("1980-2008-CO.csv", header=True, schema=air_schema)
    station_co_df = spark.read.csv("bc_air_monitoring_stations.csv", header=True, schema=station_schema)

    try:
        os.mkdir(f"test_subsets")
    except FileExistsError:
        print('directory already exists')

    numRows = NUM_START_ROWS

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        print('Test:', i)

        numRows = numRows * 2

        if numRows > co_df.count():
            co_subset = co_subset.append(co_subset)
        else:
            co_subset = co_df.limit(numRows)

        co_subset = co_subset.repartition(2)

        co_subset.write.csv(f"test_subsets/test_{i}", compression='gzip')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark etl').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
