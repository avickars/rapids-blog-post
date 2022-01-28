from pyspark.sql import SparkSession, functions, types
import os
import time
import pandas as pd

NUM_START_ROWS = 2500

NUM_EXECUTIONS_PER_TEST = 3

NUM_DSIZE_DOUBLINGS = 11


def main():
    spark_results = []

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

    station_co_df = spark.read.csv("bc_air_monitoring_stations.csv", header=True, schema=station_schema)
    station_co_df = functions.broadcast(station_co_df)

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        co_subset = spark.read.csv(f"test_subsets/test_{i}", schema=air_schema)

        for i in range(0, NUM_EXECUTIONS_PER_TEST):
            # ******************************************************************************
            # MEAN TEST

            test = {'Test': 'Mean'}

            # Starting timer
            t0 = time.time()

            # Executing the Mean test
            co_subset.agg({"RAW_VALUE": "mean"}).collect()

            # Stopping clock
            t1 = time.time()

            # Recording Results
            total_time = t1 - t0
            avg_time = total_time / NUM_EXECUTIONS_PER_TEST
            test['Total'] = total_time
            test['Average'] = avg_time

            spark_results.append(test)

            # ******************************************************************************
            # SORT TEST

            test = {'Test': 'Sort'}

            # Starting timer
            t0 = time.time()

            # Executing the Sort test
            co_subset.sort("RAW_VALUE", ascending=True).show(5)

            # Stopping clock
            t1 = time.time()

            # Recording Results
            total_time = t1 - t0
            avg_time = total_time / NUM_EXECUTIONS_PER_TEST
            test['Total'] = total_time
            test['Average'] = avg_time

            spark_results.append(test)

            # ******************************************************************************
            # MERGE TEST

            test = {'Test': 'Merge'}

            # Starting timer
            t0 = time.time()

            # Executing the Merge test
            co_subset_merged = co_subset.join(station_co_df, how='left', on='STATION_NAME')
            co_subset_merged.show(5)

            # Stopping clock
            t1 = time.time()

            # Recording Results
            total_time = t1 - t0
            avg_time = total_time / NUM_EXECUTIONS_PER_TEST
            test['Total'] = total_time
            test['Average'] = avg_time

            spark_results.append(test)

            # ******************************************************************************
            # FILTER TEST

            test = {'Test': 'Filter'}

            # Starting timer
            t0 = time.time()

            # Executing the Filter test
            co_subset[co_subset['STATION_NAME'] == 'Victoria Topaz'].show(5)

            # Stopping clock
            t1 = time.time()

            # Recording Results
            total_time = t1 - t0
            avg_time = total_time / NUM_EXECUTIONS_PER_TEST
            test['Total'] = total_time
            test['Average'] = avg_time

            spark_results.append(test)

            # ******************************************************************************
            # ALL TEST

            test = {'Test': 'All'}

            # Starting timer
            t0 = time.time()

            # Executing the Mean test
            co_subset.agg({"RAW_VALUE": "mean"}).collect()

            # Executing the Sort test
            co_subset.sort("RAW_VALUE", ascending=True).show(5)

            # Executing the Merge test
            co_subset_merged = co_subset.join(station_co_df, how='left', on='STATION_NAME')
            co_subset_merged.show(5)

            # Executing the Filter test
            co_subset[co_subset['STATION_NAME'] == 'Victoria Topaz'].show(5)

            # Stopping clock
            t1 = time.time()

            # Recording Results
            total_time = t1 - t0
            avg_time = total_time / NUM_EXECUTIONS_PER_TEST
            test['Total'] = total_time
            test['Average'] = avg_time

            spark_results.append(test)

        break

    # Keeping this as a pandas df to maintain order
    pd.DataFrame(spark_results).to_csv('spark_etl_results.csv')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark etl tests').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
