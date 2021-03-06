from pyspark.sql import SparkSession, functions, types
import sys
import time

NUM_START_ROWS = 2500

NUM_EXECUTIONS_PER_TEST = 3

NUM_DSIZE_DOUBLINGS = 12


def main(input1, input2, output):
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

    station_co_df = spark.read.csv(f"{input1}", header=True, schema=station_schema)
    station_co_df = functions.broadcast(station_co_df)

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        print('Test', i, '***************************************************************************************************************')
        co_subset = spark.read.csv(f"{input2}/test_{i}", schema=air_schema)

        # ******************************************************************************
        # MEAN TEST

        test = {'Test': 'Mean', 'Test Number': i}

        # Starting timer
        t0 = time.time()

        for j in range(0, NUM_EXECUTIONS_PER_TEST):

            # Executing the Mean test
            co_subset.select(functions.mean("RAW_VALUE")).show()

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

        test = {'Test': 'Sort', 'Test Number': i}

        # Starting timer
        t0 = time.time()

        for j in range(0, NUM_EXECUTIONS_PER_TEST):

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

        test = {'Test': 'Merge', 'Test Number': i}

        # Starting timer
        t0 = time.time()

        for j in range(0, NUM_EXECUTIONS_PER_TEST):

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

        test = {'Test': 'Filter', 'Test Number': i}

        # Starting timer
        t0 = time.time()

        for j in range(0, NUM_EXECUTIONS_PER_TEST):
            # Executing the Filter test
            co_subset.filter(co_subset['STATION_NAME'] == 'Victoria Topaz').show(5)

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

        test = {'Test': 'All', 'Test Number': i}

        # Starting timer
        t0 = time.time()

        for j in range(0, NUM_EXECUTIONS_PER_TEST):

            # Executing the Mean test
            co_subset.select(functions.mean("RAW_VALUE")).show()

            # Executing the Sort test
            co_subset.sort("RAW_VALUE", ascending=True).show(5)

            # Executing the Merge test
            co_subset_merged = co_subset.join(station_co_df, how='left', on='STATION_NAME')
            co_subset_merged.show(5)

            # Executing the Filter test
            co_subset.filter(co_subset['STATION_NAME'] == 'Victoria Topaz').show(5)

        # Stopping clock
        t1 = time.time()

        # Recording Results
        total_time = t1 - t0
        avg_time = total_time / NUM_EXECUTIONS_PER_TEST
        test['Total'] = total_time
        test['Average'] = avg_time

        spark_results.append(test)

    # Keeping this as a pandas df to maintain order
    # pd.DataFrame(spark_results).to_csv('AWS Results/spark_etl_results.csv')
    # pd.DataFrame(spark_results).to_csv(f"{output}")
    schema = types.StructType([
        types.StructField('Test', types.StringType()),
        types.StructField('Test Number', types.IntegerType()),
        types.StructField('Total', types.DoubleType()),
        types.StructField('Average', types.DoubleType())
    ])
    results_rdd = sc.parallelize(spark_results, numSlices=1)
    spark.createDataFrame(data=results_rdd, schema=schema).write.csv(output, header=True)


if __name__ == '__main__':
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('spark etl tests').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input1, input2, output)
