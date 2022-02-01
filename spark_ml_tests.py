from pyspark.sql import SparkSession, functions, types
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import time
import sys

NUM_START_ROWS = 2500

NUM_EXECUTIONS_PER_TEST = 3

NUM_DSIZE_DOUBLINGS = 10

NUM_FEATURES = 399 + 1


def main(input, output):
    schema = types.StructType([
        types.StructField(f"{i}", types.DoubleType()) for i in range(0, NUM_FEATURES)

    ])

    results = []

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        print('Test:', i, '***************************************************************************************************************')
        X = spark.read.csv(
            f"{input}/test_{i}/",
            schema=schema,
            header=True)

        test = {'Test': 'Linear Regression', 'Test Number': i}

        # ******************************************************************************
        # LINEAR REGRESSION TEST

        # Starting timer
        t0 = time.time()

        for j in range(0, NUM_EXECUTIONS_PER_TEST):
            assemble_features = VectorAssembler(
                inputCols=[f"{i}" for i in range(0, NUM_FEATURES - 1)],
                outputCol='features')
            X_vector = assemble_features.transform(X)
            X_vector = X_vector.select(['features', '399'])

            ols = LinearRegression(
                featuresCol='features', labelCol='399', fitIntercept=True)
            # pipeline_ols = Pipeline(stages=[assemble_features, ols])
            ols_model = ols.fit(X_vector)
            print("Intercept: " + str(ols_model.intercept))

        # Stopping clock
        t1 = time.time()

        # Recording Results
        total_time = t1 - t0
        avg_time = total_time / NUM_EXECUTIONS_PER_TEST
        test['Total'] = total_time
        test['Average'] = avg_time

        results.append(test)

        test = {'Test': 'K Nearest Neighbour', 'Test Number': i, 'Total': 0.0, 'Average': 0.0}
        results.append(test)

    # pd.DataFrame(results).to_csv('spark_ml_results.csv')
    schema = types.StructType([
        types.StructField('Test', types.StringType()),
        types.StructField('Test Number', types.IntegerType()),
        types.StructField('Total', types.DoubleType()),
        types.StructField('Average', types.DoubleType())
    ])

    results_rdd = sc.parallelize(results, numSlices=1)
    spark.createDataFrame(data=results_rdd, schema=schema).write.csv(output, header=True, mode='overwrite')


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('spark ml tests').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input, output)
