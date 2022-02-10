# Rapids Blog Post Repository

This repo contains the entire code used in our comparisons between Rapids - Scikit Learn - Spark - Pandas that are featured here: https://medium.com/sfu-cspmp/rapids-the-future-of-gpu-data-science-9e0524563019

# The Code

## ETL Timing Viz.ipynb \ ETL Cost.ipynb \ ML Timing Viz.ipynb \ ML Cost.ipynb

These notebooks create the graphs used in the Blog Post.  

## Rapids ETL Timing.ipynb \ Rapids ML Timing.ipynb \ Pandas Timing.ipynb \ Scikit-Learning Timing.ipynb

These notebooks perform the experiments and record the timings.  The link for the data for these notebooks is located below.

## create_ml_data.py

This script creates the data for the ML experiments (also located below - see ml_data.zip)

## spark_ml_data_subsets_creation.py \ spark_etl_subsets_creation.py

These scripts are PySpark scripts to partition the data for spark to increase the experiment speed.

## spark_ml_tests.py \ spark_etl_tests.py

These PySpark scripts execute the experiments using spark.  See below for submission instructions:

- spark-submit spark_etl_tests.py bc_air_monitoring_stations.csv spark_etl_test_subsets spark_etl_results
- spark-submit spark_ml_tests.py spark_ml_test_subsets spark_ml_results

# The Data

All data used can be found here: https://1sfu-my.sharepoint.com/:f:/g/personal/avickars_sfu_ca/Erj8utK-OatOiN9aOpWZZGABFWtGZyYPm29KrTQuc8_gWw?e=k5DXO9

The results for AWS are located in "AWS Results" in this repo.

## Blog post shared word doc
https://1sfu-my.sharepoint.com/:w:/g/personal/avickars_sfu_ca/EQZLCAxGg7tLg7NDNfnv8kMBfDZkvtPgg_VzSj_BRKiP0A?e=gbJOZQ
