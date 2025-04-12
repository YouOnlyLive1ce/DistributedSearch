#!/bin/bash
# 6min total time
source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

# remove previous data
hadoop fs -rm -r /data

# download a.parquet with kaggle cli
export KAGGLE_CONFIG_DIR=/app
kaggle datasets download -d jjinho/wikipedia-20230701 -f ./a.parquet -p /app/data
file data/a.parquet #zip file
unzip -o /app/data/a.parquet -d /app/data/
file data/a.parquet #parquet file

# register data in hdfs = slaves dockers
hdfs dfs -mkdir -p /data 
hdfs dfs -put -f ./data/a.parquet /data/

# create /app/data/*.txt files, choose columns
spark-submit prepare_data.py 
echo "Putting data to hdfs"
hdfs dfs -put data / 

# remove parquet from master?

hdfs dfs -ls /data 
hdfs dfs -ls /index/data
hdfs dfs -cat /data/9983283_A_Good_Enough_Day.txt
echo "done data preparation!"