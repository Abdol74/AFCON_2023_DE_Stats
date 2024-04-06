from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from datetime import datetime, timedelta
from pyspark.sql.types import *
import random
from os import path
import os
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):

    # credentials_location = "/home/src/keys/my-creds.json"

    # GCS_connector = "/home/src/gcp_jars/gcs-connector-hadoop3-2.2.5.jar" 
    # GBQ_connector = "/home/src/gcp_jars/spark-3.3-bigquery-0.36.1.jar"

    # GCS_connector = "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.37.0.jar" 
    # GBQ_connector = "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
    # conf_jars = f"{GCS_connector},{GBQ_connector}"

    # temp_GCS_Bucket = "cloud_bucket_dbt"

    # conf = SparkConf() \
    # .setMaster('local[*]') \
    # .setAppName('load_spark_to_bq_test_mage') \
    # .set("spark.jars", conf_jars) \
    # .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    # .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    # .set("temporaryGcsBucket",temp_GCS_Bucket)

    # sc = SparkContext.getOrCreate(conf=conf)
    # hadoop_conf = sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    # hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    # hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    # hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    kwargs['context']['spark'] = spark
    
    spark = kwargs.get('spark')
    # print(spark)
    # print(spark.version)
    
    # # print(spark)
    # # print(spark.conf)
    # app_name = spark.sparkContext.appName
    # print(f"Application Name: {app_name}")
    # # print(f"spark context is: {spark.sparkContext}")
    # spark.stop()

    for key, value in spark.sparkContext.getConf().getAll():
        print(f"{key}: {value}")

        


    # config_path = path.join(get_repo_path(), 'io_config.yaml')
    # config_profile = 'default'

    # bucket_name = 'cloud_bucket_dbt'
    # object_key = 'match_events.parquet'

    # events_df = spark.read.parquet("gs://cloud_bucket_dbt/match_events.parquet")
    return 1



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'










