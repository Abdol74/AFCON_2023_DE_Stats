from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
import os 
import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter



@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:

    project_id = os.environ['PROJECT_ID']
    dataset_name = os.environ['DATASET']
    event_fact_table_name = 'event_fact'

    df \
    .write \
    .mode("overwrite") \
    .format("bigquery") \
    .option("table", "{}.{}.{}".format(project_id, dataset_name, event_fact_table_name)) \
    .save()