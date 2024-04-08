if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
import os 
import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

@data_exporter
def export_data(data, *args, **kwargs):
    project_id = os.environ['PROJECT_ID']
    dataset_name = os.environ['DATASET']
    event_fact_table_name = 'event_fact'

    data \
    .write \
    .mode("overwrite") \
    .format("bigquery") \
    .option("table", "{}.{}.{}".format(project_id, dataset_name, event_fact_table_name)) \
    .save()


