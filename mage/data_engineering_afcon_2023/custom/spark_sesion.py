if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
    
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import os 

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/my-creds.json"



required_jars = [
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar"
    ]

@custom
def transform_custom(*args, **kwargs):
    spark = (
        SparkSession.builder
        .master("local")
        .appName('afcon_trnsformation_and_modeling')
        .config("spark.jars", ",".join(required_jars))
        .getOrCreate()
    )

    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    kwargs['context']['spark'] = spark



    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
