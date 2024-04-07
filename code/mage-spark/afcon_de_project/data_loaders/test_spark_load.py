from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/keys/my-creds.json"
required_jars = [
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar"
    ]


def create_spark_session():


    
    spark = (
        SparkSession.builder
        .master("local")
        .appName('load_to_bigquery')
        .config("spark.jars", ",".join(required_jars))
        .getOrCreate()
    )

   
    # Set GCS credentials if necessary
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    

    # Save the spark session in the context
    kwargs['context']['spark'] = spark

    return spark


def read_parquet_from_gcs(bucket_name, file_name):
    file_path = f'gs://{bucket_name}/{file_name}.parquet'
    df = spark.read.format("parquet").load(source_root_path)
    return df

def register_dataframe_as_view(df, view_name):
    df.createOrReplaceTempView(f'{view_name}')

def create_team_dimension()

@data_loader
def load_data(*args, **kwargs):

    spark = create_spark_session()

    # Set the GCS location to save the data
    bucket_name="cloud_bucket_dbt"
    project_id="primeval-legacy-412013"

    
    source_root_path= f'gs://{bucket_name}/match_events.parquet'
    print(source_root_path)
    
 
    temp_bucket_name = "matchevents"


    df = spark.read.format("parquet").load(source_root_path)
    df.show()
    # spark.stop()
    # (
    #     spark.read
    #     .format("parquet")
    #     #.csv(SparkFiles.get("20240324160000.export.CSV"), schema=schema)
    #     .load(source_root_path)
    #     #.drop("partition_date")
    #     .write
    #     .format('bigquery')
    #     #.option("table", f"{project_id}.{dataset_name}.{table_name}")
    #     .option('parentProject', project_id)
    #     .option("temporaryGcsBucket", temp_bucket_name)
    #     .mode("overwrite")
    #     .partitionBy("Year","week")
    #     .save(f"{project_id}.{dataset_name}.{table_name}")
    # )

    return {}