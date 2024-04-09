if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from datetime import datetime, timedelta
from pyspark.sql.types import *
import random

required_jars = [
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar"]
bucket_name = os.environ['BUCKET_NAME']
project_id = os.environ['PROJECT_ID']
dataset_name = os.environ['DATASET']
matches_columns = [
    
    'match_id',
    'home_team_id',
    'away_team_id',
    'home_manager_id',
    'away_manager_id',
    'stadium_id',
    'referee_id',
    'match_date',
    'match_week',
    'kick_off',
    'competition_stage',
    'home_score',
    'away_score'
    
]

def create_spark_session(required_jars, kwargs):
    spark = (
        SparkSession.builder
        .master("local")
        .appName('matches_fact')
        .config("spark.jars", ",".join(required_jars))
        .config("temporaryGcsBucket", bucket_name)
        .getOrCreate()
    )

    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])


    kwargs['context']['spark'] = spark

    return kwargs

def read_table_from_bq(spark, table_name):
    df = spark.read \
              .format('bigquery') \
              .option('parentProject', project_id) \
              .option('table', '{}:{}.{}'.format(project_id, dataset_name, table_name)) \
              .load()

    return df


def read_pq_file_from_gcs(spark, file_name):
    df = spark.read.parquet("gs://{}/{}".format(bucket_name, file_name))
    return df


def write_dataframe_into_bigquery(df, table_name):
    try:
        df.write \
          .mode("overwrite") \
          .format("bigquery") \
          .option("parentProject",project_id) \
          .option("table", "{}.{}.{}".format(project_id, dataset_name, table_name)) \
          .save()

        print("DONE WRITING {} INTO {}".format(table_name, dataset_name))
    except e:
        print(e)

@custom
def transform_custom(*args, **kwargs):

    kwargs = create_spark_session(required_jars, kwargs)
    
    spark = kwargs['context']['spark']
    matches_df = read_pq_file_from_gcs(spark, "acfon_matches.parquet")

    team_dim = read_table_from_bq(spark, "team_dim")

    team_dim = broadcast(team_dim)

    matches_df = matches_df.join(team_dim, col("home_team") == team_dim.team, how='leftouter') \
                        .drop('team') \
                        .withColumnRenamed('team_id', 'home_team_id') \
                        .join(team_dim, col("away_team") == team_dim.team, how='leftouter') \
                        .drop('team') \
                        .withColumnRenamed('team_id', 'away_team_id')

    
    manager_dim = read_table_from_bq(spark, 'manager_dim')

    manager_dim = broadcast(manager_dim)

    matches_df = matches_df.join(manager_dim, col("home_managers") == manager_dim.manager, how='leftouter') \
                        .drop("manager") \
                        .withColumnRenamed("manager_id", "home_manager_id") \
                        .join(manager_dim , col("away_managers") == manager_dim.manager, how='leftouter') \
                        .drop("manager") \
                        .withColumnRenamed("manager_id", "away_manager_id")

    stadium_dim = read_table_from_bq(spark, "stadium_dim")

    matches_df = matches_df.join(broadcast(stadium_dim), on='stadium', how='leftouter') \
                        .drop('stadium')


    referee_dim = read_table_from_bq(spark, "referee_dim")

    matches_df = matches_df.join(broadcast(referee_dim), on='referee', how='leftouter') \
                        .drop('referee')

    matches_df = matches_df.select(matches_columns)

    matches_df = matches_df.withColumn("match_id", matches_df.match_id.cast(IntegerType())) \
                      .withColumn("home_team_id", matches_df.home_team_id.cast(IntegerType())) \
                      .withColumn("away_team_id", matches_df.away_team_id.cast(IntegerType())) \
                      .withColumn("home_manager_id", matches_df.home_manager_id.cast(IntegerType())) \
                      .withColumn("away_manager_id", matches_df.away_manager_id.cast(IntegerType())) \
                      .withColumn("stadium_id", matches_df.stadium_id.cast(IntegerType())) \
                      .withColumn("referee_id", matches_df.referee_id.cast(IntegerType())) \
                      .withColumn("match_date", matches_df.match_date.cast(DateType())) \
                      .withColumn("match_week", matches_df.match_week.cast(IntegerType())) \
                      .withColumn("kick_off", matches_df.kick_off.cast(TimestampType())) \
                      .withColumn("home_score", matches_df.home_score.cast(IntegerType())) \
                      .withColumn("away_score", matches_df.away_score.cast(IntegerType()))
    
    matches_df = matches_df.withColumn('goals_scored', col("home_score") + col("away_score"))

    matches_df = matches_df.withColumn('penalties_finished', \
                when((col("competition_stage") == "Group Stage"), 'No_Penalties_Allowed') \
                .when((col("competition_stage") != "Group Stage") & (col("goals_scored") == 0), 'Penalties_Finished') \
                .otherwise("No_Penalties_Finished"))

        
    match_dim = matches_df.select(
                'match_id',
                'match_date',
                'match_week',
                'kick_off',
                'competition_stage'
        )

    
    match_fact = matches_df.select(

            'match_id',
            'home_team_id',
            'away_team_id',
            'home_manager_id',
            'away_manager_id',
            'stadium_id',
            'referee_id',
            'home_score',
            'away_score',
            'goals_scored',
            'penalties_finished'
    )


    write_dataframe_into_bigquery(match_dim, "match_dim")

    write_dataframe_into_bigquery(match_fact, "match_fact")

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
