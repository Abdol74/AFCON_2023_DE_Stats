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
def create_player_aggregated_fact(*args, **kwargs):

    kwargs = create_spark_session(required_jars, kwargs)
    
    spark = kwargs['context']['spark']

    player_dim = read_table_from_bq(spark, 'player_dim')
    player_dim.createOrReplaceTempView('player_dim')

    event_fact = read_table_from_bq(spark, 'event_fact')
    event_fact.createOrReplaceTempView('event_fact')

    player_aggregated_fact = spark.sql("""
    
            SELECT 
                    E.player_id as player_id,
                    P.player as player,
                    COUNT(CASE WHEN E.event_type = 'Pass' THEN 1 ELSE NULL END) AS total_passes,
                    COUNT(CASE WHEN E.event_type = 'Dribble' THEN 1 ELSE NULL END) AS total_dribble,
                    COUNT(CASE WHEN E.event_type = 'Shot' THEN 1 ELSE NULL END) AS total_shots,
                    COUNT(CASE WHEN E.event_type = 'Block' THEN 1 ELSE NULL END) AS total_blocks,
                    COUNT(CASE WHEN E.event_type = 'Interception' THEN 1 ELSE NULL END) AS total_interceptions,
                    COUNT(CASE WHEN E.pass_outcome = 'Out' THEN 1 ELSE NULL END) AS total_out_passes,
                    COUNT(CASE WHEN E.pass_outcome = 'Incomplete' THEN 1 ELSE NULL END) AS total_incomplete_passes,
                    COUNT(CASE WHEN E.pass_outcome = 'Pass Offside' THEN 1 ELSE NULL END) AS total_offside_passes,
                    COUNT(CASE WHEN E.pass_goal_assist = true THEN 1 ELSE NULL END) as total_pass_goal_assist,
                    COUNT(E.foul_committed_type) AS total_fouls,
                    COUNT(CASE WHEN E.foul_committed_card = 'Yellow Card' OR E.foul_committed_card = 'Second Yellow' THEN 1 ELSE NULL END ) AS total_yellow_cards,
                    COUNT(CASE WHEN E.foul_committed_card = 'Red Card' THEN 1 ELSE NULL END) AS total_red_cards,
                    COUNT(CASE WHEN E.shot_outcome = 'Goal' THEN 1 ELSE NULL END) AS total_goals,
                    COUNT(CASE WHEN E.shot_outcome IN('Off T', 'Saved', 'Blocked', 'Saved Off Target', 'Saved to Post', 'Wayward', 'Post') THEN 1 ELSE NULL END) AS total_outside_shots,
                    COUNT(CASE WHEN E.interception_outcome IN ('Won', 'Success In Play', 'Success Out') THEN 1 ELSE NULL END) AS total_succcessfully_interceptions,
                    COUNT(CASE WHEN E.interception_outcome IN ('Lost Out', 'Lost In Play') THEN 1 ELSE NULL END) AS total_wrong_interceptions 
            FROM 
                event_fact E
                INNER JOIN 
                player_dim P
                ON 
                E.player_id = P.player_id
            GROUP BY 
                E.player_id, P.player;
    """)

    write_dataframe_into_bigquery(player_aggregated_fact, 'player_aggregated_fact')

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
