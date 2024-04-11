from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
import os
import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from datetime import datetime, timedelta
from pyspark.sql.types import *
import random
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

bucket_name = os.environ['BUCKET_NAME']
project_id = os.environ['PROJECT_ID']
dataset_name = os.environ['DATASET']

required_jars = [
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.36.1.jar"
    ]

def create_spark_session(required_jars, kwargs):
    spark = (
        SparkSession.builder
        .master("local")
        .appName('afcon_trnsformation_and_modeling')
        .config("spark.jars", ",".join(required_jars))
        .config("temporaryGcsBucket", bucket_name)
        .getOrCreate()
    )

    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])


    kwargs['context']['spark'] = spark

    return kwargs

def create_hash(player_id, match_id, team_id, event, timestamp):
  return f"{player_id}_{match_id}_{team_id}_{event}_{timestamp}"


def generate_random_int_not_in_list(team_id,x_list):
    if team_id is None:
        while True:
            random_int = random.randint(1, 1000)
            if random_int not in x_list:
                return int(random_int)
    else:
        return int(team_id)


def create_team_dimension(spark, events_df):

    team_dim = spark.sql("""
            SELECT DISTINCT 
            home_team 
            FROM Matches
            """)

    
    
    team_dim = team_dim.withColumnRenamed('home_team','team')
    
    team_dim  = team_dim.join(events_df.select('team_id','team').distinct(),on='team',how='leftouter')

    team_dim = team_dim.withColumn('team_id',team_dim.team_id.cast(IntegerType()))

    team_dim = team_dim.select('team_id', 'team')

    excluded_teams = team_dim.select(collect_list('team_id')).collect()[0][0]

  
    excluded_teams_ids = spark.sparkContext.broadcast(excluded_teams)


    generate_random_int_udf= udf(lambda x: generate_random_int_not_in_list(x, excluded_teams_ids.value),IntegerType())

    team_dim = team_dim.withColumn("team_id", generate_random_int_udf("team_id"))

    return team_dim


def create_stadium_dimension(spark):

    stadium_dim = spark.sql("""
            SELECT DISTINCT stadium
            FROM Matches
    """)

    stadium_dim = stadium_dim.withColumn('stadium_id',monotonically_increasing_id())

    stadium_dim = stadium_dim.select('stadium_id', 'stadium')

    return stadium_dim


def create_referee_dimension(spark):

    referee_dim = spark.sql("""
                    SELECT DISTINCT referee
                    FROM Matches
            """)

    referee_dim = referee_dim.withColumn('referee_id', monotonically_increasing_id())

    referee_dim = referee_dim.select('referee_id', 'referee')
    
    return referee_dim


def create_manager_dimension(spark):
    manager_dim = spark.sql("""
                    SELECT DISTINCT home_managers 
                    FROM Matches
            """)
    manager_dim = manager_dim.withColumn("manager_id", monotonically_increasing_id())

    manager_dim = manager_dim.withColumnRenamed('home_managers', 'manager')

    return manager_dim


def create_player_dimension(spark):
    player_dim = spark.sql("""

            SELECT DISTINCT player_id,player
            FROM events
    """)

    player_dim = player_dim.withColumn('player_id',player_dim.player_id.cast(IntegerType()))

    player_dim = player_dim.filter(player_dim.player_id.isNotNull())

    return player_dim


def create_event_fact(spark, events_df, team_dim):

    ### pre-processing: filter the fact from api for unkoown players
    event_fact = events_df.filter(events_df.player_id.isNotNull())

    ### drop the player name from fact table --> Will get from player dimension
    event_fact = event_fact.drop(event_fact.player)

    ### We're changed the team ids in team dimension so will get from there
    event_fact = event_fact.drop('team_id')
    event_fact = event_fact.join(team_dim, on='team', how='leftouter')
    event_fact = event_fact.drop('team')
    event_fact = event_fact.withColumnRenamed('team_id', 'team_event_id')

    ### get possession team ids from team dimension
    event_fact = event_fact.join(team_dim, col("possession_team") == team_dim.team, how='leftouter') \
                         .drop(team_dim.team) \
                         .drop("possession_team") \
                         .withColumnRenamed("team_id","possession_team_id")

    event_fact = event_fact.withColumnRenamed('team_event_id', 'team_id')

    event_fact = event_fact.drop('source')

    ### create surregoate key for the fact table based on player_id, match_id, team_id, event_type, event_timestamp 
    hash_cols = ["player_id","match_id","team_id","type","timestamp"]
    create_hash_udf = udf(create_hash, StringType())

    event_fact = event_fact.withColumn("event_id", md5(create_hash_udf(*hash_cols)))

    event_fact.createOrReplaceTempView('event_fact')

    ### DROP THE DUPLICATED EVENTS 

    event_fact = spark.sql("""
    SELECT *
    FROM (
            SELECT 
                row_number() OVER(PARTITION BY EVENT_ID ORDER BY EVENT_ID) AS ROW_NUM,
                *
            FROM event_fact)L1
    WHERE L1.ROW_NUM = 1
        
    """)


    ### RENAMING COLUMNS
    event_fact = event_fact.withColumnRenamed('type', 'event_type') \
                         .withColumnRenamed('timestamp', 'event_timestamp') \
                         .withColumnRenamed('minute', 'event_minute') \
                         .withColumnRenamed('location', 'event_location')

    ### SELECT THE DESIRED COLUMNS
    event_fact = event_fact.select(
                
            'event_id',
            'player_id',
            'match_id',
            'team_id',
            'event_type',
            'event_timestamp',
            'event_minute',
            'event_location',
            'play_pattern', 
            'position',
            'pass_outcome',
            'pass_cross',
            'pass_goal_assist',
            'pass_shot_assist',
            'foul_committed_type',
            'foul_committed_card',
            'foul_committed_offensive',
            'foul_won_defensive',
            'foul_committed_penalty',
            'foul_won_penalty',
            'shot_outcome',
            'interception_outcome',
            'possession',
            'possession_team_id'       
    )


    event_fact = event_fact.withColumn("zone",
    when(col("event_location")[0] >= 80, "Attack")
    .when((col("event_location")[0] >= 40) & (col("event_location")[0] < 80), "Midfield")
    .otherwise("Defense"))


    event_fact = event_fact.withColumn('player_id', event_fact.player_id.cast(IntegerType())) \
                      .withColumn('match_id', event_fact.match_id.cast(IntegerType())) \
                      .withColumn('event_timestamp', event_fact.event_timestamp.cast(TimestampType())) \
                      .withColumn('event_location', event_fact.event_location.cast(StringType()))

    return event_fact

def write_dataframe_into_bigquery(df, table_name, partitioned, partitioned_column):
    try:
        if partitioned == True:
            
            df.write \
            .mode("overwrite") \
            .format("bigquery") \
            .option("temporaryGcsBucket",bucket_name) \
            .option("parentProject", project_id) \
            .option("partitionField", partitioned_column) \
            .option("partitionRangeStart", 3920384) \
            .option("partitionRangeEnd", 3923881) \
            .option("partitionRangeInterval", 1) \
            .option("spark.sql.sources.partitionOverwriteMode", "STATIC") \
            .option("table", "{}.{}.{}".format(project_id, dataset_name, table_name)) \
            .save()
        else:
            df.write \
                .mode("overwrite") \
                .format("bigquery") \
                .option("parentProject", project_id) \
                .option("table", "{}.{}.{}".format(project_id, dataset_name, table_name)) \
                .save()

        print("DONE WRITING {} INTO {}".format(table_name, dataset_name))

    except e:
        print(e)

@data_loader
def load_from_google_cloud_storage(*args, **kwargs):

    kwargs = create_spark_session(required_jars, kwargs)

    spark = kwargs['context']['spark']
    

    # ###########################################
    matches_file_name = "acfon_matches.parquet"
    events_file_name = "match_events.parquet"
    # ##########################################
    matches_df = spark.read.parquet("gs://{}/{}".format(bucket_name, matches_file_name))
    events_df = spark.read.parquet("gs://{}/{}".format(bucket_name, events_file_name))
    # ################################################################################
    matches_df.registerTempTable("Matches")
    events_df.registerTempTable("events")
    # ################################################################################


    #### CALLING FUNCTION TO CREATE TEAM DIMENSION #######
    team_dim = create_team_dimension(spark, events_df)

    write_dataframe_into_bigquery(team_dim, 'team_dim',False, None)
    ##### CALLING FUNCTION TO CREATE STADIUM DIMENSION #####

    stadium_dim = create_stadium_dimension(spark)

    write_dataframe_into_bigquery(stadium_dim, 'stadium_dim', False, None)
    ##### CALLING FUNCTION TO CREATE REFEREE DIMENSION ####

    referee_dim = create_referee_dimension(spark)

    write_dataframe_into_bigquery(referee_dim, 'referee_dim', False, None)

    #### CALLING FUNCTION TO CREATE MANAGER DIMENSION
    manager_dim = create_manager_dimension(spark)
    write_dataframe_into_bigquery(manager_dim, 'manager_dim', False, None)

    #### CALLING FUNCTION TO CREATE PLAYER DIMENSION ####

    player_dim = create_player_dimension(spark)
    
    write_dataframe_into_bigquery(player_dim, 'player_dim', False, None)

    ### CALL FUNCTION TO CREATE EVENT FACT TABLE
    event_fact = create_event_fact(spark, events_df, team_dim)

    
    write_dataframe_into_bigquery(event_fact, 'event_fact', True, 'match_id')


    return {}
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
