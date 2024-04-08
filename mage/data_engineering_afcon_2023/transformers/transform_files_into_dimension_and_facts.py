if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
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

def create_view_from_df(df, view_name):
    df.registerTempTable(view_name)

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


    generate_random_int_udf= udf(generate_random_int_not_in_list,IntegerType())

    team_dim = team_id.withColumn("team_id", generate_random_int_udf("team_id", excluded_teams_ids.value))

    return team_dim

@transformer
def transform(dfs, *args, **kwargs):
    matches_df = dfs[0]
    events_df = dfs[1]

    spark = kwargs['spark']

    team_dim = spark.sql("""
            SELECT DISTINCT 
            home_team 
            FROM Matches
            """)
    team_dim = team_dim.withColumnRenamed('home_team','team')
    
    team_dim  = team_dim.join(events_df.select('team_id','team').distinct(),on='team',how='leftouter')

    team_dim = team_dim.withColumn('team_id',team_dim.team_id.cast(IntegerType()))

    team_dim = team_dim.select('team_id', 'team')

    team_dim.select('team_id').show()
    # list = team_dim.select('team_id').rdd.flatMap(lambda x: x).collect()
    # spark.sql("SELECT DISTINCT home_team FROM Matches").show()


    # team_dim = create_team_dimension(spark, events_df)
    # team_dim.show(n=5)

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
