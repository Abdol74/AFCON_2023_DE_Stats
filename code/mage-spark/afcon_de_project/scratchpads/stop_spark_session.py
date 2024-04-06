
def stop_spark_session(**kwargs):
    spark = kwargs.get('spark')
    print(spark)
    if spark:
        spark.stop()
    print(spark)