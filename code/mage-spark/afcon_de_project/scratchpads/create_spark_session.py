import pyspark
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


credentials_location = "/home/src/keys/my-creds.json"
GCS_connector = "/home/src/gcp_jars/gcs-connector-hadoop3-2.2.5.jar" 
GBQ_connector = "/home/src/gcp_jars/spark-3.3-bigquery-0.36.1.jar"
conf_jars = f"{GCS_connector},{GBQ_connector}"
temp_GCS_Bucket = "cloud_bucket_dbt"


# conf = SparkConf() \
# .setMaster('spark://de-zoomcamp.europe-west1-b.c.primeval-legacy-412013.internal:7077') \
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


# spark = (
#         SparkSession
#         .builder
#         .config(conf=sc.getConf())
#         .getOrCreate()
#          )

GCS_connector = "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.37.0.jar" 
GBQ_connector = "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
conf_jars = f"{GCS_connector},{GBQ_connector}"

conf = SparkConf() \
.set("spark.jars", conf_jars) 

sc = SparkContext.getOrCreate(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

spark.stop()