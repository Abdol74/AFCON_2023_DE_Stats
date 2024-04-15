Mage URL: http://localhost:6789/

Mage Parameterized Execution:

Pipelines depend on variables or parameters to execute.
Example:
If execution date is in JAN, load to BigQuery located in ASIA.
If execution date is in FEB, load to BigQuery located in US.
Google Cloud Permissions to Deploy Mage on Google Cloud:

Artifact registry reader
Artifact registry writer
Cloud Run developer
Cloud SQL admin
Service account token creator
Run Up/Down Stream DBT Model with Variable Through CLI:

dbt build --select +fact_trips+ --vars '{"is_test_run": "false"}'
Execute PySpark in Jupyter:

Export PYTHONPATH:
bash
Copy code
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
Create Spark Standalone Cluster:
Go to Spark installation directory.
Execute ./sbin/start-master.sh to create Spark master.
Execute ./sbin/start-worker spark_master_url to start worker.
Build Mage with Spark Docker Image:

Download Dockerfile.
Build the image: docker build -t mage_spark .
Start Docker container:
bash
Copy code
docker run -it -d --name mage_spark -p 6789:6789 -v /home/abdol/AFCON_2023_DE_Stats/code/mage-spark:/home/src mage_spark \
/app/run_app.sh mage start afcon_de_project
Download Google BigQuery Connector:

bash
Copy code
gsutil cp gs://spark-lib/bigquery/spark-3.3-bigquery-0.36.1.jar /home/abdol/AFCON_2023_DE_Stats/lib
Generate YAML Models for Your DBT Models Using Codegen Package:

jinja
Copy code
{% set models_to_generate = codegen.get_models(directory='staging', prefix='stg_') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}
Run Entire Project with Input Variable from CLI:

dbt run --vars '{"is_test_run": "false"}'