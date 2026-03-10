from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.decorators import task #- or use `'airflow.sdk.task'`
from datetime import datetime, timedelta

# Define connection IDs for Airflow connections
DATABRICKS_CONN_ID='databricksconnection'
MEDALLION_JOB_ID=829906787936123
EXPORT_TO_POSTGRES_ID=703194797797452

# default_args for the DAG
default_args = {
  'owner': 'airflow',
  'start_date': datetime.now() - timedelta(days=1)
}

# create the DAG
with DAG(dag_id='databricks_etl',
         schedule = None,  # Triggered manually,
         default_args = default_args
        ) as dag:
    
#Task1 - Run the Databricks medallion job
  medallion_job = DatabricksRunNowOperator(
    task_id = 'run_databricks_job',
    databricks_conn_id = DATABRICKS_CONN_ID,
    job_id = MEDALLION_JOB_ID
  )

#Task2 - Run the Databricks export to postgres job
  export_job = DatabricksRunNowOperator(
    task_id="export_gold_to_postgres",
    databricks_conn_id = DATABRICKS_CONN_ID,
    job_id=EXPORT_TO_POSTGRES_ID
  )

## DAG Worflow
medallion_job >> export_job