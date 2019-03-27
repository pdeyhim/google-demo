from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow import DAG

from datetime import datetime, timedelta
from base64 import b64encode as b64e

import pandas as pd

import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 3, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG('bq_publish_to_pubsub_dev',
          default_args=default_args,
          catchup=True,
          schedule_interval='*/5 * * * *')

##gcp_project = "deyhim-sandbox"
##pubsub_topic = "splunk"
##detection_query = '''
##SELECT *
##    FROM
##         realtime_agg.random_data_raw
##         realtime_agg.random_data_raw
##    WHERE
##        ts > TIMESTAMP "{{ prev_execution_date.strftime("%Y-%m-%d %H:%M:%S") }}"
##    AND
##        ts <= TIMESTAMP "{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}"
##'''

detection_query = Variable.get("detection_query")
gcp_project = Variable.get("detection_logs_project")
pubsub_topic = Variable.get("detection_pubsub_topic")

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def bq_to_pubsub_query_executor(**kwargs):
    """Executes a custom detector query in BigQuery and passes the results to the next task"""

    query = kwargs['templates_dict']['query']
    logging.info(query)
    bigquery_hook = BigQueryHook(use_legacy_sql=False)
    df = bigquery_hook.get_pandas_df(sql=query)

    messages = [{'data': b64e(row.to_json().encode()).decode()} for index, row in df.iterrows()]

    """splitting the array to 1000 size chunks (PubSub limit)"""
    messages_chunks = chunks(messages, 1000)
    pubsub_hoook = PubSubHook()
    for chunk in messages_chunks:
        pubsub_hoook.publish(project=gcp_project, topic=pubsub_topic, messages=chunk)


t1 = PythonOperator(task_id="bq_to_pubsub_query_executor", python_callable=bq_to_pubsub_query_executor,
                    templates_dict={'query': detection_query}, dag=dag)


t1
