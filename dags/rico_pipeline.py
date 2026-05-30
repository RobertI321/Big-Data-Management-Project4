from datetime import date, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add slack callback on failure 

# Ingest

def _start_run():
    pass

def _ingest():
    pass

def _parse():
    pass

def _embed_image():
    pass

def _embed_text():
    pass

def _extract():
    pass

def _load():
    pass

def _audit():
    pass


def build_dag():
    dag_id = "pipeline"

    default_args = {
        "retries" : 0,
        #"on_failure_callback" : "",
    }

    with DAG(
        dag_id = dag_id,
        default_args = default_args,
        schedule ="@hourly",
        start_date = datetime.now(),
        catchup = False,
        params={"limit" : 5} 

    ) as dag:
        start_run = PythonOperator(
            task_id = "_start_run",
            python_callable = _start_run
        )
        
        ingest = PythonOperator(
            task_id = "_ingest",
            python_callable = _ingest
        )
        parse = PythonOperator(
            task_id = "_parse",
            python_callable = _parse
        )
        embed_image = PythonOperator(
            task_id = "_embed_image",
            python_callable = _embed_image
        )
        embed_text = PythonOperator(
            task_id = "_embed_text",
            python_callable = _embed_text
        )
        extract = PythonOperator(
            task_id = "_extract",
            python_callable = _extract
        )
        load = PythonOperator(
            task_id = "_load",
            python_callable = _load
        )

        audit = PythonOperator(
            task_id = "_audit",
            python_callable = _audit
        )

        start_run >> ingest >> [parse, embed_image, embed_text] >> extract >> load >> audit
    
    return dag

dag = build_dag()