"""DAG to load chargy data"""

import json
from datetime import datetime, timedelta

import xmltodict
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from include.chargy.tasks import _store_values


@dag(start_date=datetime(2024, 1, 1), schedule=timedelta(minutes=4), catchup=False)
def chargy():
    """Load data coming from the chargy connectors"""

    source_data = HttpOperator(
        task_id="fetch_source_data",
        http_conn_id="chargy",
        method="GET",
        do_xcom_push=True,
        response_filter=lambda response: json.dumps(xmltodict.parse(response.text)),
    )

    store_values = PythonOperator(
        task_id="store_values",
        python_callable=_store_values,
        op_kwargs={
            "values": '{{ task_instance.xcom_pull(task_ids="fetch_source_data") }}'
        },
    )

    source_data >> store_values


chargy()
