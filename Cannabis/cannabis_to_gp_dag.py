import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

import datetime

path_to_main_script = '<path_to_main_py>'

with DAG (
    dag_id = 'cannabis_to_gp_dag',
    default_args = {
        'owner': 'airflow',
	'start_date': datetime.datetime(2023, 2, 19),
	'email': '<your_email>',
	'email_on_failure': True,
	'depends_on_past': False
    },
    schedule_interval = '0 */12 * * *',
    catchup = False,
    dagrun_timeout = datetime.timedelta(days = 1),
    max_active_runs = 1,
    tags = ['test']
) as dag:

    start = DummyOperator(
        task_id = 'start',
	dag = dag
    )

    main = BashOperator(
	task_id = 'main',
	bash_command = f'/usr/local/basement/Python-3.8.7/bin/python3.8 {path_to_main_script}/main.py',
	dag = dag
    )

    end = DummyOperator(
        task_id = 'end',
	dag = dag
    )
    

start >> main >> end