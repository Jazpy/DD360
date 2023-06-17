from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from handler import update_all

args = {
    'owner': 'JMT',
    'start_date': days_ago(1)
}

#defining the dag object
dag = DAG(dag_id='simple-dag', default_args=args, schedule_interval='@hourly')

#assigning the task for our dag to do
with dag:
    update_reports = PythonOperator(task_id='update_reports',
        python_callable=update_all)
