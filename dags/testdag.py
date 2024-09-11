from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a Python function to be run by the DAG
def hello_world():
    print("Hello, World!")

# Define default arguments for the DAG
default_args = {
    'start_date': datetime(2023, 1, 1),  # Adjust to your needs
}

# Create the DAG
with DAG(
    'hello_world_dag',  # Name of the DAG
    default_args=default_args,
    schedule_interval=None,  # Set to None if you want to trigger manually
    catchup=False,  # Skip backfilling for missed intervals
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id='hello_world_task',  # Name of the task
        python_callable=hello_world  # The Python function to call
    )

hello_task
# If needed, add more tasks or dependencies
# hello_task >> some_other_task
