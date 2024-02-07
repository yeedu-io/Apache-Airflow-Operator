# Airflow Yeedu Operator
[![PyPI version](https://badge.fury.io/py/airflow-yeedu-operator.png)](https://badge.fury.io/py/airflow-yeedu-operator)

## Installation

To use the Yeedu Operator in your Airflow environment, install it using the following command:

```bash
pip3 install airflow-yeedu-operator
```

# DAG: Yeedu Job Execution

## Overview

The `YeeduJobRunOperator` in this DAG facilitates the submission and monitoring of jobs using the Yeedu API in Apache Airflow. This DAG enables users to execute Yeedu jobs and handle their completion status and logs seamlessly within their Airflow environment.

## Prerequisites

Before using this DAG, ensure you have:

- Access to the Yeedu API.
- Proper configuration of Airflow with required connections and variables (if applicable).

## Usage

### DAG Initialization

Import the necessary modules and instantiate the DAG with required arguments and schedule interval.

```python
from datetime import datetime, timedelta
from airflow import DAG
from yeedu.operators.yeedu import YeeduJobRunOperator

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate DAG
dag = DAG(
    'yeedu_job_execution',
    default_args=default_args,
    description='DAG to execute jobs using Yeedu API',
    schedule_interval='@once',
    catchup=False,
)
```
### Task Initialization

Create tasks using `YeeduJobRunOperator` to perform various Yeedu API operations.
# Define YeeduJobRunOperator tasks

```python
    submit_job_task = YeeduJobRunOperator(
    task_id='submit_yeedu_job',
    job_conf_id='your_job_config_id',  # Replace with your job config ID
    token='your_yeedu_api_token',  # Replace with your Yeedu API token
    hostname='yeedu.example.com',  # Replace with your Yeedu API hostname
    workspace_id=123,  # Replace with your Yeedu workspace ID
    dag=dag,
)
```
### Execution

To execute this DAG:

1. Ensure all required configurations (job config ID, API token, hostname, workspace ID) are correctly provided in the task definitions.
2. Place the DAG file in the appropriate Airflow DAGs folder.
3. Trigger the DAG manually or based on the defined schedule interval.
4. Monitor the Airflow UI for task execution and logs.

