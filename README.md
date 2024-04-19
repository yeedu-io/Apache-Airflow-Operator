# Airflow Yeedu Operator
[![PyPI version](https://badge.fury.io/py/airflow-yeedu-operator.png)](https://badge.fury.io/py/airflow-yeedu-operator)

## Installation

To use the Yeedu Operator in your Airflow environment, install it using the following command:

```bash
pip3 install airflow-yeedu-operator
```

# DAG: Yeedu Job Execution

## Overview

The `YeeduOperator` in this DAG facilitates the submission and monitoring of jobs and notebooks using the Yeedu API in Apache Airflow. This DAG enables users to execute Yeedu jobs and handle their completion status and logs seamlessly within their Airflow environment.

## Prerequisites

Before using this DAG, ensure you have:

- Access to the Yeedu API.
- Proper configuration of Airflow with required connections and variables (if applicable).

1. Open your shell configuration file (`.bashrc` for Bash).
2. Add the below lines

   ```bash
   export YEEDU_SCHEDULER_USER=example@test.com
   export YEEDU_SCHEDULER_PASSWORD=password

   ```

## Usage

### DAG Initialization

Import the necessary modules and instantiate the DAG with required arguments and schedule interval.

```python
from datetime import datetime, timedelta
from airflow import DAG
from yeedu.operators.yeedu import YeeduOperator

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

Create tasks using `YeeduOperator` to perform various Yeedu API operations.
# Define YeeduOperator tasks

```python

    submit_job_task = YeeduOperator(
        task_id='demo_dag',
        conf_id='config_id',  # Replace with your job config ID or Notebook Config ID
        tenant_id='tenant_id',  # Replace with your Yeedu tenant_id
        base_url='http://hostname:8080/api/v1/',  # Replace with your Yeedu API URL
        workspace_id='your_workspace_id',  # Replace with your Yeedu workspace ID
        dag=dag,
    )

```
### Execution

To execute this DAG:

1. Ensure all required configurations (config ID, API URL, tenant ID, workspace ID) are correctly provided in the task definitions,
and YEEDU_SCHEDULER_USER, YEEDU_SCHEDULER_PASSWORD are added as Environment Variables.
2. Place the DAG file in the appropriate Airflow DAGs folder.
3. Trigger the DAG manually or based on the defined schedule interval.
4. Monitor the Airflow UI for task execution and logs.


