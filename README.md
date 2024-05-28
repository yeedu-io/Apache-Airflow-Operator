# Airflow Yeedu Operator
[![PyPI version](https://badge.fury.io/py/airflow-yeedu-operator.png)](https://badge.fury.io/py/airflow-yeedu-operator)

### Installation

To use the Yeedu Operator in your Airflow environment, install it using the following command:

```bash
pip3 install airflow-yeedu-operator
```

### Overview

The `YeeduOperator` acts as a bridge within Airflow, allowing you to effortlessly interact with both Yeedu jobs and notebooks. It streamlines the process of:

- **Submitting Jobs and Notebooks**: You can directly send jobs and notebooks to Yeedu using this operator, integrating them seamlessly into your Airflow workflows.
- **Monitoring Progress**: The operator keeps you informed about the status of your submitted Yeedu jobs and notebooks, providing you with real-time updates.
- **Handling Completion**: Upon completion, the operator gracefully handles the outcome (success or failure) for both jobs and notebooks.
- **Managing Logs**: All relevant logs associated with Yeedu jobs and notebooks are conveniently accessible within Airflow, keeping your workflow environment organized.


### Prerequisites

Before using the YeeduOperator, ensure you have the following:

- **Access to the Yeedu API**: You'll need valid credentials to interact with the Yeedu API.
- **Creating Airflw Connection (For Yeedu Credentials)**:

<img src="images/yeedu_connection.png" alt="Airflow Connection" width="800" height="600" />


 **Steps to Create an Airflow Connection**

1. **Navigate to Connections**:
   - In the Airflow UI, click on the `Admin` tab at the top of the screen.
   - From the dropdown menu, select `Connections`.

2. **Create a New Connection**:
   - On the Connections page, click the `+` (plus) button to add a new connection.

3. **Fill in the Connection Details**:
   - **Conn Id**: A unique identifier for your connection. Example: `my_yeeduu_connection`.
   - **Conn Type**: Select the appropriate type for your connection. For a Yeedu Notebook or job, `HTTP` is appropriate.
   - **Host**: Yeedu hostname.
   - **Login**: The username for the connection.
   - **Password**: The password for the connection.

5. **Extra**:
   - Click on the `Extra` field to expand it. This field allows you to provide additional parameters in JSON format. 

   ```json
   {
       "YEEDU_AIRFLOW_VERIFY_SSL": "true",
       "YEEDU_SSL_CERT_FILE": "/path/to/cert/file"
   }


### DAG: Yeedu Job Execution

- #### Setting Up the DAG

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
- #### Creating Yeedu Operator Tasks

    Create tasks using `YeeduOperator` to perform various Yeedu API operations.

    ```python

        submit_job_task = YeeduOperator(
            task_id='demo_dag',
            job_url='https://hostname/tenant/tenant_id/workspace/workspace_id/spark/notebook/notebook_id', # Replace with job or notebook url
            connection_id='yeedu_connection', # Replace with your connection id
            dag=dag,
        )

    ```

- #### Execution

    To execute this DAG:

    1. Ensure all required configurations (job_url, connection_id) are correctly provided in the task definition.
    2. Place the DAG file in the appropriate Airflow DAGs folder.
    3. Trigger the DAG manually or based on the defined schedule interval.
    4. Monitor the Airflow UI for task execution and logs.



