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
- **Proper Airflow Configuration**: Make sure Airflow is configured with the necessary connections and variables (if applicable) to connect to Yeedu and Airflow resources.
- **Setting Up Environment Variables (For Bash)**:
  If you're using Bash and prefer to set Yeedu credentials as environment variables, follow these steps:

    1. **Open your shell configuration file**: The default file for Bash is .bashrc. You can edit this file using a text editor of your choice.
    2. **Add environment variables**: Paste the following lines into your .bashrc file, replacing <placeholders> with your actual Yeedu credentials:

    ```bash
    export YEEDU_SCHEDULER_USER=example@test.com
    export YEEDU_SCHEDULER_PASSWORD=password
    export YEEDU_SSL_VERIFICATION=True
    export YEEDU_SSL_CERT_FILE=/path/to/cert/yeedu.crt
    ```

    - `YEEDU_SCHEDULER_USER`: Your Yeedu scheduler username.
    - `YEEDU_SCHEDULER_PASSWORD`: Your Yeedu scheduler password.
    - `YEEDU_SSL_VERIFICATION`: Controls SSL certificate verification for HTTPS connections. Set to `True` to enable SSL verification, or `False` to disable it.
    - `YEEDU_SSL_CERT_FILE`: Path to the SSL certificate file for Yeedu connections.


    3. **Save and source the file**:
      - Save your changes to the .bashrc file.
      - Source the file to apply the changes to your current shell session:

    ```bash
    source ~/.bashrc
    ```


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
            conf_id='config_id',  # Replace with your job config ID or Notebook Config ID
            tenant_id='tenant_id',  # Replace with your Yeedu tenant_id
            base_url='http://hostname:8080/api/v1/',  # Replace with your Yeedu API URL
            workspace_id='your_workspace_id',  # Replace with your Yeedu workspace ID
            dag=dag,
        )

    ```
    **Explanation of Variables:**

- `task_id='demo_dag'`: This variable defines the unique identifier for the task within the Airflow DAG. It's recommended to use a descriptive name that reflects the task's purpose. In this case, `demo_dag` suggests it's a demonstration task.

- `conf_id='config_id'`: This variable specifies the ID of the configuration you want to use with the `YeeduOperator`. It could be either a Yeedu job configuration ID or a notebook configuration ID. Replace `'config_id'` with the actual ID from your Yeedu environment.

- `tenant_id='tenant_id'`: This variable defines your Yeedu tenant ID. Each Yeedu account might be associated with multiple tenants, so it's important to specify the correct one for the job or notebook you want to submit. Replace `'tenant_id'` with your actual tenant ID.

- `base_url='http://hostname:8080/api/v1/'`: This variable sets the base URL for the Yeedu API. It specifies the location where the API endpoints can be accessed. Replace `'http://hostname:8080/api/v1/'` with the actual URL for your Yeedu API endpoint.

- `workspace_id='your_workspace_id'`: This variable defines the ID of the Yeedu workspace where the job or notebook resides. Yeedu workspaces organize your jobs and notebooks. Replace `'your_workspace_id'` with the ID of the relevant workspace.

- `dag=dag`: This variable associates the `submit_job_task` with a specific Airflow DAG object (`dag`). This connection allows the task to be integrated into the workflow defined by the DAG.

- #### Execution

    To execute this DAG:

    1. Ensure all required configurations (config ID, API URL, tenant ID, workspace ID) are correctly provided in the task definitions, and `YEEDU_SCHEDULER_USER`, `YEEDU_SCHEDULER_PASSWORD` ,`YEEDU_SSL_VERIFICATION`, `YEEDU_SSL_CERT_FILE` are added as Environment Variables.
    2. Place the DAG file in the appropriate Airflow DAGs folder.
    3. Trigger the DAG manually or based on the defined schedule interval.
    4. Monitor the Airflow UI for task execution and logs.



