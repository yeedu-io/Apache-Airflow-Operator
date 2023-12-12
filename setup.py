from setuptools import setup, find_packages

setup(
    name='airflow-yeedu-operator',
    version='1.0.1',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.5.0',
        'requests>=2.27',
        # Add any other dependencies here
    ],
    project_urls={
        'GitHub': 'https://github.com/yeedu-io/Apache-Airflow-Operator',
        # Add more URLs as needed
    },
)

