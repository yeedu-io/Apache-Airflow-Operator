from setuptools import setup, find_packages
import codecs
import re
import os.path
here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    return codecs.open(os.path.join(here, *parts), 'r').read()


setup(
    name='airflow-yeedu-operator',
    version='1.0.2',
    description='Submission and monitoring of jobs using the Yeedu API in Apache Airflow. ',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Yeedu',
    author_email='yeedu@modak.com',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.5.0',
        'requests>=2.27',
    ],
    project_urls={
        'GitHub': 'https://github.com/yeedu-io/Apache-Airflow-Operator',
    },
    license='All Rights Reserved',

)
