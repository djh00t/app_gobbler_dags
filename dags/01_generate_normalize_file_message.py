###
### Receive webhook from S3 and generate normalize file message
###
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import requests
import json
from datetime import datetime
