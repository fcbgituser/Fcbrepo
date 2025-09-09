# models/FCB/staging/store_tests_results.py
from json import loads
import snowflake.connector as sf
from yaml.loader import SafeLoader

def model(dbt, session):
    # Configure materialization (table, view, incremental, etc.)
    dbt.config(materialized="table")
    user = 'FCB_AMIT'
    password = 'FCBTrial@123456'
    account = 'dphbamj-snc95345'
    warehouse = 'RL_FCB_D'
    database = 'TEST_FCB'
    schema= 'DBT_ATIWARI'
    target_path='target'
    with open(rf'{target_path}/run_results.json', 'r') as input_file:
        my_json = input_file.read()
    return {my_json}



