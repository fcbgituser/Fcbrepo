# models/FCB/staging/store_tests_results.py
from json import loads
import snowflake.connector as sf
from yaml.loader import SafeLoader

# ---------------------------
# HARD-CODED SNOWFLAKE CREDENTIALS (as requested)
# ---------------------------
SF_USER = 'FCB_AMIT'
SF_PASSWORD = 'FCBTrial@123456'
SF_ACCOUNT = 'dphbamj-snc95345'
SF_WAREHOUSE = 'RL_FCB_D'
SF_DATABASE = 'TEST_FCB'
SF_SCHEMA = 'DBT_ATIWARI'
TARGET_TABLE = f"{SF_DATABASE}.{SF_SCHEMA}.DBT_TEST_RESULTS"

with open(rf'{target_path}/run_results.json', 'r') as input_file:
    my_json = input_file.read()

new_json = loads(my_json)

my_tables = []
with open(rf'{dbt_path}/models/source/source.yml') as f:
    my_source = load(f, Loader=SafeLoader)
for i in (range(len(my_source['sources']))):
    for j in (range(len(my_source['sources'][i]['tables']))):
        schema_name = my_source['sources'][i]['name']
        table_name = my_source['sources'][i]['tables'][j]['name']
        result = table_name
        my_tables.append(result)

my_test_names = ['source_not_null','source_table_not_empty', 'not_null', 'relationships']

for i in range(len(new_json['results'])):
    unique_id = new_json['results'][i]['unique_id']
    test_name_model_name_column_name = ''
    test_name = ''
    ref_test_name = ''
    model_name = ''
    column_name = ''
    severity = 'error'
    if unique_id.split('.')[0] == 'test':
        test_name_model_name_column_name = unique_id.split('.')[2]
        for test in my_test_names:
            if test in test_name_model_name_column_name:
                test_name = test
                if test_name == 'relationships':
                    test_name_model_name_column_name = test_name_model_name_column_name.replace('relationships', '')
                    ref_test_name = test_name_model_name_column_name.lstrip('_').replace('__', ',').split(',')[-1]
                    test_name_model_name_column_name = test_name_model_name_column_name.replace(ref_test_name, '')
            test_name_model_name_column_name = test_name_model_name_column_name.replace(
                test_name, '')

        for model in my_tables:
            if model in test_name_model_name_column_name:
                model_name = model
                if '__warn' in test_name_model_name_column_name:
                    severity = 'warn'
                    test_name_model_name_column_name = test_name_model_name_column_name.replace(
                        '__warn', '')
                else:
                    test_name_model_name_column_name = test_name_model_name_column_name.replace(
                        '__error', '')

                if test_name != 'relationships':
                    column_name = test_name_model_name_column_name.replace(model, '').replace('_amis_', '').lstrip(
                        '_').replace('__', ',').replace('sk', model+'_sk').replace('key', model+'_key')  # has to be in this order
                else:
                    column_name = test_name_model_name_column_name.replace(model, '').lstrip(
                        '_').replace('__', ',')

        # print(model_name, '-->', column_name)
        # print(severity)
        execution_time = new_json['results'][i]['execution_time']
        status = new_json['results'][i]['status']
        failures = new_json['results'][i]['failures']
        message = new_json['results'][i]['message']

        values += f"""
        (
            '{'Relationship Integrity test' if ref_test_name != '' else test_name}',
            '{model_name}',
            '{column_name if column_name != '' else 'NULL'}',
            '{status}',
            '{0 if failures == None else failures}',
            '{'NULL' if message == None else message.replace("'", '`')}',
            '{execution_time}', 
            current_timestamp::timestamp_ntz
        ),"""

def insert_values_into_log():
        conn = sf.connect(user=user, password=password, account=account,
                        warehouse=warehouse, database=database, authenticator=authenticator)

        def run_query(conn, query):
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.close()
        insert_values = f"""insert into DATABASE_NAME.SCHEMA_NAME.TABLE_NAME(
                                test_name, 
                                model_name,
                                column_names, 
                                test_status, 
                                failures, 
                                message, 
                                test_execution_time, 
                                effective_timestamp
                            )
                            values {values[:-1]}"""
        run_query(conn, insert_values)
        print('Success')

    insert_values_into_log()
