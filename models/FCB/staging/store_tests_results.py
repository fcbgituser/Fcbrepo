from json import loads
import snowflake.connector as sf
from yaml.loader import SafeLoader

with open(rf'{target_path}/run_results.json', 'r') as input_file:
    my_json = input_file.read()

new_json = loads(my_json)

my_tables = []
with open(rf'{dbt_path}/models/source/source.yml') as f:
    my_source = load(f, Loader=SafeLoader)
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
