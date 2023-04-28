import sqlglot
from sqlglot.dataframe.sql.session import SparkSession
import sqlglot.dataframe.sql.functions as f

# First schema
primary_table_schema = dict()

primary_table_schema['id'] = "STRING"
primary_table_schema['second_join_value'] = "STRING"

sqlglot.schema.add_table('primary_table', primary_table_schema)

# Second schema 
second_table_schema = dict()

second_table_schema['id'] = "STRING"
second_table_schema['primary_table_fk'] = "STRING"
second_table_schema['some_value'] = "STRING"

sqlglot.schema.add_table('second_table', second_table_schema)

# Third schema 
third_table_schema = dict()

third_table_schema['id'] = "STRING"
third_table_schema['second_join_value_fk'] = "STRING"
third_table_schema['other_value'] = "STRING"

sqlglot.schema.add_table('third_table', third_table_schema)

# Create dataframes
primary_table = SparkSession().table('primary_table')
second_table = SparkSession().table('second_table')
third_table = SparkSession().table('third_table')

# Perform joins
joined_tables = primary_table.join(
    second_table, 
    on=(
        primary_table['id'] == second_table['primary_table_fk']
    ),
    how='left'
).join(
    third_table,
    on=(
        primary_table['second_join_value'] == third_table['second_join_value_fk'] 
    ),
    how='left'
)

# filter 
filtered_table = joined_tables.filter(*[
    (
        f.coalesce(second_table['some_value'], 'NULL').like('%abc%') & 
        f.coalesce(second_table['some_value'], 'NULL') != 'X' &
        f.coalesce(second_table['some_value'], 'NULL').isNotNull()
    )
])

# select 
selected = filtered_table.select(*[
    f.coalesce(
        primary_table['id'],
        'NULL'
    ).alias('the_primary_id'),
    f.coalesce(
        second_table['some_value'],
        'NULL'
    ).alias('second_table_value'),
    f.coalesce(
        primary_table['id'],
        'NULL'
    ).alias('the_primary_id'),
])

# sql
print('selected sql:')
print(selected.sql(dialect='snowflake', optimize=False))
print('\n')

try:
    print(selected.sql(dialect='snowflake'))
except Exception as e:
    print(e)

print('\n\n')

# Selects without coalesce
no_coalesce_selected = filtered_table.select(*[
    primary_table['id'].alias('the_primary_id'),
    second_table['some_value'].alias('second_table_value'),
    primary_table['id'].alias('the_primary_id'),
])

print('uncoalesced sql:')
print(no_coalesce_selected.sql(dialect='snowflake', optimize=False))
print('\n')

try:
    print(no_coalesce_selected.sql(dialect='snowflake'))
except Exception as e:
    print(e)

print('\n\n')

# Try selecting all columns 
select_refereneced_columns = filtered_table.select(*[
    primary_table['id'],
    primary_table['second_join_value'],
    second_table['primary_table_fk'],
    second_table['some_value'],
    third_table['second_join_value_fk'],
    third_table['other_value']
])

print('all columns sql:')
print(select_refereneced_columns.sql(dialect='snowflake', optimize=False))
print('\n')

try:
    print(select_refereneced_columns.sql(dialect='snowflake'))
except Exception as e:
    print(e)
