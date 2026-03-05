from data_utils import create_schema, create_tables


create_schema('raw')
create_schema('staging')
create_schema('dim')
create_schema('fact')

create_tables('raw')
create_tables('staging')
create_tables('dim')
create_tables('fact')