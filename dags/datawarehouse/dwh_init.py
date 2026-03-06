from data_utils import create_schema, create_tables
from data_modification import create_artist
import json

create_schema('raw')
create_schema('staging')
create_schema('dim')
create_schema('fact')

create_tables('raw')
create_tables('staging')
create_tables('dim')
create_tables('fact')

with open('./data/artists.json', 'r') as file:
    data = json.load(file)

for artist in data:
    create_artist(artist['artist_name'])