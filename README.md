# Atlas Data management application 

Responsible for downloading, updating and maintaining data between the numerous state and federal data sources, and the user edits made within the Atlas application.




## setup 

### project location
c/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets

### activate environment
source env/scripts/activate

### run download
cd scripts
python update_data.py

## Testing

### end to end testing
cd scripts
pytest -s tests/end_to_end_test.py
This will prompt you on which section you want to test for.


### create database dump
pg_dump --port=5432 --username=postgres --host=localhost --dbname=atlas > atlas_dump.sql

