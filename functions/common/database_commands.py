import subprocess
import os

from ..setup import SetUp, Logger


""" 
functions to deal with database dumping and restoring 
set capture_output=False to print the output of the commands
"""
    
def pg_dump(filename,
            directory=SetUp.db_dumps_dir,
            dbname='atlas', 
            host='localhost', 
            username='postgres', 
            port='5432'
            ):
    """ dumps will be saved by default to the db_dumps directory in the root directory """
    Logger.logger.info(f"Creating .sql dump file for '{dbname}'")
    path=os.path.join(directory,filename)
    command = f"pg_dump --port={port} --username={username} --host={host} --dbname={dbname} > {path}"
    subprocess.run(command, shell=True)
    
    
def drop_db(dbname='atlas', 
            username='postgres'
            ):
    Logger.logger.info(f"Dropping database '{dbname}'")
    close_db_connections(dbname=dbname,username=username)
    command = f'psql --username={username} --command="DROP DATABASE IF EXISTS {dbname}"'
    subprocess.run(command, shell=True, capture_output=True)
    

def create_db(dbname='atlas', 
            username='postgres',
            port='5432',
            host='localhost'
            ):
    Logger.logger.info(f"Creating database '{dbname}'")
    command = f"createdb --host={host} --port={port} --username={username} {dbname}"
    subprocess.run(command, shell=True, capture_output=True)


def restore_db_sql(
            filename,
            directory=SetUp.db_dumps_dir,
            dbname='atlas', 
            host='localhost', 
            username='postgres', 
            port='5432'
            ):
    Logger.logger.info(f"Restoring .sql dump to database '{dbname}'")
    path=os.path.join(directory,filename).replace('/','\\')
    command = f'psql --dbname={dbname} --port={port} --username={username} --host={host} < {path}'
    subprocess.run(command, shell=True, capture_output=True)
        
    
def close_db_connections(
            dbname='atlas', 
            username='postgres', 
            ):
    Logger.logger.info(f"Closing all database connections for database '{dbname}'")
    sql = f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname='{dbname}' AND pid <> pg_backend_pid();"
    command = f'psql --username={username} --command="{sql}"'
    subprocess.run(command, shell=True, capture_output=True)
    

def drop_and_restore_db(
        filename,
        directory=SetUp.db_dumps_dir,
        dbname='atlas', 
        host='localhost', 
        username='postgres', 
        port='5432'
        ):
    # drop the db if it exists
    drop_db(dbname=dbname, 
            username=username
            )
    # create new db
    create_db(dbname=dbname, 
            username=username,
            port=port,
            host=host
            )
    # restore the database with the .sql dump file
    restore_db_sql(
            filename=filename, 
            directory=directory, 
            dbname=dbname,
            host=host, 
            username=username, 
            port=port, 
            )
        




# print(sys.argv) # use this for unittesting
# if 'pytest' in sys.modules:
#         print('testing')



# import argparse

# parser = argparse.ArgumentParser(description="testing")
# parser.add_argument('-k','--keepdb',action="store_true",help="Use database from last test")
# args = parser.parse_args()

# def testy(keepdb):
#     print(keepdb)

# if __name__ == '__main__':
#     testy(args.keepdb)







# p1 = subprocess.run(['ls', '-la'], capture_output=True, text=True)

# p1 = subprocess.run('pwd', capture_output=True, text=True, shell=True)

# subprocess.run('pg_dump --port=5432 --username=postgres --host=localhost --schema-only atlas > C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/functions/backup_file.sql', shell=True)

# print(p1.stdout)

# # create sql pg_dump
# sql_file_path = 'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/functions/atlas_backup_2.sql'
# subprocess.run(f'pg_dump --port=5432 --username=postgres --host=localhost --dbname=atlas > {sql_file_path}', shell=True)
# print('dump complete')

# # drop database
# subprocess.run('psql --username=postgres --command="DROP DATABASE atlas"', shell=True)
# print('drop complete')

# # re-create database
# subprocess.run('createdb --host=localhost --port=5432 --username=postgres atlas', shell=True)
# print('created new database')

# # restore database with dump file
# subprocess.run('psql --dbname=atlas --port=5432 --username=postgres --host=localhost < atlas_backup.sql', shell=True)
# print('database restored')

# # close database connections
# command = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname='atlas' AND pid <> pg_backend_pid();"
# process = "psql --username=postgres --command=%s"%(command)
# subprocess.run(process, shell=True)




# psql --dbname=atlas --port=5432 --username=postgres --host=localhost < C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/test_data/01_initial/05_find_changes_and_update_database/atlas_dump.sql

# psql --dbname=atlas --port=5432 --username=postgres --host=localhost < C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/test_data/01_initial/05_find_changes_and_update_database/atlas_dump.sql