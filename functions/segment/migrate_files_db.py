import pandas as pd
import os
import sys
import sqlalchemy
import psycopg2

from functions.common.directory_files import get_json
from functions.common.db_functions import sqlalchemy_engine, connect_psycopg2, orderTables, clearDatabaseTable, convert_date_fields_to_datetime

from ..setup import SetUp
from functions.logging.logger import logger



class DatabaseManagement:

    def __init__(self):
        self.access_configs = get_json(os.path.join(SetUp.configs_dir,'db_access_configs.json'))
        self.update_configs = get_json(os.path.join(SetUp.configs_dir,'db_update_configs.json'))

        # swap change_dir for core_dir


    # def commit_all_files_to_db(self,tables=None):
    def clear_db_tables_and_remigrate(self, src_dir):
        ''' clear all tables in the database and re-migrate them from the requested directory '''

        db_keys = self.access_configs[SetUp.active_atlas_directory_name]
        con = sqlalchemy_engine(db_keys).connect()
        conn = connect_psycopg2(db_keys)
        # print(engine.table_names()) # print all tables in the database

        configs = self.update_configs
        orig_lst = [ table for table in configs ] # get a list of all the database tables

        ordered_tables, temp_lst = orderTables(configs,orig_lst,[],[]) #orders the tables so there are no conflicts when entering into the database

        logger(message="Clearing rows from database", category=2)
        # delete all data in all tables in order
        for table in ordered_tables[::-1]: 
            table_name = "gp_%s"%(table.lower())
            try:
                clearDatabaseTable(conn,table_name)
            except OperationalError:
                logger(message=f"Server close error on table '{table}'", level=ERROR, category=4)
            except Exception as e:
                logger(message=f"Error deleting rows in '{table}' in the database\n{repr(e)}", level=ERROR, category=4)
                print(repr(e))
                con.close()
                conn.close()
                sys.exit(1)


        # drop all table names that are to do with updating. These Won't be added on the initial table creation.
        update_tables_lst = [x for x in configs if configs[x]["update_table"]]
        ordered_tables = [x for x in ordered_tables if not x in update_tables_lst]

        logger(message="Migrating tables to database", category=2)
        for table in ordered_tables:        
            logger(message=f"Migrating: {table}", category=4)
            path = os.path.join(src_dir,"%s.csv"%(table))
            table_name = "gp_%s"%(table.lower())

            df = pd.read_csv(path,engine='python')

            df = convert_date_fields_to_datetime(df)

            try:
                df.to_sql(table_name,con,if_exists='append',index=False, method='multi')
            except Exception as e:
                # print(e.args)
                # print the error without all the sql
                logger(message=f"Error migrating rows in '{table}' to the database\n{repr(e)}", level=ERROR, category=4)
                print(repr(e))
                con.close()
                conn.close()
                sys.exit(1)

        con.close()
        conn.close()

