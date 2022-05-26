import sqlalchemy
import psycopg2
from psycopg2 import Error
import pandas as pd

from functions.logging.logger import logger
from functions.common.database_commands import close_db_connections


def sqlalchemy_engine(db_configs):
    return sqlalchemy.create_engine('postgresql://%s:%s@%s/%s' %(db_configs['user'], db_configs['password'], db_configs['host'], db_configs['dbname']))

def connect_psycopg2(db_keys):
    return psycopg2.connect("dbname='%s' user='%s' password='%s' host='%s'" %(db_keys['dbname'],db_keys['user'],db_keys['password'],db_keys['host']))

def orderTables(configs,input_lst,carry_lst,temp_lst):
    for table in input_lst:
        temp_lst = []
        if not table in carry_lst:

            sub_tables = configs[table]['related_tables']
            
            if not len(sub_tables) == 0:
                carry_lst, temp_lst = orderTables(configs,sub_tables,carry_lst,temp_lst)
            temp_lst.insert(0,table)

        carry_lst = carry_lst + [x for x in temp_lst[::-1] if not x in carry_lst ]

    return carry_lst, temp_lst


def clearDatabaseTable(conn, table_name):
    cur = conn.cursor()
    cur.execute("DELETE FROM %s"%(table_name))
    rows_deleted = cur.rowcount
    conn.commit()
    logger(message=f"'{rows_deleted}' rows cleared from '{table_name}'", category=4)


def convert_date_fields_to_datetime(df):
    for col in df.columns:
        if col in ['date_modified','date_created','date']:
            df[col] = pd.to_datetime(df[col]).copy()
    return df


def drop_all_db_tables(configs, _exclude=[]):
    """ drop all db tables in the database except for those in exclude """
    try:
        db_conn = connect_psycopg2(configs)
    except:
        # databse doesn't exist, so no need to delete
        return
    try:
        db_conn.autocommit = True
        db_cursor = db_conn.cursor()
        # returns a query for the tables to drop
        s = "SELECT CONCAT('DROP TABLE ', TABLE_NAME , ';') FROM information_schema.tables WHERE table_schema = 'public' AND TABLE_NAME NOT IN ('geography_columns', 'geometry_columns', 'spatial_ref_sys');"
        db_cursor.execute(s)  
        list_tables = db_cursor.fetchall()   
        for table_action in list_tables:
            db_cursor.execute(table_action[0])
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (db_conn):
            db_cursor.close()
            db_conn.close()          
    
                                                                                      