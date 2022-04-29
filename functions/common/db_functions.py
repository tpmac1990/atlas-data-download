import sqlalchemy
import psycopg2
import pandas as pd

from functions.logging.logger import logger


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