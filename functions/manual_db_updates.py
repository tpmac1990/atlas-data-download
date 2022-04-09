import os
import pandas as pd
import json
from .directory_files import get_json
from .setup import SetUp, Logger
from pprint import pprint
from .db_functions import connect_psycopg2



class ManualUpdates:
    """ makes the changes to the database that multiple users would make. Required for testing """
    # db_update_configs = get_json(os.path.join(SetUp.configs_dir,'db_update_configs.json'))
    edit_df = pd.read_csv('C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/test_data/01_initial/01_all_data/08_user_edits/user_edits.csv')
    db_update_configs = get_json(os.path.join(SetUp.configs_dir,'db_update_configs.json'))
    access_configs = get_json(os.path.join(SetUp.configs_dir,'db_access_configs.json'))
    
    
    def get_updating_tables(self):
        conn = connect_psycopg2(self.access_configs[SetUp.active_atlas_directory_name])
        df = self.edit_df
        table_lst = self.get_unique_values(df,'TABLE')
        
        configs = self.db_update_configs
        # key = db name, value = model/csv name
        model_db_name_dic = { configs[x]['db_name']:x for x in configs}
        
        df_lst = df.to_dict('records')
        for row in df_lst:
            action = row['ACTION']
            table = row['TABLE']
            id_column = row['ID_COLUMN']
            id_value = row['ID']
            value_column = row['VALUE_COLUMN']
            value = row['VALUE']
            model_name = model_db_name_dic[table]
            get_next_args = {}
            
            if id_value == "get_next":
                get_next_args = {"get_next_pk_table":model_name, "pk_field":id_column}
            
            if value_column == "multi_column":
                keys, values = self.json_to_keys_and_values(value, **get_next_args)
            else:
                keys, values = (id_column,value_column,), (id_value,value,)
                
            if id_column == "multi_lookup":
                vals = json.loads(id_value)
                keys = list(vals.keys())
                sql_where = f"{keys[0]}={vals[keys[0]]} AND {keys[1]}={vals[keys[1]]}"
            else:
                sql_where = f"{id_column}={id_value}"
                
            if action == 'DELETE':
                command = f"DELETE FROM {table} WHERE {id_column}={id_value} AND {value_column}={value}"
            
            elif action == 'INSERT':
                command = f"INSERT INTO {table} {keys} VALUES {values}"
                
            elif action == 'UPDATE':
                command = f"UPDATE {table} SET {value_column}={value} WHERE {sql_where}"
                
            cur = conn.cursor()
            cur.execute(command)
            # rows_deleted = cur.rowcount
            conn.commit()


    
    @staticmethod
    def get_unique_values(df,field):
        return df[field].drop_duplicates().tolist()
    
    
    @staticmethod
    def json_to_keys_and_values(value, **kwargs):
        result = json.loads(value)
        for key in result.keys():
            if 'date_' in key and '/' in result[key]:
                s = result[key].split('/')
                result[key] = f"{s[2]}-{s[1]}-{s[0]}"
        if 'get_next_pk_table' in kwargs:
            table = kwargs.pop('get_next_pk_table')
            field = kwargs.pop('pk_field')
            occ_df = pd.read_csv(os.path.join(SetUp.output_dir,'core','%s.csv'%(table)))
            next_ind = occ_df[field].max() + 1
            result[field] = next_ind
        return tuple(result.keys()), tuple(result.values())
    
