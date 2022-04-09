import os
import pandas as pd
import json
from .directory_files import get_json, file_exist
from ..setup import SetUp, Logger
from .db_functions import connect_psycopg2, sqlalchemy_engine
from functions.segment.commit_new_values import append_to_db



def load_user_edits(**kwargs):
    """ 
    makes the changes to the database that multiple users would make. Required for testing 
    3 steps:
        1. makes changes to table
        2. set user_edit to true
        3. append 'change' & 'addition' tables to db. These are currently manually built
    """
    segment_dir = kwargs.get("segment_dir")
    user_edits = os.path.join(segment_dir,"USER_EDITS.csv")
    df = pd.read_csv(user_edits)
    db_update_configs = get_json(os.path.join(SetUp.configs_dir,'db_update_configs.json'))
    access_configs = get_json(os.path.join(SetUp.configs_dir,'db_access_configs.json'))
    
    conn = connect_psycopg2(access_configs[SetUp.active_atlas_directory_name])
    con = sqlalchemy_engine(access_configs[SetUp.active_atlas_directory_name])
    table_lst = _get_unique_values(df,'TABLE')
    
    configs = db_update_configs
    # key = db name, value = model/csv name
    model_db_name_dic = { configs[x]['db_name']:x for x in configs}
    
    record_changes = []
    
    df_lst = df.to_dict('records')
    for row in df_lst:
        action = row['ACTION']
        table = row['TABLE']
        id_column = row['ID_COLUMN']
        id_value = row['ID']
        value_column = row['VALUE_COLUMN']
        value = row['VALUE']
        model_name = model_db_name_dic[table]
        useredit_model = configs[model_name]["record_changes"]["data_group"]
        
        get_next_args = {}
        
        if id_value == "get_next":
            get_next_args = {"get_next_pk_table":model_name, "pk_field":id_column}
            
        if value_column == "multi_column":
            keys, values = _json_to_keys_and_values(value, **get_next_args)
        else:
            keys, values = f"{id_column},{value_column}", (id_value,value,)
            
        record_id = _get_table_and_pk_for_user_edit(useredit_model,keys,values)
        change_obj = {"dataset": useredit_model, "id": record_id}
        if not change_obj in record_changes:
            record_changes.append(change_obj)
            
        if id_column == "multi_lookup":
            vals = json.loads(id_value)
            keys = list(vals.keys())
            sql_where = f"{keys[0]}={vals[keys[0]]} AND {keys[1]}={vals[keys[1]]}"
        else:
            sql_where = f"{id_column}={id_value}"
            
        if action == 'DELETE':
            command = f"DELETE FROM {table} WHERE {id_column}={id_value} AND {value_column}={value}"
        
        elif action == 'INSERT':
            command = f"INSERT INTO {table} ({keys}) VALUES {values}"
            
        elif action == 'UPDATE':
            command = f"UPDATE {table} SET {value_column}='{value}' WHERE {sql_where}"
            
        cur = conn.cursor()
        cur.execute(command)
        # rows_deleted = cur.rowcount
        conn.commit()
    
    # change 'user_edit' to true for fields that have had a change
    for row in record_changes:
        table = "gp_" + row["dataset"]
        ind = row["id"]
        command = f"UPDATE {table} SET user_edit=true WHERE ind={ind}"
        
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()

    # append the 'change' files to the database tables
    for change_file in ["HolderChange","OccurrenceAddition","OccurrenceChange","TenementAddition","TenementChange","TenementRemoval"]:
        path = os.path.join(segment_dir,"{}.csv".format(change_file))
        if file_exist(path):
            change_df = pd.read_csv(path,dtype=str)
            table = "gp_%s"%(change_file.lower())
            append_to_db(con,change_file,change_df)
        
def _get_unique_values(df,field):
    return df[field].drop_duplicates().tolist()

def _json_to_keys_and_values(value, **kwargs):
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
    # return column names as a string
    return (',').join([x for x in result.keys()]), tuple(result.values())

def _get_table_and_pk_for_user_edit(useredit_model,keys,values):
    fields = keys.split(",")
    if "multi_lookup" in fields:
        index = fields.index("multi_lookup")
        multi_grp = json.loads(values[index])
        field = _get_core_field(multi_grp.keys())
        record_id = multi_grp[field]
    else:
        field = _get_core_field(fields)
        index = fields.index(field)
        record_id = values[index]
        
    return int(record_id)
        
def _get_core_field(obj):
    for field in ["ind","tenement_id","occurrence_id"]:
        if field in obj:
            return field
    

        
        
    