import pandas as pd
import numpy as np
import os
import sys
import re
from datetime import datetime, date, timedelta
# import time
import ctypes
import csv
import psycopg2
import sqlalchemy

from functions.common.timer import Timer
from functions.common.directory_files import get_json
from functions.common.backup import complete_script__restore
from .db_update import clear_db_table_rows_in_lst, sqlalchemy_engine, connect_psycopg2, update_db_table_by_index_field_and_value_lst
from ..setup import SetUp
from functions.logging.logger import logger



class UpdateMissingData:

    def __init__(self):
        # # if task is not 'manual_updates' then skip this step. It has not beed requested

        self.core_dir = os.path.join(SetUp.output_dir,'core')
        self.update_dir = os.path.join(SetUp.output_dir,'update')

        access_configs = get_json(os.path.join(SetUp.configs_dir,'db_access_configs.json'))
        self.configs = get_json(os.path.join(SetUp.configs_dir,'commit_updates.json'))

        # update the database tables to match the csv files
        db_keys = access_configs[SetUp.active_atlas_directory_name]
        self.con = sqlalchemy_engine(db_keys).connect()
        self.conn = connect_psycopg2(db_keys)

        self.manual_update_path = os.path.join(self.update_dir,'manual_update_required.csv')
        self.missing_all_path = os.path.join(self.update_dir,'missing_all.csv')
        manual_update_df = pd.read_csv(self.manual_update_path)
        manual_update_df = manual_update_df[['STATE','GROUP','FIELD','ORIGINAL','LIKELY_MATCH']]
        missing_all_df = pd.read_csv(self.missing_all_path)

        # save the remaining rows from the 'manual_update_required' that are yet to have the correct value assigned
        self.re_reduced_df = manual_update_df[manual_update_df['LIKELY_MATCH'].isnull()]
        self.re_full_df = missing_all_df.merge(self.re_reduced_df,left_on=['STATE','GROUP','FIELD','VALUE'],right_on=['STATE','GROUP','FIELD','ORIGINAL'],how='inner').drop(columns=['ORIGINAL','LIKELY_MATCH'])

        # filter for rows that have had a matching updating value added to LIKELY_MATCH
        self.manual_update_df = manual_update_df[~manual_update_df['LIKELY_MATCH'].isnull()]
        self.missing_all_df = missing_all_df.merge(self.manual_update_df,left_on=['STATE','GROUP','FIELD','VALUE'],right_on=['STATE','GROUP','FIELD','ORIGINAL'],how='inner').drop(columns=['ORIGINAL','LIKELY_MATCH']).drop_duplicates()
        



    def apply_missing_data_updates(self):
        ''' backup necessary files to the backup folder
            Once the data has been reviewed and all the fields have been filled out in the 'manual_update_required' file found in the 
            update folder, this will add these new values to the core files and database files.
            Files that have no new value applied to the LIKELY_MATCH field will remain in these update files through updates until they have a value
            added to LIKELY_MATCH
        '''
        # if task is not 'manual_updates' then skip this step. It has not beed requested
        if SetUp.task != 'manual_updates':
            return

        timer = Timer()
        logger(message="Apply Missing Data Updates", category=1)

        try:
            configs = self.configs

            # commit changes to the database
            for x in configs:
                logger(message=f"Working on field '{x}'", category=4)
                self.commit_fields_updated_data(x,configs[x])

            # overwrite the update files with the remaining rows that had no new value to apply, the user will then be able to update the value at a later date
            self.re_reduced_df.to_csv(self.manual_update_path,index=False)
            self.re_full_df.to_csv(self.missing_all_path,index=False)

            self.con.close()
            self.conn.close()
            complete_script__restore()

        except:
            complete_script__restore()
            self.con.close()
            self.conn.close()
            raise

        logger(message='Committed missing values to database and files: %s' %(timer.time_past()), category=4)
        print('Manual data updates complete')
        sys.exit(1)



    def commit_fields_updated_data(self,field,configs):
        '''
            raw_file: file that contains the raw values and the formatted match
                dataset: the directory it lives in inside the convert folder
                name: name of the raw file that has prefix '_R'
            raw_update: configs to update the raw file
                style: either 'simple'
                    simple: gets the original value and manually formatted file and adds it to the raw file
                            all fields use this except for NAME which requires no raw_file update as the values are only formatted
                fields: the keys are used to slice the relevant fields of and then the dic is used to rename the fields to concat to the original raw file
            id_file: the file that contains the id of the values and not the ind value
                name: name of the file
                left_on: the key to merge the raw file on 
                right_on: the key to merge the id_file on
                index: the index field of the id_file
            miss_merge: configs to merge the complete missing file with the reduced missing file
                left_on: key to match the complete missing file
                right_on: key to match the reduced missing file
            ind_file: name of the ind file
            df_build: dic with the fields and values to build the extra columns to concat to the existing ind file. 'date' will be converted to todays date
            remove_temp: boolean if the temp values need to be removed before the formatted values are added. currenlty only used for holder where the unformatted values are used until they are manually updated
            db_update: configs for updating the relevant csv's and db tables
                type: either 'holder','append','insert'
                    holder: only used for holder. drops the temp unformatted values from the ind & id df's, then adds the formatted values to the ind & id df's and db tables
                    append: designed to only append new rows to the ind_df and db table
                    insert: replaces specific values in a table. this is used when the rows can not be deleted because related table rely on them.
                columns: column names of the ind_df
                index: the index value to look up. in tenement this would be ind
                index_dtype: either null or 'string'. determine if the index field need to be formatted into a string before merging so the two tables index fields are of the same type
                process: clear, append, update
                    clear: only used for holder. clears updating rows in both the ind & id tables
                    append: appends new rows to the ind table. tables may have previously had some rows deleted such as for the holder table
                    update: updates values of a given field for rows of given indexes
        '''
        csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

        con = self.con
        conn = self.conn
        manual_update_df = self.manual_update_df
        missing_all_df = self.missing_all_df

        # get the reduced missing values for this field
        reduced_miss_df = manual_update_df[manual_update_df['FIELD'] == field]

        # nothing to do if there are no missing values
        if reduced_miss_df.empty:
            return

        config = configs['update_core']
        raw_path = get_path(SetUp.convert_dir,config['raw_file'])
        id_path = get_path(self.core_dir,config['id_file']['name'])
        ind_path = get_path(self.core_dir,config['ind_file'])

        raw_df = open_file_as_df(raw_path)
        id_df = open_file_as_df(id_path)
        ind_df = open_file_as_df(ind_path)

        # filter the missing df that includes the 'ind' values for the target field
        miss_df = missing_all_df.query('FIELD == "%s"'%(field))
        # merge the recorded missing value with formatted value manually given to it
        miss_left_on = config['miss_merge']['left_on']
        miss_right_on = config['miss_merge']['right_on']
        old_to_new_merge_df = miss_df.merge(reduced_miss_df,left_on=miss_left_on,right_on=miss_right_on,how='left')

        # all the missing values matched with the id_df to see which already exist and can retrieve an existing _id and which need to be added to the id_df
        right_on = config['id_file']['right_on']
        left_on = config['id_file']['left_on']
        index = config['id_file']['index']
        remove_temp = config['remove_temp']

        # df of the reduced_miss_df rows that exist in the id_df
        existing_ids_df = reduced_miss_df.merge(id_df,left_on=left_on,right_on=right_on,how='left')
        # filter for only the rows that don't exist in the id_df. These will be added to the id_df and assigned an _id which will be used in the ind_df later. 
        to_add_df = existing_ids_df[existing_ids_df[index].isnull()][['LIKELY_MATCH']].drop_duplicates().rename(columns={'LIKELY_MATCH':right_on})

        # single column of values to remove from the id_df. required for holder where the temp unformatted names are removed and the new fromatted names are added
        if remove_temp:
            # only the 'LIKELY MATCH' values that don't exist in the id_df need to be removed. A previous error was the result of removing all 'ORIGINAL' names, even the names that didn't change and this
            #   resulted in blanks in the tenholder as it wasn't able to find its _id later on
            to_remove_df = existing_ids_df[existing_ids_df['_id'].isnull()][['ORIGINAL']].rename(columns={'ORIGINAL':right_on})

        else:
            to_remove_df = pd.DataFrame([],columns=[right_on])


        id_df, to_add_df, remove_id_df = add_missing_rows_to_id_df(id_df,to_add_df,to_remove_df,config['df_build'])

        # match the missing values with their _id value from the id_df
        ind_merge_df = old_to_new_merge_df.merge(id_df,left_on=left_on,right_on=right_on,how='left')[['IND',index]]
        columns = config['db_update']['columns']
        table_index = config['db_update']['index']
        db_add_style = config['db_update']['type']
        update_process = config['db_update']['proccess']
        index_lst = []

        ind_merge_df.columns = columns

        # update the raw_df file if necessary
        raw_update = config['raw_update']
        style = raw_update['style'] if raw_update else ''

        if style == 'simple':
            # copies the missing values with the formatted match to the raw file so they are available on next update
            mfields = raw_update['fields']
            sfields = [x for x in mfields]
            raw_add_df = existing_ids_df[sfields].rename(columns=mfields)
            raw_df = pd.concat((raw_df,raw_add_df)).drop_duplicates()
        

        # find the changes to be made to the ind & id db files and update their respective df's
        if db_add_style == 'insert': 
            # this is for fields in one of the dataset df's. filter the df for the ind and other required fields and replace the neccessary field with the updated values and then update the original df
            snippet_ind_df = ind_df[ind_df[table_index].isin(ind_merge_df[table_index])].drop(columns[1],1)
            update_df = ind_merge_df
            ind_merge_df = snippet_ind_df.merge(ind_merge_df,on=table_index).set_index(table_index,drop=False)
            ind_df.set_index(table_index,drop=False,inplace=True)
            ind_df.update(ind_merge_df)
            # create dic to update values in the db. I can't delete all the rows as they have relations in other tables
            update_dic = {'index': columns[0], 'field':columns[1], 'lst':update_df.values.tolist()}

        elif db_add_style == 'append':
            # perform an outer merge to remove any rows in ind_merge_df that already exist in the ind_df. There shouldn't be any duplicates, but if they exist and aren't remove it will throw an error when appending to the db
            ind_merge_df = ind_merge_df.merge(ind_df,how='outer',indicator=True).query('_merge == "left_only"').drop('_merge',1)
            # merge additions to the final ind_df
            ind_df = pd.concat((ind_df,ind_merge_df))


        elif db_add_style == 'holder':
            # print(id_df[id_df['name'] == 'Burtorn Silver Pty Ltd'])
            # get the unformatted and formatted values
            old_new_name_df = reduced_miss_df[['ORIGINAL','LIKELY_MATCH']]
            # match the old names with their id's. these have been deleted from the id_df so they are retrieved from the 'remove_id_df'
            old_ids_df = old_new_name_df.merge(remove_id_df,left_on='ORIGINAL',right_on='name',how='left')
            # match the new names with their ids
            name_to_id_df = reduced_miss_df.merge(id_df,left_on='LIKELY_MATCH',right_on='name',how='left')
            # join the df's so the the old id's are matched with the new ids
            new_ids_df = old_ids_df.merge(name_to_id_df,left_on='LIKELY_MATCH',right_on='name',how='left')[['_id_x','_id_y']]
            # convert to dictionary for the values to be replaced
            old_to_new_ids_dic = {x[0]:x[1] for x in new_ids_df.values.tolist()}
            # get the id_ind rows that need to be edited
            ind_merge_df = ind_df[ind_df['name_id'].isin(new_ids_df['_id_x'])].copy()
            # convert the ncessary ind rows
            ind_merge_df.replace({"name_id": old_to_new_ids_dic},inplace=True)
            # make a list out of the _id values to use to delete these rows in the db
            index_lst = ind_merge_df['_id'].tolist()
            # build the final updated ind_df
            remain_ind_df = ind_df[~ind_df['name_id'].isin(new_ids_df['_id_x'])]
            ind_df = pd.concat((remain_ind_df,ind_merge_df))



        # save the files and commit to the db
        # save raw_df
        if raw_update:
            raw_df.to_csv(raw_path,index=False)

        if "clear" in update_process:
            # remove ind_df rows
            clear_db_rows(conn, config['ind_file'], '_id', index_lst)
            # remove and add new id_df rows
            clear_db_rows(conn, config['id_file']['name'], '_id', remove_id_df['_id'].to_list())

        # append new id_df rows to the db and save to csv. This is for files like OccName where new names need to be added to give them an id
        if not to_add_df.empty:
            append_to_db(con,config['id_file']['name'],to_add_df)
            id_df.to_csv(id_path,index=False)

        # if data exists in ind_merge_df
        if "append" in update_process and not ind_merge_df.empty:
            append_to_db(con,config['ind_file'],ind_merge_df)
            ind_df.to_csv(ind_path,index=False)

        # this will update rows rather than delete and add new rows. This is used for the dataset tables, occurrence and tenement, where the ind value can not be deleted without causing an error
        if "update" in update_process and not ind_merge_df.empty:
            table_name = 'gp_%s'%(config['ind_file'])
            update_db_table_by_index_field_and_value_lst(conn, table_name, update_dic)
            ind_df.to_csv(ind_path,index=False)




def get_path(directory,file_name):
    return os.path.join(directory,'%s.csv'%(file_name)) if file_name else None

def open_file_as_df(path):
    return pd.read_csv(path,engine='python') if path else None

def insert_date_in_date_fields(dic):
    ''' replace dic value 'date' with todays date. This is required as the date can not be stored in json config file '''
    for x in dic:
        dic[x] = SetUp.pyDate if dic[x] == 'date' else dic[x]
    return dic


def add_missing_rows_to_id_df(id_df,to_add_df,to_remove_df,configs):
    ''' if it is permitted then add the new values to the id_df so the next time the data is updated this value will have an _id.
        return the to_add_df as well as this will be inserted into the db table
    ''' 
    # print(to_add_df[to_add_df['name'] == 'Burtorn Silver Pty Ltd'])
    if configs and not to_add_df.empty:
        # filter out the remove values and return them in their own df. if 'to_remove_df' is empty then it will return an empty df and id_df will be unchanged. only used for holder where the temp names need to be removed
        col_name = to_remove_df.columns[0]
        remove_id_df = id_df[id_df[col_name].isin(to_remove_df[col_name])]
        id_df = id_df[~id_df[col_name].isin(to_remove_df[col_name])]
        next_id = id_df['_id'].max() + 1
        to_add_df['_id'] = np.arange(next_id, len(to_add_df) + next_id)
        dic = insert_date_in_date_fields(configs)
        for i in dic:
            to_add_df[i] = dic[i]
        id_df = pd.concat((id_df,to_add_df))
    else:
        # return an empty df to prevent error
        remove_id_df = pd.DataFrame()
    return id_df, to_add_df, remove_id_df


def append_to_db(con,file_name,df):
    ''' appends a df to the given db table '''
    table_name = 'gp_%s'%(file_name.lower())
    try:
        df.to_sql(table_name,con,if_exists='append',index=False, method='multi')
    except:
        try:
            logger(message=f'error when appending to table {table_name}', level=ERROR, category=4)
            raise

        except Exception as e: 
            # print representation of error only to exclude the massive print out of the failed sql query
            print(repr(e))
            # con.close()
            sys.exit(1)

        # except psycopg2.errors.NotNullViolation as e:
        # except psycopg2.errors.UniqueViolation as e:
        # except sqlalchemy.exc.IntegrityError as e:

        
    logger(message="'%s' rows appended to '%s'"%(len(df.index),table_name), category=4)


def clear_db_rows(conn,file_name,table_index,index_lst):
    table_name = 'gp_%s'%(file_name.lower())
    clear_db_table_rows_in_lst(conn, table_name, table_index, index_lst)


