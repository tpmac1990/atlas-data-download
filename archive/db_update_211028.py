from .directory_files import copy_directory, getJSON, fileExist, copy_directory_in_list
from .preformat import singleColumnDfToList
from .timer import time_past
# from .my_geopandas import df_to_geo_df_wkt, geoDfToDf_wkt
from .data_download import geoDfToDf_wkt

import geopandas as gpd
from shapely import wkt
import sqlalchemy
import pandas as pd
import numpy as np
import os
import psycopg2
from datetime import datetime, date, timedelta
import time
import ctypes
import csv
import sys
# from math import ceil



def find_changes_update_core_and_database(self):
    ''' If the Tenement.csv file doesn't exist in the core folder then isUpdate is False. This will clear all the rows in each table in the database and 
        reload it with a new batch of data, generally used to insert the initial data.
        if isUpdate is True then only the changes need to be added to the database.
    '''
    func_start = time.time()
    # directories
    self.core_dir = os.path.join(self.output_dir,'core')
    self.ss_dir = os.path.join(self.output_dir,'ss')
    self.new_dir = os.path.join(self.output_dir,'new')
    self.onew_dir = os.path.join(self.output_dir,'onew')
    self.update_dir = os.path.join(self.output_dir,'update')
    self.change_dir = os.path.join(self.output_dir,'change')
    # paths
    self.updates_path = os.path.join(self.update_dir,"update.csv")
    self.changes_path = os.path.join(self.update_dir,"change.csv")
    self.core_updates_path = os.path.join(self.core_dir,'update.csv')
    self.core_changes_path = os.path.join(self.core_dir,'change.csv')
    # configs
    self.update_configs = getJSON(os.path.join(self.configs_dir,'db_update_configs.json'))
    self.access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))

    # get congif files
    # If less than three files, then all new files will be pushed to db
    if not self.isUpdate:
        ''' Copy the files from the new folder to the core & change folder. Then copy the new files to the core folder
            and add the user_input, valid & modified columns. Copy these files to the us_change folder which will then be loaded into the database.
            lastly, create an empty change.csv file in the change folder.
        '''
        print('No update.csv file in the change directory. Creating core & change files from new directory.')
        # copy relevant files from the new folder to the core and ss folders
        copy_new_files_to_core(self)
        # create qgis compatible files for tenement & occurrence files
        create_qgis_spatial_files(self)
        # delete all content in tables and copy all files to the db
        commit_all_files_to_db(self) 
        # create empty change file. This will tell the script to update rather than renew everything the next time it is run.
        create_empty_change_file(self)
    else:
        '''
        '''
        print('This is an update only. Creating CHANGE and UPDATE files.')
        # add core data to new csv file for Tenement & Occurrence values that were added in the tenement_occurrence relation step
        add_relation_core_rows_to_new_file(self) # need to fix
        # update the db for core files that are updated manually and are not compared to 
        update_db_for_manually_handled_core(self)
        # copy new files that will be updated with user edits to a separate folder. This will only be used to compare the new file updated with the user edits and the original
        backup_new_useredit_file(self)
        # Compare all the files that don't have changes recorded. Add new rows to db.
        compare_base_tables_add_new(self) 
        # makes the changes to the ss files, then update the new files with the user changes from the Change tables/files
        make_ss_file_and_db_changes(self) # blank in tenement_geoprovince was the result of drop_duplicate replacing value with Nan for no reason
        # builds the files (update and change) that record the additions, removals and changes made for the relevant ids
        build_update_and_change_files(self)
        # delete the rows from the change, addition and remove files for both data_groups. This will prevent an error in the next step when their foreign keys may be deleted.
        #   these rows will be added again later.
        delete_updating_rows_from_updating_db_tables(self)
        # makes the changes to the core file and the db
        make_core_file_and_db_changes(self) 
        # create the change, add and remove tables and update them in the core files and database 
        build_update_tables_update_db(self)
        # create qgis compatible files for tenement & occurrence files
        create_qgis_spatial_files(self)

    print('Find changes and updates: %s' %(time_past(func_start,time.time())))




def sqlalchemy_engine(db_configs):
    return sqlalchemy.create_engine('postgresql://%s:%s@%s/%s' %(db_configs['user'], db_configs['password'], db_configs['host'], db_configs['dbname']))

def connect_psycopg2(db_keys):
    return psycopg2.connect("dbname='%s' user='%s' password='%s' host='%s'" %(db_keys['dbname'],db_keys['user'],db_keys['password'],db_keys['host']))


def clearDatabaseTable(conn, table_name):
    cur = conn.cursor()
    cur.execute("DELETE FROM %s"%(table_name))
    rows_deleted = cur.rowcount
    conn.commit()
    print("%s rows cleared from %s"%(rows_deleted, table_name))


def clear_db_table_rows_in_lst(conn, table, field, lst):
    ''' clears all the rows in a given db table where the values in the field given exist in the given list '''
    cur = conn.cursor()
    lst_count = len(lst)
    if lst_count == 1:
        command = "DELETE FROM %s WHERE %s = CAST(%s AS varchar)"%(table,field,lst[0])
    else:
        command = "DELETE FROM %s WHERE %s IN %s"%(table,field,tuple(lst))
    
    if lst_count == 0:
        rows_deleted = 0
    else:
        cur.execute(command)
        rows_deleted = cur.rowcount
        conn.commit()
    print("%s rows cleared from %s"%(rows_deleted, table))


def update_db_table_by_index_field_and_value_lst(conn, table_name, dic):
    ''' update list of values for a given field in a database '''
    cur = conn.cursor()
    update_index = dic['index']
    update_field = dic['field']
    for x in dic['lst']:
        if type(x[1]) == str: # required for the size_id update where the index is a string
            command = "UPDATE %s SET %s = '%s' WHERE %s = '%s'"%(table_name.lower(),update_field,x[1],update_index,x[0])
        else:
            command = "UPDATE %s SET %s = %s WHERE %s = '%s'"%(table_name.lower(),update_field,x[1],update_index,x[0])
        cur.execute(command)
        conn.commit()
    print('%s rows updated for %s'%(len(dic['lst']),table_name))



def append_df_to_db_table(con,table_name,df):
    ''' append a df to its database table '''
    try:
        df.to_sql(table_name,con,if_exists='append',index=False, method='multi')
    except Exception as e:
        print(repr(e))
        con.close()
        sys.exit(1)


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


def commit_all_files_to_db(self):
    print('Clearing rows from database.')
    db_keys = self.access_configs[self.db_location]
    con = sqlalchemy_engine(db_keys).connect()
    conn = connect_psycopg2(db_keys)
    # print(engine.table_names()) # print all tables in the database

    configs = self.update_configs
    orig_lst = [ table for table in configs ] # get a list of all the database tables

    ordered_tables, temp_lst = orderTables(configs,orig_lst,[],[]) #orders the tables so there are no conflicts when entering into the database

    # delete all data in all tables in order
    for table in ordered_tables[::-1]: 
        table_name = "gp_%s"%(table.lower())
        try:
            clearDatabaseTable(conn,table_name)
        except OperationalError:
            print('This is a delete rows, server close error: %s'%(table))
        except Exception as e:
            # print(str(e))
            print(repr(e))
            con.close()
            conn.close()
            sys.exit(1)


    # drop all table names that are to do with updating. These Won't be added on the initial table creation.
    update_tables_lst = [x for x in configs if configs[x]["update_table"] != None]
    ordered_tables = [x for x in ordered_tables if not x in update_tables_lst]
    print("Pushing tables to database.")
    for table in ordered_tables:        
        print('Loading: %s'%(table))
        path = os.path.join(self.change_dir,"%s.csv"%(table))
        table_name = "gp_%s"%(table.lower())

        try:
            df = pd.read_csv(path)
        except:
            df = pd.read_csv(path,engine='python')

        df = convert_date_fields_to_datetime(df)

        try:
            df.to_sql(table_name,con,if_exists='append',index=False, method='multi')
        except Exception as e:
            # print(e.args)
            # print the error without all the sql
            print(repr(e))
            sys.exit(1)
        # except OperationalError:
        #     print('This is a enter rows, server close error: %s'%(table))

    con.close()
    print('Complete.')


# def insert_into_db(df,table_name,con):
#     df.to_sql(table_name,con,if_exists='append',index=False, method='multi')
    

def create_empty_change_file(self):
    df = pd.DataFrame()
    df.to_csv(self.changes_path)


# 'TYPE' = the type of action = reomve or add 
# 'GROUP' = occurrence or tenement = data_group
# 'TABLE' = the table in question = file
# 'KEY_VALUE' = gplore id
# 'CHANGE_FIELD' = the field the change is in
# 'VALUE' = the value (the removed value or the added value)
# there needs to be a type so I can have adds and removes. i.e. This works well for materials where there may be mutliple adds or removes while others remaining.

def build_update_and_change_files(self):
    ''' final_changes_df: where a key exists in the new & core files but has at least one difference in its fields.
        final_updates_df: Where a key exists only in either the new or core files. If only in the new then it is a new entry, only 
            in the core than it becomes a inactive entry
    '''
    # configs
    update_configs = self.update_configs

    # Create the empty dfs for the change and update data
    final_changes_df = pd.DataFrame(columns=['TYPE','GROUP','TABLE','KEY_VALUE','CHANGE_FIELD','VALUE'])
    final_updates_df = pd.DataFrame(columns=['TYPE','GROUP','KEY_VALUE'])

    # 'occurrence' then 'tenement'
    for data_group in self.data_groups:
        # Find the change ids and look for differences between the core and new. I will need to loop through all the related files. 
        # If there are changes, then add the id to the update file and change file
        datagroup_update_df = pd.read_csv(os.path.join(self.input_dir,data_group,'update','update.csv'))
        # Gets the df of the ids to be removed.
        datagroup_remove_df = datagroup_update_df[datagroup_update_df["ACTION"] == "REMOVE"][["NEW_ID"]]
        date
        # list of files to record the changes in the change file for
        file_lst = [x for x in update_configs if update_configs[x]["record_changes"] != None and update_configs[x]["record_changes"]["data_group"] == data_group and update_configs[x]["record_changes"]["track_changes"]]

        # loop through db update configs and only act on tables/files that are associated with the data_group i.e. have an ind field.
        for file in file_lst:
            # if file in ['tenement_majmat']:
            record_changes = update_configs[file]["record_changes"]
            # if record_changes != None and record_changes["data_group"] == data_group:
            # set path of the file in the output/new directory
            new_path = os.path.join(self.new_dir,"%s.csv"%(file))
            # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
            if fileExist(new_path):
                print("working on: %s"%(file))
                # set the key (index) column and the fields to drop from the db_update_configs file
                key = record_changes["key"]
                drop_fields = record_changes["drop_fields"]
                # get the core file and filter for the change ids
                core_df = pd.read_csv(os.path.join(self.core_dir,"%s.csv"%(file)))
                new_df = pd.read_csv(new_path)

                # get ids from the new_df, then compare to the core_df ids. Then we can find which are ADD and which are CHANGE
                new_ids_df = new_df[[key]].drop_duplicates()
                core_ids_df = core_df[[key]].drop_duplicates()
                merge_df = new_ids_df.merge(core_ids_df,indicator=True,how='outer')

                # add ids are those that exist in the new_df and not the core_df, but only for the Tenement & Occurrence files/tables.
                if file.lower() == data_group:
                    add_ids_df = merge_df[merge_df["_merge"] == "left_only"].drop(columns=["_merge"])
                    # Add the ADD updates to the update df
                    final_updates_df = concat_to_update_df(final_updates_df,add_ids_df,"ADD",data_group)

                # change ids are those that exist in both
                change_ids_df = merge_df[merge_df["_merge"] == "both"].drop(columns=["_merge"])

                # get the rows of the core and new df for the change ids
                core_change_df = pd.merge(change_ids_df, core_df, on=key).drop(columns=drop_fields)
                new_change_df = pd.merge(change_ids_df, new_df, on=key).drop(columns=drop_fields)

                # merge the two to find only the rows that are different
                merged_df = core_change_df.merge(new_change_df,indicator=True,how='outer')
                diff_df = merged_df[merged_df["_merge"] != "both"]

                # if len is 0 then there were no differences, therefore no further action required
                if len(diff_df.index) > 0:
                    # get the ids for the change rows 
                    change_ids_lst = diff_df[key].drop_duplicates().values.tolist()
                    core_change_df = core_df[core_df[key].isin(change_ids_lst)].drop(columns=drop_fields)
                    new_change_df = new_df[new_df[key].isin(change_ids_lst)].drop(columns=drop_fields)
                    columns = list(core_change_df.columns)
                    columns.pop(columns.index(key))

                    changes_df = find_differences_in_each_field(fields=columns,key=key,core_df=core_change_df,new_df=new_change_df,data_group=data_group,file=file)
                    final_changes_df = pd.concat((final_changes_df,changes_df))

        # Add the REMOVE updates to the update df
        final_updates_df = concat_to_update_df(final_updates_df,datagroup_remove_df,"REMOVE",data_group)

    # add the ID and DATE fields, write and append to core file
    add_id_date_fields_and_write_files(self,final_changes_df,self.changes_path,self.core_changes_path)
    add_id_date_fields_and_write_files(self,final_updates_df,self.updates_path,self.core_updates_path)



def add_id_date_fields_and_write_files(self,df,file_path,core_file_path):
    ''' adds the id column to the df. It will start from the next number if data already exists in the file.
        adds a date field so we know what date the changes were made on.
        The data is saved in two places:
            1. update directory 'file_path': Only the current changes for this update are saved here. It will be overwirtten on each update.
            2. core directory 'core_file_path': The changes are concatenated to the maintained change dataset which is a copy of the table in the db
    '''
    # drop any duplicates
    df.drop_duplicates(inplace=True)
    # add date field
    df["DATE"] = self.pyDate
    # concat to core file
    if fileExist(core_file_path):
        core_file_df = pd.read_csv(core_file_path)
        # core_changes_df = pd.read_csv(self.core_changes_path)
        next_id = core_file_df["_ID"].max() + 1
        df["_ID"] = np.arange(next_id, len(df) + next_id)
        core_file_df = pd.concat((core_file_df,df))
    else:
        df["_ID"] = np.arange(1, len(df) + 1)
        core_file_df = df
    # write to csv
    df.to_csv(file_path,index=False)
    # write updated core file to the core directory
    core_file_df.to_csv(core_file_path,index=False)



def concat_to_update_df(main_df,join_df,typ,data_group):
    join_df["TYPE"] = typ
    join_df["GROUP"] = data_group
    join_df.columns = ["KEY_VALUE","TYPE","GROUP"]
    return pd.concat((main_df,join_df))


# gets the list of files that are part of the currect data_group and have their changes recorded.
def get_file_lst(configs,data_group):
    lst = []
    for file in configs:
        # tables that have their changes recorded in the change file
        record_changes = configs[file]["record_changes"]
        if record_changes != None:
            if record_changes["data_group"] == data_group:
                lst.append(file)
            elif record_changes["second_data_group"] != None and record_changes["second_data_group"]["data_group"] == data_group:
                lst.append(file)
    return lst



def assign_ids(add_df,core_df,edit_core_df):
    ''' This manages the '_id' field when concatenating the new/changed rows to the core file. As all the change rows are removed from the core file and db table
        the rows that have only been assigned a change need to have their original _id value re-assigned. The new values need to be given a new _id value which continues
        from the max value of the existing _id values so there is no conflict when pushing to the database.
        add_df: holds the data to add to the core file and db
        core_df: the core file before any data was deleted
        edit_core_df: the core file after the change and remove rows are deleted
    '''
    diff_df = add_df.merge(core_df, how='outer', on=['name_id','tenement_id'], suffixes=('', '_y'), indicator=True)
    exist_df = diff_df[diff_df['_merge'] == 'both'].drop(columns=['_id','percown_y','_merge']).rename(columns={'_id_y': '_id'})
    new_df = diff_df[diff_df['_merge'] == 'left_only'].drop(columns=['_id','_id_y','percown_y','_merge'])

    core_build_df = pd.concat((edit_core_df,exist_df))
    next_id = core_build_df['_id'].max() + 1
    new_df["_id"] = np.arange(next_id, len(new_df) + next_id)

    final_add_df = pd.concat((exist_df,new_df))

    return final_add_df


def make_ss_file_and_db_changes(self):
    ''' This method performs two tasks:
        1. updates in the new file are applied to the ss file so the ss file is maintained as a true copy of the state sources. This is used to determine
            which changes have come from the state sources and which are user edits.
        2. loops through the files that are editable by the user, then loops through the relevant fields and updates the new file depending on which of the following 
            four update methods is required:
                new-edits-changes-single: if ss is different to new then replace value, if the same then use the core value, this will maintain user edits
                edits-new-single: regardless if the new file is different, always leave the same as the core. add the new values
                new-edits-changes-multi: keep the user edits, add new ss rows, remove dropped ss rows if not added by user
                new-edits-changes-tenholder: only used for TenHolder. It updates the holders and ther percown from the state sources along with carrying forward the user edits
    ''' 
    print("Updating the ss files. These are a maintained dataset of the state source data")
    # Configs
    update_configs = self.update_configs

    # list of all files that have their changes tracked
    all_file_lst = [x for x in update_configs if update_configs[x]['record_changes'] != None and update_configs[x]['record_changes']['user_edits'] != None]

    # loop through the datagroups i.e. occurrence and tenements
    for data_group in self.data_groups:
        self.data_group = data_group

        # rows related to ind values that are either new or have a change related to them somewhere
        new_grp_df = pd.read_csv(os.path.join(self.new_dir,"%s.csv"%(data_group.capitalize())))['ind']
        # a maintained file that is updated with the new data on each new download of state data. No user edits are found here, only state data
        ss_grp_df = pd.read_csv(os.path.join(self.ss_dir,"%s.csv"%(data_group.capitalize())))['ind']

        # merge the new & ss group files together to find which ind values are new and which are changes
        grp_merge_df = pd.merge(ss_grp_df, new_grp_df, how='outer', indicator=True)
        # list of the new ind values. Rows with these values are new and therefore is no need to look for changes wither in the ss or core file
        new_ind_lst = grp_merge_df.query('_merge == "right_only"').drop('_merge', 1)['ind'].values.tolist()
        # list of the change values. Not all rows with these values are different between the ss & new, but differrences do exist, they just need to be found throu comparison
        change_ind_lst = grp_merge_df.query('_merge == "both"').drop('_merge', 1)['ind'].values.tolist()
        
        # list of files to update for the currect data_group
        file_lst = [x for x in all_file_lst if update_configs[x]['record_changes']['data_group'] == data_group]
        
        for file in file_lst:
            # if file == 'tenement_geoprovince':
            # field that holds the ind value. Usually get the key from the update_configs, but this was much simpler
            key = 'ind' if data_group.capitalize() == file else "%s_id"%(data_group)
            # set the new & ss df's for the current file
            ss_path = os.path.join(self.ss_dir,"%s.csv"%(file))
            new_path = os.path.join(self.new_dir,"%s.csv"%(file))
            ss_df = pd.read_csv(ss_path)
            new_df = pd.read_csv(new_path)
            # remove all the rows with the updating ind values
            reduced_ss_df = ss_df[~ss_df[key].isin(change_ind_lst)].copy()
            # concat the new to the adjusted ss_df to create the updated ss_df
            final_ss_df = pd.concat((reduced_ss_df,new_df))

            # set the core df for the current file
            core_df = pd.read_csv(os.path.join(self.core_dir,"%s.csv"%(file)))
            # filter the core & ss df for rows for ind values with potential changes.
            f_core_df = core_df[core_df[key].isin(change_ind_lst)]
            f_ss_df = ss_df[ss_df[key].isin(change_ind_lst)]

            # get a list of the fields for the current file which need to be compared in search of changes and user edits
            field_lst = [x for x in update_configs[file]['record_changes']['user_edits']['actions']]

            for field in field_lst:
                print("Working on: %s, Field: %s"%(file,field))
                # the action to determine how the field is updated. See in the desciption above for more details
                action = update_configs[file]['record_changes']['user_edits']['actions'][field]

                # extract only the key and current field from the three df's. This will allow the df's to be compared more easily
                t_core_df = f_core_df[[key,field]]
                t_ss_df = f_ss_df[[key,field]]
                t_new_df = new_df[[key,field]]

                # list of new ind values that won't exist in the core or ss, as they are new. All there relations are added
                a_new_df = t_new_df[t_new_df[key].isin(new_ind_lst)]
                # list of ind values that have fields somewhere that are difference to the ss file, these are compared to the core
                c_new_df = t_new_df[t_new_df[key].isin(change_ind_lst)]

                if action == 'new-edits-changes-single':
                    # All the values that have been changed in the the state source
                    change_diff_df = pd.merge(t_ss_df,t_new_df,how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', 1)
                    # only update the filtered core df if there are differences between the ss & new file
                    if len(change_diff_df.index) > 0:
                        # the other values will remain the same as the core
                        col_core_df = t_core_df[~t_core_df[key].isin(change_diff_df[key])]
                        # concat the two together
                        concat_df = pd.concat((col_core_df,change_diff_df))
                        # concat the rows from the new_df that don't exist in the core/ss df's
                        new_rows_df = t_new_df[~t_new_df[key].isin(concat_df[key])]
                        concat_df = pd.concat((concat_df,new_rows_df))
                        # update the new_df with the new data
                        new_df = pd.merge(new_df,concat_df,on=key,suffixes=("", "_x")).drop(field, 1).rename(columns={"%s_x"%(field): field})

                elif action == 'edits-new-single':

                    # concat the new rows, to add the new data, and the filtered core rows to maintain all user edits. 
                    concat_df = pd.concat((t_core_df,a_new_df))
                    # update the new df
                    new_df = pd.merge(new_df,concat_df,on=key,suffixes=("", "_x")).drop(field, 1).rename(columns={"%s_x"%(field): field})

                    # # df of only the new values that don't exist in the core file
                    # new_only_df = new_df[~new_df[key].isin(t_core_df[key])]
                    # # concat new values with the core values
                    # concat_df = pd.concat((t_core_df,new_only_df))
                    # # update the new df
                    # new_df = pd.merge(new_df,concat_df,on=key,suffixes=("", "_x")).drop(field, 1).rename(columns={"%s_x"%(field): field})
                    
                elif action == 'new-edits-changes-multi':

                    # if in new and not in ss then add
                    add_df = pd.merge(t_ss_df,c_new_df,how='outer', indicator=True).query('_merge == "right_only"').drop('_merge', 1)

                    # find user edits by comparing ss to core. 
                    diff_df = pd.merge(t_ss_df,t_core_df,how='outer', indicator=True)
                    # those in core but not in ss are user edits to add
                    user_add_df = diff_df.query('_merge == "right_only"').drop('_merge', 1)
                    # those in ss but not in core have been removed by the user
                    user_drop_df = diff_df.query('_merge == "left_only"').drop('_merge', 1)
                    # those that exist in new and core are kept
                    same_df = pd.merge(t_core_df,c_new_df,how='inner')
                    # put the new all together
                    sfinal_df = pd.concat((a_new_df,add_df,user_add_df,same_df))
                    # drop rows dropped by user
                    # print(new_df[new_df['tenement_id'] == 1028563])
                    new_df = sfinal_df.merge(user_drop_df,how='left').drop_duplicates()
                    # print(len(new_df.index))
                    # temp_df = sfinal_df[~sfinal_df.isin(user_drop_df)].drop_duplicates()
                    # print(len(temp_df.index))
                    # # new_df.drop_duplicates(ignore_index=True) # this converts the integer field to float and replaced a non-duplicate value to Nan

                    # print(new_df[new_df['tenement_id'] == 1028563])
                    # print(temp_df[temp_df['tenement_id'] == 1028563])

                elif action == 'new-edits-changes-tenholder':
                    
                    h_core_df = f_core_df[[key,field,'percown']]
                    h_ss_df = f_ss_df[[key,field,'percown']]
                    h_new_df = new_df[[key,field,'percown']]

                    # list of new ind values that won't exist in the core or ss, as they are new. All there relations are added
                    ah_new_df = h_new_df[h_new_df[key].isin(new_ind_lst)]
                    # list of ind values that have fields somewhere that are difference to the ss file, these are compared to the core
                    ch_new_df = h_new_df[h_new_df[key].isin(change_ind_lst)]

                    # rows with existing ind values but new name_id relations. 
                    add_df = pd.merge(h_ss_df,ch_new_df, on=[key,field], how='outer', indicator=True, suffixes=('_x','')).query('_merge == "right_only"').drop(columns=['_merge','percown_x'], axis=1)
                    
                    # compare ss & core to get all user edit rows. this does not include percown field
                    diff_df = pd.merge(h_ss_df,h_core_df, on=[key,field], how='outer', indicator=True).query('_merge == "left_only"')
                    # those in core but not in ss are user edits to add
                    user_add_df = diff_df.query('_merge == "right_only"').drop(['_merge','percown_x'], 1).rename(columns={'percown_y':'percown'})
                    # those in ss but not in core have been removed by the user. These rows are dropped from the df at the end
                    user_drop_df = diff_df.query('_merge == "left_only"').drop(['_merge','percown_y'], 1).rename(columns={'percown_x':'percown'})

                    # rows that exist in the new, core & ss need to have their percown field updated accordingly. If the percown is different between the ss & core
                    #   then this means it is a user edit which will need to be transfered to the final df to maintian the edits, but if the new percown is different
                    #   to the ss df, then there has been a state source update and this new value needs to be updated.

                    # find the rows for ind/name_id that exist in the core and new df. This is used to determine if the percown is a user edit or not
                    same_df = pd.merge(h_core_df,ch_new_df, on=[key,field], how='inner', suffixes=('_x','')).drop('percown_x',1)
                    # find the common ss rows that exist in both the core & new df's. This is used to update the percown field. 
                    common_ss_df = pd.merge(same_df,h_ss_df, on=[key,field], how='outer', indicator=True, suffixes=('_x','')).query('_merge == "both"').drop(columns=['_merge','percown_x'],axis=1)
                    common_core_df = pd.merge(same_df,h_core_df, on=[key,field], how='outer', indicator=True, suffixes=('_x','')).query('_merge == "both"').drop(columns=['_merge','percown_x'],axis=1)
                    # find the rows where the percown is different between the new & ss. These are updates from the state source and therefore overwite everything
                    ss_update_df = pd.merge(common_ss_df,same_df, how='outer', indicator=True).query('_merge == "right_only"').drop('_merge',1)
                    # common_core_df holds all the user edits, along with the rest of the other non-edited rows. just need to drop rows that exist in ss_update_df and concat it
                    final_common_df = pd.merge(common_core_df,ss_update_df, how='outer', indicator=True, on=[key,field], suffixes=('','_x')).query('_merge == "left_only"').drop(['_merge','percown_x'],1)
                    # combine all the df's to create create the final table. The rows the user dropped still need to be removed below
                    sfinal_df = pd.concat((ah_new_df,add_df,user_add_df,final_common_df,ss_update_df))
                    # drop rows dropped by user
                    new_df = pd.merge(sfinal_df,user_drop_df, how='outer', indicator=True, on=[key,field], suffixes=('','_x')).query('_merge == "left_only"').drop(['_merge','percown_x'],1)
                    new_df["_id"] = np.arange(1, len(new_df) + 1)

            # overwrite the new file with the updated new df
            new_df.to_csv(new_path,index=False)
            # write the updated ss df to file. This is at the end of the method incase of an error, which would prevent a re-run
            final_ss_df.to_csv(ss_path,index=False)
            




def make_core_file_and_db_changes(self):
    ''' Creates the change files which are the snippets of data which will then be pushed to the db. The changes are also applied to 
        the core files so they mimic the db.
            inactive_ids: only applies to Occurrence & Tenement files. holds the configs on how to deal with entires that have been dropped from the datasets
    '''
    print("Creating change files, updating core files and updating the database.")
    # Configs
    update_configs = self.update_configs
    access_configs = self.access_configs
    # db connections
    sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()
    psycopg2_con = connect_psycopg2(access_configs[self.db_location])
    
    # loop through the datagroups i.e. occurrence and tenements
    for data_group in self.data_groups:
    # for data_group in ['occurrence']:
        self.data_group = data_group
        # initialise dictionary to hold the change file dfs
        change_dic = {}

        # create a list of the add, change & remove values from the updates.csv and changes.csv files.
        remove_keys_lst, add_keys_lst, change_keys_lst = getRemoveAddChangeIdLists(self)

        # gets the list of files that are part of the current data_group and have their changes recorded or are tables that hold these recorded changes.
        file_lst = get_file_lst(update_configs,data_group)

        #orders the tables so there are no conflicts when entering into or removing from the database
        ordered_tables, temp_lst = orderTables(update_configs,file_lst,[],[]) 
        # I only want the tables that are in file_lst
        ordered_file_lst = [x for x in ordered_tables if x in file_lst]

        for file in ordered_file_lst[::-1]:
            # set path of the file in the output/new directory
            new_path = os.path.join(self.new_dir,"%s.csv"%(file))
            # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
            if fileExist(new_path):
                print("Working on: %s"%(file))
                # set the paths
                core_path = os.path.join(self.core_dir,"%s.csv"%(file))
                change_file_path = os.path.join(self.change_dir,"%s.csv"%(file))

                # set the key (index) column. 
                record_changes = update_configs[file]["record_changes"]
                key = record_changes["key"] if record_changes["data_group"] == data_group else record_changes["second_data_group"]["key"]

                # get the core file and filter for the change ids
                core_df = pd.read_csv(core_path)
                new_df = pd.read_csv(new_path)

                # Get the relevant rows from the new file. These values are the additions and rows with a change. This will be the change file that is added to the db.
                to_add_df = new_df[new_df[key].isin(add_keys_lst + change_keys_lst)].copy()

                # Tenement: change the status of the removal ids to inactive, adds new ids and updates change ids.
                # Occurrence: No change to removal ids, adds new ids, updates change ids. No need to drop as an occurrence shouldn't just disappear.
                # other: deletes remove ids, adds new ids and updates change ids
                inactive_ids = update_configs[file]["inactive_ids"]
                if inactive_ids == None:
                    # drop the relevant rows from the core df and concat to to_add_df
                    remove_from_core_lst = change_keys_lst + remove_keys_lst
                    # these are the ids that will be deleted from the db table
                    db_remove_lst = stringifyIntLst(remove_from_core_lst)
                else:
                    if inactive_ids["type"] == "update":
                        if len(remove_keys_lst) != 0:
                            # I want to keep the rows that have been removed from the download files but change their status to inactive and remove all data in m2m fields.
                            remove_core_df = core_df[core_df[key].isin(remove_keys_lst)].copy()
                            # update the values in the STATUS column
                            remove_core_df["status_id"] = inactive_ids["value"]
                            # append the updated remove_core_df to the to_add_df so when it is added back to the core df it will take the updated remove_core_df with it.
                            to_add_df = pd.concat((to_add_df,remove_core_df))
                            remove_from_core_lst = change_keys_lst + remove_keys_lst
                            db_remove_lst = stringifyIntLst(remove_from_core_lst)
                        else:
                            remove_from_core_lst = change_keys_lst
                            db_remove_lst = stringifyIntLst(change_keys_lst)
                    elif inactive_ids["type"] == "keep":
                        remove_from_core_lst = change_keys_lst
                        db_remove_lst = stringifyIntLst(change_keys_lst)


                # drop the relevant rows from the core df and concat to to_add_df. This will be the new core file.
                edit_core_df = core_df[~core_df[key].isin(remove_from_core_lst)]
                #  If the table contains the _id field, such as TenHolder, then the index field needs to be calculated starting from the last of the core_df values.
                if "_id" in edit_core_df.columns:
                    to_add_df = assign_ids(to_add_df,core_df,edit_core_df)
                edit_core_df = pd.concat((edit_core_df,to_add_df))

                # Write to core csv and change csv
                edit_core_df.to_csv(core_path,index=False)
                to_add_df.to_csv(change_file_path,index=False)

                # delete all rows from the db
                # field = update_configs[file]["columns"][key]
                field = key
                table = "gp_%s"%(file.lower())
                # clear necessary rows from the db table 
                if len(db_remove_lst) > 0:
                    clear_db_table_rows_in_lst(psycopg2_con, table, field, db_remove_lst) # uncomment this

                # Place change_file_df into a dictionary. Tables need to be appended in the db in order. Therefore, they need to be saved in a dictionary and added in the next step
                change_dic[file] = to_add_df

        
        # write dfs to db in order
        print("Appending data to database for the %s data_group"%(data_group))
        for file in ordered_file_lst:
            print("Writing to Database: %s"%(file))
            table = "gp_%s"%(file.lower())
            df = change_dic[file]
            # df = format_date_columns(df)
            df.to_sql(table, sqlalchemy_con, if_exists='append', index=False, method='multi') # uncomment this to push to db




# create a list of the add, change & remove values from the updates.csv and changes.csv files.
def getRemoveAddChangeIdLists(self):
    # Read update and change files to df
    update_records_df = pd.read_csv(self.updates_path)
    change_records_df = pd.read_csv(self.changes_path)
    # separate the ids by type and datagroup.
    remove_keys_lst = update_records_df[(update_records_df["GROUP"] == self.data_group) & (update_records_df["TYPE"] == "REMOVE")]["KEY_VALUE"].values.tolist()
    add_keys_lst = update_records_df[(update_records_df["GROUP"] == self.data_group) & (update_records_df["TYPE"] == "ADD")]["KEY_VALUE"].values.tolist()
    change_keys_lst = list(set(change_records_df[change_records_df["GROUP"] == self.data_group]["KEY_VALUE"].values.tolist()))
    return remove_keys_lst, add_keys_lst, change_keys_lst



# get the ids that will be deleted from the db table. convert to int to remove the decimal and then to string as that is how it is stored in the db
def stringifyIntLst(lst):
    return [str(int(x)) for x in lst]



def compare_base_tables_add_new(self):
    ''' This compares the new files, produced from the vba macros, to the core files. It filters out the ids from the core_df that 
        exist in the new_df and then concatenates the two df's together.
        comparing 'Holder' will always return 0 as new names are added to the core file before running the vba macro. The new names are added 
        to the db in the previous step.
        Base tables are those that are not related to either the Occurrence or Tenement tables. They are tables like Listed, HolderType, OccType etc that when
        updated don't require the Tenement or Occurrence to be updated first to prevent a non existing id error.
        Also excludes the Change, Addition & Removal tables
    '''
    print("Finding new entries for the base tables and updating the core file and database.")
    # Configs
    update_configs = self.update_configs
    access_configs = self.access_configs

    # db connections
    sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()

    file_lst = [x for x in update_configs if update_configs[x]["is_base_table"]]
    
    for file in file_lst:
        # set path of the file in the output/new directory
        new_path = os.path.join(self.new_dir,"%s.csv"%(file))
        # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
        if fileExist(new_path):
            print("Working on: %s"%(file))
            # Set the paths
            core_path = os.path.join(self.core_dir,"%s.csv"%(file))
            change_file_path = os.path.join(self.change_dir,"%s.csv"%(file))

            # Get the core file and filter for the change ids
            core_df = pd.read_csv(core_path,engine="python")
            new_df = pd.read_csv(new_path,engine="python")
            
            # get the df of only the new ids
            ind = update_configs[file]["index"]
            new_ids_df = new_df[~new_df[ind].isin(core_df[ind])].copy()

            # only required if there are new ids
            if len(new_ids_df.index) > 0:
                # renumber the _ID field if it exists
                if "_id" in new_ids_df.columns:
                    next_id = core_df["_id"].max() + 1
                    new_ids_df["_id"] = np.arange(next_id, len(new_ids_df) + next_id)

                # concat new rows to the core_df & save
                new_core_df = pd.concat((core_df,new_ids_df))
                new_core_df.to_csv(core_path,index=False)

                # append new rows to the database table
                table = "gp_%s"%(file.lower())
                # new_ids_df = convert_date_fields_to_datetime(new_ids_df)

                # print(new_ids_df.head())
                new_ids_df.to_sql(table, sqlalchemy_con, if_exists='append', index=False, method='multi') # uncomment

    sqlalchemy_con.close()


def convert_date_fields_to_datetime(df):
    for col in df.columns:
        if col in ['date_modified','date_created','date']:
            df[col] = pd.to_datetime(df[col]).copy()
    return df


def update_db_for_manually_handled_core(self):
    ''' The 'Holder' file is updated manually when running the tenement vba macro, thus when the new and core files are compared there 
        are no new values to add to the db. This is not true, so this method is to update the db for manually managed core files so the core is 
        maintained as a copy of its equivalent db table.
        This works by getting the last _id value from the db and then adding all rows with an _id greater than that to the table.
    '''
    print("Updating db for manually managed core files.")
    # Configs
    access_configs = self.access_configs
    # db connections
    sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()
    psycopg2_con = connect_psycopg2(access_configs[self.db_location])

    # put Holder.csv into a df
    core_path = os.path.join(self.core_dir,'Holder.csv')
    core_df = pd.read_csv(core_path,engine="python")

    # get the max _id value from the db
    cur = psycopg2_con.cursor()
    cur.execute("SELECT MAX(_id) FROM gp_holder")
    max_id = cur.fetchall()[0][0]
    cur.close()

    # filter the core_df for _id above max_id
    new_values_df = core_df[core_df['_id'] > max_id]
    if len(new_values_df.index) > 0:
        # new_values_df = convert_date_fields_to_datetime(new_values_df) # I don't think i need this now that i have changed the date formatting in the vba macros
        new_values_df.to_sql('gp_holder', sqlalchemy_con, if_exists='append', index=False, method='multi')

    sqlalchemy_con.close()


def drop_id(val):
    if val[len(val)-3:] == '_id':
        return val[:-3]
    else:
        return val

# builds the change file for the required datagroup, updates its equivalent core file and adds it to the database
def build_group_change_file(self):
    ''' This builds the TenementChange & OccurrenceChange dataframes which are saved in the 'update' directory, appended to the core file and 
        appended to the database table. It loops through the rows in 'change.csv' that was created earlier and separates the fields into their 
        own column so when they are loaded into the database they are treated as foreign keys to their respective models.
        When loading the df to the database it is split by field and loaded separately so the foreign key fields can be converted to integers first. Without splitting
        fields pandas will not allow the conversion of a mixture of Nonetypes/Nans and strings to integers and therefore will throw an error when appending to the db table. 
    '''
    print("Creating the database ready change file for data group %s"%(self.data_group))

    file_name = "%sChange"%(self.data_group.capitalize())
    self.group_change_path = os.path.join(self.update_dir,"%s.csv"%(file_name))
    self.group_change_core_path = os.path.join(self.core_dir,"%s.csv"%(file_name))

    config = self.update_configs[file_name]["columns"]
    # use this to correct the names in the field column. It drops all the '_id' suffix which will make it the same as the django model field names
    field_config = {x:drop_id(config[x]) for x in config}
    change_lst = self.change_df[self.change_df["GROUP"] == self.data_group].drop(columns=["GROUP","_ID"]).values.tolist()

    # get the headers from the config file
    headers = [x for x in config]

    lst = []
    count = 0 # used for the index column
    for row in change_lst:
        count += 1
        # assign the values in each row to variables
        action, table, ind, field, value, date = tuple([row[x] for x in range(6)])
        # as the MATERIAL field exists in both major and minor tables, extracting whether it is a maj or min from the table name and concatenating it to 
        # the field name with distinguish between the two.
        if "mat" in table:
            field = "%s_%s"%(field,table.split("_")[1][:3].upper())
        # create the row. only the changed value will sit in 1 of the 9 None columns. This is found using its field which is the same of the column header.
        working_lst = [count,ind,action,field,date,"ss"]
        # insert the correct number of None values in the list. This is found by subtracting 5 from the headers list.
        for x in range(len(headers)-6):
            working_lst.insert(-2,None)
        # only append if the field exists in the headers. No need to add things like region which cannot change and only appear because the title has become 
        # inactive and had its fields removed.
        if field in headers:
            working_lst[headers.index(field)] = value
            lst.append(working_lst)

    df = pd.DataFrame(lst,columns=headers)
    # replace the field names with the database field values in the config file
    df.replace({"FIELD": field_config},inplace=True)
    # rename columns to suit the db table
    df.rename(columns=config,inplace=True)

    # if the core file exists then concat latest df to it, otherwise export the new df to the update and core directories.
    final_df = export_as_csv_to_update_and_core(self,df,self.group_change_path,self.group_change_core_path,file_name)
    # append to the database table
    append_to_db_by_field(self,final_df,file_name)


def append_to_db_by_field(self,df,file_name):
    ''' Split the 'Change' df by field and convert the relevant fields to integer type and push to the database '''
    table = "gp_%s"%(file_name.lower())
    # fields that need to be comverted to integer type before being appended to the database
    int_fields = ['statusval_id', 'nameval_id', 'typeval_id','statusval_id', 'holderval_id']
    # list of the fields in the change df
    field_lst = list(dict.fromkeys(df['field'].values.tolist()))
    for group in field_lst:
        select_df = df[df['field'] == group].copy()
        if group in int_fields:
            select_df[group] = select_df[group].astype(float).astype(int)
        select_df.to_sql(table, self.sqlalchemy_con, if_exists='append', index=False, method='multi')



# build the addition and removal database ready files
def build_group_addition_remove_files(self):
    ''' The 'update.csv' created earlier has all the Tenement & Occurrence ind values that were either added or removed from the state source data.
        This data is then split between datagroup and whether it was added or deleted and then added to the core file and the db table.
    '''
    data_group = self.data_group
    print("Creating the database ready addition & removal files for data group %s"%(data_group))

    # filter the df for the current data group
    add_remove_df = self.update_df[self.update_df["GROUP"] == data_group]

    # 2 types of actions for indexes that have been removed and those that have been added. the second value is suffix of the model name.
    for action in [["ADD","Addition"],["REMOVE","Removal"]]:
        df = add_remove_df[add_remove_df["TYPE"] == action[0]][["KEY_VALUE","DATE"]]
        df["_ID"] = np.arange(1, len(df) + 1)
        df.rename(columns={"_ID": "_id", "KEY_VALUE": "ind_id", "DATE": "date"},inplace=True)
        # set the paths
        file_name = "%s%s"%(data_group.capitalize(),action[1])
        self.group_action_path = os.path.join(self.update_dir,"%s.csv"%(file_name))
        self.group_action_core_path = os.path.join(self.core_dir,"%s.csv"%(file_name))
        # if the core file exists then concat latest df to it, otherwise export the new df to the update and core directories.
        final_df = export_as_csv_to_update_and_core(self,df,self.group_action_path,self.group_action_core_path,file_name)
        # format date column and append to database
        # final_df = format_date_columns(final_df) # delete this
        table = "gp_%s"%(file_name.lower())
        final_df.to_sql(table, self.sqlalchemy_con, if_exists='append', index=False, method='multi')
        


# if the core file exists then concat latest df to it, otherwise export the new df to the update and core directories.
def export_as_csv_to_update_and_core(self,df,update_dir,core_dir,file_name):

    # create update file and add entries to the core file
    if fileExist(core_dir):
        # read the core equivalent file
        core_df = pd.read_csv(core_dir)

        # correct the ids so they continue on from the max of the core file
        next_id = core_df["_id"].max() + 1 if len(core_df.index) > 0 else 1
        df["_id"] = np.arange(next_id, len(df) + next_id)

        # This part will get the core rows that were deleted before updating the other tables. This needs to be re-added to the database. 
        # create a list of the add, change & remove values from the updates.csv and changes.csv files.
        remove_keys_lst, add_keys_lst, change_keys_lst = getRemoveAddChangeIdLists(self)
        core_ids_to_readd_lst = stringifyIntLst(remove_keys_lst + change_keys_lst)
        field = self.update_configs[file_name]["index"]
        readd_df = core_df[core_df[field].isin(core_ids_to_readd_lst)]

        # This will overwrite the core file with the new additions. No need to add readd_df here as it was only deleted from the db table.
        updated_core_df = pd.concat((core_df,df))
        # write to csv
        df.to_csv(update_dir,index=False)
        updated_core_df.to_csv(core_dir,index=False)

        # concat the readd_df to the df to add not only the new additions, but also the change and remove rows that were deleted earlier.
        df = pd.concat((readd_df,df))
    else:
        # write to csv
        df.to_csv(update_dir,index=False)
        df.to_csv(core_dir,index=False)

    return df




# delete the rows from the change, addition and remove files for both data_groups. This will prevent an error in the next step when their 
# foreign keys may be deleted. these rows will be added again later.
def delete_updating_rows_from_updating_db_tables(self):
    ''' This will get the ids of the rows that are to be updated which comes from the update & change files created in the previous step. Rows with these ids 
        are then removed from the Change, Remove & Addition tables.
    '''
    print("Removing all change & remove id rows from the database.")
    # Configs
    update_configs = self.update_configs
    access_configs = self.access_configs
    # db connections
    psycopg2_con = connect_psycopg2(access_configs[self.db_location])

    # loop through the datagroups i.e. occurrence and tenements
    for data_group in self.data_groups:
    # for data_group in ['tenement']:
        self.data_group = data_group
        # create a list of the add, change & remove values from the updates.csv and changes.csv files.
        remove_keys_lst, add_keys_lst, change_keys_lst = getRemoveAddChangeIdLists(self)

        keys_to_remove_lst = stringifyIntLst(remove_keys_lst + change_keys_lst)

        # list of tables/files that contain the updating tables data for the currect data_group
        file_lst = [ x for x in update_configs if update_configs[x]["update_table"] != None and update_configs[x]["update_table"]["data_group"] == data_group]

        # loop through the files and delete all the remove_keys_lst from its table.
        for file in file_lst:
            file_config = update_configs[file]
            # field = file_config["columns"][file_config["index"]]
            field = file_config["index"]
            table = "gp_%s"%(file.lower())
            # clear necessary rows from the db table
            clear_db_table_rows_in_lst(psycopg2_con, table, field, keys_to_remove_lst)

    psycopg2_con.close()



# create the change, add and remove tables and update them in the core files and database
def build_update_tables_update_db(self):
    ''' Build the 'Change', 'Additional' & 'Removal' core files and append them to the database tables ''' 
    # open as df
    self.update_df = pd.read_csv(self.updates_path)
    self.change_df = pd.read_csv(self.changes_path)
    # db connection
    self.sqlalchemy_con = sqlalchemy_engine(self.access_configs[self.db_location]).connect()
    
    for data_group in self.data_groups:
        self.data_group = data_group
        # builds the change file for the required datagroup, updates its equivalent core file and adds it to the database
        build_group_change_file(self)
        # # build the addition and removal database ready files
        # build_group_addition_remove_files(self)

    self.sqlalchemy_con.close()



def copy_new_files_to_core(self):
    ''' Copy entire files from the new directory to the core directory when in the db_update_configs file_type = update or replace.
        This is only used on the initial creation on of tables, after, the core files are update with changes only.
    '''
    print('Copying required files from new to core, change & ss directories')
    update_configs = self.update_configs
    # files to copy from new directory to core directory
    copy_files = ["%s.csv"%(x) for x in update_configs if update_configs[x]['file_type'] in ['update','replace']]
    # copy files from new directory to core directory. The core set of data will be updated from both user edits and state source updates
    copy_directory_in_list(copy_files,self.new_dir,self.core_dir)
    # copy files from new directory to ss directory. This is the core set of data that is only updated from changes in the new state data and not user edits
    copy_directory(self.core_dir,self.ss_dir)
    # copy the core directory to the change directory. Files from the change directory are pushed to the db
    copy_directory(self.core_dir,self.change_dir)



def backup_new_useredit_file(self):
    ''' This is designed to copy the new files to the onew folder before user edits are applied. This will provide a set of data to compare to the post user edit new
        data.
    '''
    print('Copying required files from new to onew directory before user edits are applied')
    update_configs = self.update_configs
    # files to copy from new directory to core directory
    file_lst = ["%s.csv"%(x) for x in update_configs if update_configs[x]['record_changes'] != None and update_configs[x]['record_changes']['user_edits'] != None]
    # copy files from the new directory to the onew directory. 
    copy_directory_in_list(file_lst,self.new_dir,self.onew_dir)



def find_differences_in_each_field(fields,key,core_df,new_df,data_group,file):
    ''' loops through each field of two df and compares the values in them with a unique key to match them. Values that 
        exist only in the new_df are new and recorded in the changes_df as NEW, while those that only exist in the core_df have been 
        removed and thus are recorded as REMOVE in the changes_df. If the values are the same then no changes have taken place and nothing is added to 
        the changes_df. 
        The changes_df is then used to update the core_df which is a dataset that mirrors the db dataset.
    '''
    # Create an empty df for the change data 
    changes_df = pd.DataFrame(columns=['TYPE','GROUP','TABLE','KEY_VALUE','CHANGE_FIELD','VALUE'])

    # dic to convert the filed name
    lst = ['occurrence_majmat','occurrence_minmat','tenement_majmat','tenement_minmat','TenHolder']

    dic = { "status_id": "STATUS", "typ_id": "TYPE", "lodgedate": "LODGEDATE", "startdate": "STARTDATE", "enddate": "ENDDATE",
             "occurrence_majmat-material_id": "MATERIAL_MAJ", "occurrence_minmat-material_id": "MATERIAL_MIN",
             "tenement_majmat-material_id": "MATERIAL_MAJ", "tenement_minmat-material_id": "MATERIAL_MIN",
             "tenoriginalid_id": "RELATEDID", "occoriginalid_id": "RELATEDID",
             "TenHolder-name_id": "HOLDER_ID", "occname_id": "NAME", "occtype_id":"TYPE"}


    # loop through the fields of the table and record the additions and removals
    for field in fields:
        # print("%s - %s"%(field, file))
        core_field_df = core_df[[key,field]].drop_duplicates()
        new_field_df = new_df[[key,field]].drop_duplicates()
        merge_field_df = core_field_df.merge(new_field_df,indicator=True,how='outer')
        remove_df = merge_field_df[merge_field_df["_merge"] == "left_only"].drop(columns="_merge")
        add_df = merge_field_df[merge_field_df["_merge"] == "right_only"].drop(columns="_merge")
        for type_group in [[remove_df,"REMOVE"],[add_df,"ADD"]]:
            df = type_group[0]
            df.columns = ["KEY_VALUE","VALUE"]
            df["GROUP"] = data_group
            df["TYPE"] = type_group[1]
            df["TABLE"] = file
            lookup = "%s-%s"%(file,field) if file in lst else field
            new_field = field if lookup not in dic else dic[lookup]
            # df["CHANGE_FIELD"] = field
            df["CHANGE_FIELD"] = new_field
            # concat the df to the maintained changes_df 
            changes_df = pd.concat((changes_df,df))
    return changes_df

    # i can use the file name to determine the actual field name
    # tenement = {"LODGEDATE": "lodgedateval", "STARTDATE": "startdateval", "ENDDATE": "enddateval", "HOLDER_ID": "holderval_id", "TYPE": "typeval_id", "STATUS": "statusval_id", "RELATEDID": "oidval_id", "MATERIAL_MAJ": "majmatval_id", "MATERIAL_MIN": "minmatval_id", "DATE": "date"}

    # occurrence = { "TYPE": "typeval_id", "STATUS": "statusval_id", "RELATEDID": "oidval_id", "NAME": "nameval_id", "MATERIAL_MAJ": "majmatval_id", "MATERIAL_MIN": "minmatval_id", "DATE": "date" }


def format_date(date_string):
    ''' formats date from y-m-d to d/m/y '''
    try:
        s = str(date_string).split('-')
        return "%s/%s/%s"%(s[2],s[1],s[0])
    except:
        return date_string


def format_date_r(date_string):
    ''' formats date from d/m/y to m-d-y '''
    if '/' in date_string:
        s = str(date_string).split('/')
        return "%s-%s-%s"%(s[2],s[1],s[0])
    else:
        return date_string


def format_date_columns(df):
    ''' format the date columns in a given df '''
    columns = [ x for x in df.columns if 'date_' in x ]
    for col in columns:
        df[col] = df[col].apply(lambda x: format_date_r(x))
    return df

def format_date_columns_b(df):
    ''' format the date columns in a given df '''
    columns = [ x for x in df.columns if 'date' in x ]
    for col in columns:
        df[col] = df[col].apply(lambda x: format_date(x))
    return df



def add_relation_core_rows_to_new_file(self):
    ''' When the relation files are created on update, ind values are added to the change file in the 'update' directory, but the data for these ind values are not
        in the 'new' files. So later on, these rows are removed from the database but are not reinstated again as it does not exist in the 'new' file. 
        This method finds these 'ind' values, loops through all relevant files and adds all the rows of data with the ind value.
    '''
    print("Adding spatial additions to new files")
    # Configs
    update_configs = self.update_configs

    # get the 'ind' values from the tenement_occurrence file that don't exist in the 'new' Tenement or Occurrence files
    ten_occ_df = pd.read_csv(os.path.join(self.new_dir,'tenement_occurrence.csv'))
    ten_df = pd.read_csv(os.path.join(self.new_dir,'Tenement.csv'))
    ten_lst = ten_occ_df[~ten_occ_df['tenement_id'].isin(ten_df['ind'])]['tenement_id']
    # ten_lst = ten_occ_df[~ten_occ_df['tenement_id'].isin(ten_df['ind'])]['tenement_id'].values.tolist()
    # ten_lst = list(dict.fromkeys(ten_lst))

    # # data_group = 'tenement'
    # # file_lst = get_file_lst(update_configs,data_group)
    # # # exclude files were created in the geospatial relation step.
    # # exclude_files = ['tenement_majmat', 'tenement_minmat', 'tenement_occurrence']
    # # select_file_lst = [x for x in file_lst if update_configs[x]['record_changes']['data_group'] == data_group and x not in exclude_files]

    # list of files to update
    file_lst = [x for x in update_configs if update_configs[x]['record_changes'] != None and update_configs[x]['record_changes']['relation_update']]

    for file in file_lst:
        print('Working on: %s'%(file))

        new_path = os.path.join(self.new_dir,'%s.csv'%(file))
        new_df = pd.read_csv(new_path)
        core_df = pd.read_csv(os.path.join(self.core_dir,'%s.csv'%(file)))

        key = update_configs[file]['record_changes']['key']

        filtered_core_df = core_df[core_df[key].isin(ten_lst)]
        final_new_df = pd.concat((new_df,filtered_core_df))

        final_new_df.to_csv(new_path,index=False)




# Resets the database back to a previous archived core tables.
def previous_core_to_db(self):
    func_start = time.time()
    # the output archive directory that hold the previous set of core files
    self.output_archive_dir = os.path.join(self.output_dir,'archive')
    # get the latest folder archive folder
    archive_date = os.listdir(self.output_archive_dir)[-1]
    # output archive directory. The previous core data from the output archive directory
    previous_core_dir = os.path.join(self.output_archive_dir,archive_date,'core')
    # configs
    self.update_configs = getJSON(os.path.join(self.configs_dir,'db_update_configs.json'))
    self.access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))

    # the archived core directory needs to be set as change_dir so it works with the commit_all_files_to_db function which is used else where.
    self.change_dir = previous_core_dir
    # clear the tables from the db and load the latest archived core files.
    commit_all_files_to_db(self)

    print('Find changes and updates: %s' %(time_past(func_start,time.time())))



def create_qgis_spatial_files(self):
    ''' create the qgis compatible files for the tenement & occurrence files '''
    print("Creating qgis compatible files")

    for directory in [self.core_dir,self.new_dir]:
        for file in ['Occurrence','Tenement']:
        # for file in ['OccGeom','TenGeom']:
            df = pd.read_csv(os.path.join(directory,'%s.csv'%(file)))
            df['geom'] = df['geom'].apply(lambda x: x.replace("SRID=4202;",""))
            df.rename(columns={'geom': 'WKT'}, inplace=True)
            df.to_csv(os.path.join(directory,'qgis_%s.csv'%(file)),index=False)



            

# def update_new_files_with_user_changes(self):
#     ''' This method looks through the TenementChange, OccurrenceChange & HolderChange tables for the user changes and then adds them to the new file
#         so in the next step when all rows with the ind value in the new file are deleted, the new file rows to be added will maintain the user updates.
#         I will need to use both the core file and the Change file so I am not adding data that the Change file says exists which doesn't exist in the core file. This 
#         shouldn't be an issue, but a good safety net.
#     '''
#     # Configs
#     update_configs = self.update_configs

#     # this in only adding updates to the new file
#     dic = {
#         "occurrence": {
#             "majmatval_id": {"action": "apply all changes from change file, leave new ss values"},
#             "minmatval_id": {"action": "apply all changes from change file, leave new ss values"},
#             "nameval_id": {"action": "apply all changes from change file, leave new ss values"}, 
#             "oidval_id": {"action": "apply all changes from change file, leave new ss values"},  
#             "statusval_id": {"action": "update if ss value has changed"}, 
#             "typeval_id": {"action": "update if ss value has changed"}, 
#             "geoprovinceval_id": {"action": "apply all changes from change file, leave new ss values"},  
#             "sizeval_id": {"action": "apply all changes from change file, leave new ss values"}, 
#         },
#         "tenement": {
#             "lodgedateval": {"action": None}, 
#             "startdateval": {"action": None}, 
#             "enddateval": {"action": None},  
#             "oidval_id": {"action": "apply all changes from change file, leave new ss values"}, 
#             "statusval_id": {"action": "update if ss value has changed"}, 
#             "typeval_id": {"action": "update if ss value has changed"}, 
#             "geoprovinceval_id": {"action": "apply all changes from change file, leave new ss values"}, 
#             "holderval_id": {"action": "update if ss value has changed"},
#             "holderperc": {"action": "update if ss value has changed"},
#         }
#     }

#     # might be easier to maintain a ss updated core file set. This woulf allow the comparison of the new files and which values have actually changed.
#     #   it wouldn't need to be every file, just those that are user editable.


#     # get a list of all the tables which have changes recorded
#     all_file_lst = [x for x in update_configs if update_configs[x]["record_changes"] != None and update_configs[x]["record_changes"]['track_changes']]

#     for data_group in self.data_groups:

#         change_path = os.path.join(self.core_dir,"%sChange.csv"%(data_group.capitalize()))

#         if fileExist(change_path):

#             change_df = pd.read_csv(change_path,engine='python')
#             change_df = change_df[change_df['user'] != 'ss']

#             print(change_df.head())

#             # get the ind values from the data_group. These are all the instances that contain changes somewhere
#             df = pd.read_csv(os.path.join(self.new_dir,"%s.csv"%(data_group.capitalize())),engine="python")
#             ind_lst = df['ind'].values.tolist()

#             file_lst = [x for x in all_file_lst if update_configs[x]["record_changes"]['data_group'] == data_group]
        
#             for file in file_lst:

#                 new_path = os.path.join(self.new_dir,"%s.csv"%(file))
#                 # core_df = pd.read_csv(os.path.join(self.core_dir,"%s.csv"%(file)),engine="python")
#                 new_df = pd.read_csv(new_path,engine="python")

#                 for ind in ind_lst:
#                     pass








            # print(file)
            # # set path of the file in the output/new directory
            # new_path = os.path.join(self.new_dir,"%s.csv"%(file))
            # # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
            # if fileExist(new_path):
            #     print("Working on: %s"%(file))
            #     # Set the paths
            #     core_path = os.path.join(self.core_dir,"%s.csv"%(file))
            #     change_file_path = os.path.join(self.change_dir,"%s.csv"%(file))

            #     # Get the core file and filter for the change ids
            #     core_df = pd.read_csv(core_path,engine="python")
            #     new_df = pd.read_csv(new_path,engine="python")

            #     # get the df of only the new ids
            #     ind = update_configs[file]["index"]














# def copy_to_us_core_add_fields(self):
#     ''' copy the files from the ds_new folder to the us_core folder. As this is the initial dataset and not updating, the only
#         thing that is required is to add the three extra columns, 'user_input', 'valid' & 'modified' to the editable tables from the frontend.
#         user_input: True if the user has made updates to the field
#         valid: True if the data has been checked and verified. Only False after a user has made an update
#         modified: The date the last time the field was editied
#     '''
#     func_start = time.time()
#     print('Copying all files from ds_new to us_core and adding user_input, valid & modified fileds to editable tables.')
#     # increase the field size to handle the Tenement WKT field
#     csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
#     # copy files from new_dir to the us_core_dir
#     copy_directory(self.new_dir,self.us_core_dir)
#     # add extra fields to relevant tables
#     # for table in ["OccName","OccOriginalID","TenOriginalID","Listed","Holder","Parent","TenHolder","Occurrence","Tenement"]:
#     for table in ["Holder","Occurrence","Tenement"]:
#         print('working on: %s'%(table))
#         path = os.path.join(self.us_core_dir,table + '.csv')
#         df = pd.read_csv(path, engine="python")
#         df['user_input'] = False
#         df['valid'] = True
#         df['modified'] = datetime.now()
#         if table == 'Holder':
#             df['created'] = datetime.now()
#         df.to_csv(path,index=False)

#     for table in ["OccName","OccOriginalID","TenOriginalID","Listed"]:
#         print('working on: %s'%(table))
#         path = os.path.join(self.us_core_dir,table + '.csv')
#         df = pd.read_csv(path, engine="python")
#         df['valid'] = True
#         df['created'] = datetime.now()            
#         df.to_csv(path,index=False)


#     print('Copying files to us_change folder.')
#     copy_directory(self.us_core_dir,self.us_change_dir)
#     print('Complete')
#     print('Copy to us_core & us_change time: %s' %(time_past(func_start,time.time())))



# # update core & user files with user updates
# def record_user_edits(self):
#     ''' When a change is made in the application, the instance, whether is it related to the Tenement, Occurrence or Holder dataset has the
#         user_input & valid fields updated to True & False respectively. This function filters for these changes and gets the ids of these fields that
#         have been updated. It is not recorded which field is updated, but with the ids each of the related db tables are filtered by these keys and compared 
#         to its equivalent core file. This comparison finds the values that have been added, or removed by the user and added to a core file named 'change.csv' 
#     '''
#     # increase the field size to handle the Tenement WKT field
#     csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
#     # Configs
#     access_configs = self.access_configs
#     update_configs = self.update_configs

#     # db connections
#     sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()

#     # loop through Tenement and Occurrence
#     for data_group in self.data_groups:

#         # get the ids of the occurrence table where valid = False & input = True
#         sql = "SELECT ind FROM gp_%s WHERE valid = False AND user_input = True"%(data_group)
#         ind_lst = pd.read_sql(sql, sqlalchemy_con)['ind'].values.tolist()
        
#         # if the length of the ind_lst is equal to 0 then there are no user updates, so no need to proceed.
#         if len(ind_lst) > 0:
#             for file in update_configs:
#                 record_changes = update_configs[file]["record_changes"]
#                 if record_changes != None and record_changes["data_group"] == data_group:
#                     # if file == 'Occurrence':
#                     # print(file)
#                     # extract the data from the db table for ind's in the ind_lst
#                     # get the tables db field names
#                     columns = update_configs[file]['columns']
#                     rev_columns = {columns[x]: x for x in columns}
#                     drop_fields = record_changes['drop_fields']
#                     drop_fields = drop_fields + ['user_input','valid','modified','created']
#                     key = record_changes['key']
#                     db_fields = ','.join([columns[x] for x in columns if x not in drop_fields])
#                     core_path = os.path.join(self.core_dir,"%s.csv"%(file))

#                     sql = "SELECT %s FROM gp_%s WHERE %s IN %s"%(db_fields,file.lower(),columns[key],tuple(ind_lst))
#                     user_df = pd.read_sql(sql, sqlalchemy_con).rename(columns=rev_columns)
#                     core_df = pd.read_csv(core_path)
#                     core_columns = core_df.columns
#                     drop_fields = [x for x in drop_fields if x in core_columns]
#                     loop_columns = [x for x in core_columns if x not in drop_fields]
#                     core_df.drop(columns=drop_fields,inplace=True)
#                     core_df = core_df[core_df[key].isin(ind_lst)]
#                     user_df = user_df.astype(core_df.dtypes.to_dict())
#                     merge_df = user_df.merge(core_df,indicator=True,how='outer')

#                     if len(merge_df.index) > 0:
#                         final_changes_df = find_differences_in_each_field(fields=loop_columns,key=key,core_df=core_df,new_df=user_df)

#     # adds date and id field and commits to csv files. 
#     add_id_date_fields_and_write_files(self,final_changes_df,self.changes_path,self.core_changes_path)

#     sqlalchemy_con.close()





# def transfer_user_edits_to_core(self):
#     ''' This function loops over the three groups; Holder, Tenement & Occurrence. It filters for rows where 'user_edit=True' which means that a user 
#         has made an edit to the group file or one of its related files. If there are rows returned, then two steps follow; 
#             1. This group file is updated by filtering out the edited rows and replacing with their updated rows from the db table and saved as the core file.
#             2. Each of the related tables are looped and the edited ids are filtered in both the db and core files. These df's are then compared and 
#                 if there are differences then the changes are applied to the core file and saved
#         There are no recording of changes in the 'Change' file here as this is done in the application when a change is made.
#     '''
#     print('Updating core files with changes in db')
#     csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

#     # Configs
#     access_configs = self.access_configs
#     update_configs = self.update_configs

#     # db connections
#     sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()

#     # this will create a list with 'Holder','Occurrence','Tenement'
#     creation_tables = [x for x in update_configs if update_configs[x]['db_to_core_transfer'] != None]

#     for group in creation_tables:
#         # if group == 'Holder':
#         print('Working on Group: %s'%(group))

#         dic = update_configs[group]['db_to_core_transfer']
#         pk = dic['pk']
#         related_field = dic['related_field']
#         is_geospatial = dic['is_geospatial']
#         table_lst = dic['table_lst']

#         # if there are no user edit rows then get all user edits from the db, otherwise find the date of the last user_edit from the core file 
#         # and get the new user edits from this date
#         core_path = os.path.join(self.core_dir,"%s.csv"%(group))
#         core_df = pd.read_csv(core_path,engine='python')
#         user_edit_df = core_df[core_df['user_edit'] == True]

#         if user_edit_df.empty:
#             sql = "SELECT * FROM gp_%s WHERE user_edit = True"%(group.lower())
#         else:
#             # latest_date = format_date_r(user_edit_df['date_modified'].max())
#             latest_date = user_edit_df['date_modified'].max()
#             sql = "SELECT * FROM gp_%s WHERE user_edit = True AND date_modified >= '%s'"%(group.lower(), latest_date)

#         # get the ids of only the latest 'user_edit' values from the db
#         group_db_df = gpd.GeoDataFrame.from_postgis(sql, sqlalchemy_con) if is_geospatial else pd.read_sql(sql, sqlalchemy_con)
#         # these are the keys of the instances that have had at least one update made by the user in either the group or its related tables
#         keys_df = group_db_df[pk].values.tolist()

#         # If there are no new rows with user_edits then no need to progress, the core file is already up to date
#         if len(keys_df) > 0:

#             # filter out these keys from the core_df, they will be replaced with the rows from the db
#             reduced_core_df = core_df[~core_df[pk].isin(keys_df)]

#             # filter the db data for the necessary ids, format the date columns & add the crs prefix to the geom column
#             group_db_df = group_db_df[group_db_df[pk].isin(keys_df)]
#             # create list of date fields to format for this file
#             # date_columns = [x for x in group_db_df.columns if x in ['date_created','date_modified','lodgedate','startdate','enddate']]
#             # for col in date_columns:
#             #     group_db_df[col] = group_db_df[col].apply(lambda x: format_date(x))

#             if is_geospatial:
#                 group_db_df = geoDfToDf_wkt(group_db_df).drop(columns=['geometry'])
#                 group_db_df['geom'] = group_db_df['geom'].apply(lambda x: "{}{}".format('SRID=4202;',x))

#             # join the two df's to create the updated df and save
#             final_df = pd.concat((reduced_core_df,group_db_df))

#             final_df.to_csv(core_path,index=False) # uncomment this

#             # update the edit file. this file contains only the user edits
#             edit_path = os.path.join(self.edit_dir,"%s.csv"%(group))
#             if fileExist(edit_path):
#                 edit_df = pd.read_csv(edit_path,engine='python')
#                 final_edit = pd.concat((edit_df,group_db_df)).drop_duplicates(ignore_index=True)
#             else:
#                 final_edit = group_db_df

#             final_edit.to_csv(edit_path,index=False)

#             for table in table_lst:
#                 print("Working on related table: %s"%(table))
#                 # get the values from the core file
#                 core_path = os.path.join(self.core_dir,"%s.csv"%(table))
#                 # loop through all the possible fields. 
#                 for target_field in related_field:
#                     # read the core_df. It needs to be re-read after each loop to include updates from the previous loop
#                     core_df = pd.read_csv(core_path)
#                     # only take action if the field exists in the df. This is the case for the 'Holder' group only
#                     if target_field in core_df.columns:
#                         filtered_core_df = core_df[core_df[target_field].isin(keys_df)].copy()
#                         # convert ind values to 'str' format. Currently, they are an 'object' in the db_df and 'int64' in the core file which will cause an error on merge
#                         convert_columns = [x for x in filtered_core_df.columns if x in ['tenement_id','occurrence_id']]
#                         for col in convert_columns:
#                             filtered_core_df[col] = filtered_core_df[col].astype(str)

#                         # get the values from the db table
#                         if len(keys_df) == 1:
#                             if is_geospatial:
#                                 # I have to cast to text otherwise i get an error even if i pass the key as a string
#                                 sql = "SELECT * FROM gp_%s WHERE %s = CAST(%s as text)"%(table.lower(),target_field,keys_df[0])
#                             else:
#                                 sql = "SELECT * FROM gp_%s WHERE %s = %s"%(table.lower(),target_field,keys_df[0])
#                         else:
#                             sql = "SELECT * FROM gp_%s WHERE %s in %s"%(table.lower(),target_field,tuple(keys_df))
#                         db_df = pd.read_sql(sql, sqlalchemy_con)

#                         # drop the 'id' field of the m2m fields. these are automatically populated
#                         if 'id' in db_df.columns: 
#                             db_df.drop(columns=['id'],inplace=True)

#                         # merge the db & core values to find if there are any differences between the two. Firstly, convert db_df dtypes to that of the core_df
#                         db_df = db_df.astype(filtered_core_df.dtypes.to_dict())
#                         merge_df = filtered_core_df.merge(db_df,indicator=True,how='outer')
#                         diff_df = merge_df[merge_df["_merge"] != 'both']

#                         # if the diff_df length is more than 0 then the core file needs to be updated. remove the rows with the filtered ids, concat the updated rows and save.
#                         if len(diff_df.index) > 0:
#                             reduced_core_df = core_df[~core_df[target_field].isin(keys_df)]
#                             final_df = pd.concat((reduced_core_df,db_df))
#                             final_df.to_csv(core_path,index=False) # uncomment this
#                             # save edits to the edit file. This is only used to view the edits separatly
#                             edit_path = os.path.join(self.edit_dir,"%s.csv"%(table))
#                             if fileExist(edit_path):
#                                 edit_df = pd.read_csv(edit_path,engine='python')
#                                 final_edit = pd.concat((edit_df,db_df)).drop_duplicates(ignore_index=True)
#                             else:
#                                 final_edit = db_df
#                             final_edit.to_csv(edit_path,index=False)

#     sqlalchemy_con.close()


# def transfer_user_creations_to_core(self):
#     ''' Copy the user created instances from db tables and add to the core file '''
#     print("Copy the user created instances to their core file.")
#     # Configs
#     access_configs = self.access_configs
#     update_configs = self.update_configs

#     # db connections
#     sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()

#     for table in ['OccName','OccOriginalID','TenOriginalID','Listed','Holder']:
#         print("Updating: %s"%(table))
#         core_path = os.path.join(self.core_dir,"%s.csv"%(table))
#         edit_path = os.path.join(self.edit_dir,"%s.csv"%(table))
#         core_df = pd.read_csv(core_path,engine='python')
#         temp_df = core_df.copy()
#         # temp_df['date_created'] = temp_df['date_created'].apply(lambda x: format_date_r(x))
#         last_date = temp_df[['date_created']].max()
#         f_date = last_date[0]
#         # Get the latest rows entered by users by filtering from the latest date in the core file and where user_name is 'user'
#         sql = "SELECT * FROM gp_%s WHERE user_name = 'user' AND CAST(date_created as date) >= '%s'"%(table.lower(),f_date)
#         # convert the blanks to Nan
#         df = pd.read_sql(sql, sqlalchemy_con).fillna(value=np.nan)
#         # no need to continue if there are no new rows from the db
#         if len(df.index) > 0:
#             # convert dates to d/m/y format
#             # df = format_date_columns_b(df)
#             # concat user edits to the edit file. create new file if it doesn't exist 
#             if fileExist(edit_path):
#                 edit_df = pd.read_csv(edit_path,engine='python')
#                 final_edit = pd.concat((edit_df,df)).drop_duplicates(ignore_index=True)
#             else:
#                 final_edit = df
#             final_edit.to_csv(edit_path,index=False)
#             # concat the new rows from the db table to the core file and drop the duplicates
#             final_core_df = pd.concat((core_df,df)).drop_duplicates(ignore_index=True)
#             # overwrite the core file with the updates
#             final_core_df.to_csv(core_path,index=False)




# def transfer_changes_to_core(self):
#     ''' Copy the Change tables from the db and concatenate the user additions to the core file '''
#     print("Updating the core Change files with the latest db updates")
#     # Configs
#     access_configs = self.access_configs
#     update_configs = self.update_configs

#     # db connections
#     sqlalchemy_con = sqlalchemy_engine(access_configs[self.db_location]).connect()

#     for table in ['OccurrenceChange','TenementChange','HolderChange']:
#         # if table == 'HolderChange':
#         print("Working on: %s"%(table))
#         # get the date of the last user entry. This is the date the db search will filter from 
#         core_path = os.path.join(self.core_dir,"%s.csv"%(table))
#         if fileExist(core_path):
#             core_df = pd.read_csv(core_path)
#             if len(core_df.index) > 0:
#                 temp_df = core_df.copy()
#                 # temp_df['date_created'] = temp_df['date_created'].apply(lambda x: format_date_r(x))
#                 last_date = temp_df[['date_created']].max()
#                 f_date = last_date[0]
#                 # last_date = core_df[['date_created']].max()
#                 # last_date = pd.to_datetime(last_date)[0]
#                 # f_date = (last_date - timedelta(days=1)).strftime('%Y%m%d')
#             else:
#                 # f_date = date(2000, 2, 1).strftime('%Y%m%d')
#                 f_date = '2000-02-01'
#         else:
#             core_df = pd.DataFrame()
#             f_date = '2000-02-01'
#             # f_date = date(2000, 2, 1).strftime('%Y%m%d')

#         # e_date = (date.today() + timedelta(days=1)).strftime('%Y%m%d')
#         # Get the latest rows from the Change tables by filtering rows after the last date in the core file. I was not able to query for user = 'user', so i used pandas as below
#         sql = "SELECT * FROM gp_%s WHERE CAST(date_created as date) >= '%s'"%(table.lower(),f_date)
#         # drop id column and convert all None to Nan
#         df = pd.read_sql(sql, sqlalchemy_con).fillna(value=np.nan)
#         df = df[df['user'] != 'ss']
#         # convert all types to string so there are no differences between types when dropping duplicates, then replace 'nan'
#         df = df.astype(str).replace('nan', np.nan, regex=True)
#         core_df = core_df.astype(str).replace('nan', np.nan, regex=True)
#         # convert the date format of the db table to fit the csv format
#         # # date_columns = [x for x in df.columns if 'date' in x]
#         # # for col in date_columns:
#         # #     df[col] = df[col].apply(lambda x: format_date(x))
#         # if the core_df is empty then create core with the db df, otherwise concat the db table with the core file. If there are duplicates, the first will be kept
#         #  and the others deleted
#         final_core_df = df if core_df.empty else pd.concat((core_df,df)).drop_duplicates(ignore_index=True)
#         # only create file if the df is not empty. An empty df might cause issues later when trying to find the max _id value that would't exist
#         if not final_core_df.empty:
#             final_core_df.to_csv(core_path,index=False)








# if 'geom' in core_df.columns:
        #     core_df['geom'] = core_df['geom'].str.replace('SRID=4202;', '')
        #     core_df = df_to_geo_df_wkt(core_df)

        # filtered_core_df = core_df[core_df['ind'].isin(keys_df)].copy()

        # filtered_core_df = filtered_core_df.astype({'ind': 'str'})

        # for col in ['date_created','date_modified']:
        #     group_db_df[col] = group_db_df[col].apply(lambda x: format_date(x))

        # group_db_df = group_db_df[["ind", "geom", "govregion_id", "localgov_id", "size_id", "state_id", "status_id", "date_modified", "valid_instance", "date_created"]].copy()
        # filtered_core_df = filtered_core_df[["ind", "geom", "govregion_id", "localgov_id", "size_id", "state_id", "status_id", "date_modified", "valid_instance", "date_created"]].copy()

        # merge_df = group_db_df.merge(filtered_core_df,indicator=True,how='outer')
        # diff_df = merge_df[merge_df["_merge"] != 'both']
        # print(merge_df)

        # # print(group_db_df.head())
        # print(group_db_df.dtypes)
        # # print(filtered_core_df.head())
        # print(filtered_core_df.dtypes)






# core_path = os.path.join(self.core_dir,"%s.csv"%(table))
            # core_df = pd.read_csv(core_path)

            # # remove irrelevant fields and check if any entries alreay exist
            # merge_df = pd.merge(new_df,core_df,how="left",on='NAME',suffixes=("", "_core"))
            # drop_columns = [x for x in merge_df if '_core' in x]
            # unique_new_df = merge_df.drop(columns=drop_columns)
            # # append to the core and update
            # core_df = core_df.append(unique_new_df)
            # overwrite the core file



# if len(merge_df.index) > 0:
                    #     for field in loop_columns:
                    #         core_field_df = core_df[[key,field]].drop_duplicates()
                    #         user_field_df = user_df[[key,field]].drop_duplicates()
                    #         merge_field_df = core_field_df.merge(user_field_df,indicator=True,how='outer')
                    #         remove_df = merge_field_df[merge_field_df["_merge"] == "left_only"].drop(columns="_merge")
                    #         add_df = merge_field_df[merge_field_df["_merge"] == "right_only"].drop(columns="_merge")
                    #         for type_group in [[remove_df,"REMOVE"],[add_df,"ADD"]]:
                    #             df = type_group[0]
                    #             df.columns = ["KEY_VALUE","VALUE"]
                    #             df["GROUP"] = data_group
                    #             df["TYPE"] = type_group[1]
                    #             df["TABLE"] = file
                    #             df["CHANGE_FIELD"] = field
                    #             # concat the df to the maintained final_changes_df 
                    #             final_changes_df = pd.concat((final_changes_df,df))


    # sql = "SELECT * FROM gp_occname WHERE valid = False"
    # # df = gpd.GeoDataFrame.from_postgis(sql, sqlalchemy_con)
    # df = pd.read_sql(sql, sqlalchemy_con)


# this can be deleted
    # if file == "Tenement":
    #     if len(remove_keys_lst) != 0:
    #         remove_core_df = core_df[core_df[key].isin(remove_keys_lst)].copy()
    #         remove_core_df["STATUS"] = 20
    #         to_add_df.append(remove_core_df,inplace=True)
    #         edit_core_df = core_df[~core_df[key].isin(change_keys_lst + remove_keys_lst)]
    #         edit_core_df = pd.concat((edit_core_df,to_add_df,remove_core_df))
    #     else:
    #         edit_core_df = core_df[~core_df[key].isin(change_keys_lst)]
    #         edit_core_df = pd.concat((edit_core_df,to_add_df))
    #     # these are the ids that will be deleted from the db table
    #     db_remove_lst = [str(int(x)) for x in (change_keys_lst + remove_keys_lst)]
    # elif file == "Occurrence":
    #     edit_core_df = core_df[~core_df[key].isin(change_keys_lst)]
    #     edit_core_df = pd.concat((edit_core_df,to_add_df))
    #     # these are the ids that will be deleted from the db table
    #     db_remove_lst = [str(int(x)) for x in (change_keys_lst)]
    # else:
    #     # drop the relevant rows from the core df and concat to to_add_df
    #     edit_core_df = core_df[~core_df[key].isin(change_keys_lst + remove_keys_lst)]
    #     edit_core_df = pd.concat((edit_core_df,to_add_df))
    #     # these are the ids that will be deleted from the db table
    #     db_remove_lst = [str(int(x)) for x in (change_keys_lst + remove_keys_lst)]


    # # concat

    # # print(datagroup_change_df.head())
    # datagroup_change_df = final_changes_df[['GROUP','KEY_VALUE']].copy()
    # datagroup_change_df["TYPE"] = "CHANGE"
    # final_updates_df = pd.concat((final_updates_df,datagroup_change_df))


    # final_updates_df = final_updates_df[final_updates_df["GROUP"] == "tenement"].copy()

    # # check if there are any duplicate ids between the three types
    # change_update_df = final_updates_df.loc[final_updates_df["TYPE"] == "CHANGE",["KEY_VALUE"]]
    # remove_update_df = final_updates_df.loc[final_updates_df["TYPE"] == "REMOVE",["KEY_VALUE"]]
    # add_update_df = final_updates_df.loc[final_updates_df["TYPE"] == "ADD",["KEY_VALUE"]]
    # change_update_lst = final_updates_df.loc[final_updates_df["TYPE"] == "CHANGE","KEY_VALUE"]
    # remove_update_lst = final_updates_df.loc[final_updates_df["TYPE"] == "REMOVE","KEY_VALUE"]
    # add_update_lst = final_updates_df.loc[final_updates_df["TYPE"] == "ADD","KEY_VALUE"]


    # # print(add_update_df.values.tolist())


    # ten_df = add_update_df[add_update_df["KEY_VALUE"].isin(remove_update_lst.values.tolist())]
    # print(ten_df.head())
    # print(len(ten_df.index))



    # diff_df = add_update_df.merge(final_updates_df.loc[final_updates_df["TYPE"].isin(["REMOVE"]),["KEY_VALUE"]],indicator=True,how='outer')
    # print(len(diff_df.index))
    # print(len(diff_df[diff_df["_merge"] == "both"].index))

    # print(remove_update_df.head())

    # # find remove that exist in new_df
    # ten_df = pd.read_csv(os.path.join(self.new_dir,"Tenement.csv"))
    # ten_ids_df = ten_df["TENID"]

    # ten_df = ten_df[ten_df["TENID"].isin(change_update_df.values.tolist())]
    # # # ten_df = ten_df.loc[ten_df["TENID"].isin([remove_update_df]),["TENID"]]
    # print(ten_df.head())
    # print(len(ten_df.index))




    # save_path = os.path.join(self.new_dir,"%s_poo.csv"%(file))
    # new_df.drop(columns=["WKT"],inplace=True)
    # new_df.to_csv(save_path)







        # print(datagroup_change_lst)

# save_path = os.path.join(self.new_dir,"%s_poo.csv"%(file))
#                         new_df.drop(columns=["WKT"],inplace=True)
#                         new_df.to_csv(save_path)

# def removeOldAddNewToCoreAndDb(self,update_lst):
#     # get the new and old ids for the tenement and occurrence datasets.
#     occurrence_old_ids, occurrence_new_ids, occurrence_change_ids = getOldAndNewIdLists(self.occurrence_update_path)
#     # tenement_old_ids, tenement_new_ids, tenement_change_ids = getOldAndNewIdLists(self.tenement_update_path)
#     # # # remove all old entries and changed entries from db
#     # # removeOldAndChangeEntriesDb(self,{"tenement": tenement_old_ids + tenement_change_ids,"occurrence": occurrence_old_ids + occurrence_change_ids})
#     # # add all new and changed entries to db (need to move this within the atlas app to map shapefiles to db)

#     # # add new and old entries to the update list
#     # update_lst = addNewAndOldEntriesToChangeLst(update_lst,{"tenement":{"REMOVE":tenement_old_ids,"NEW":tenement_new_ids},"occurrence":{"REMOVE":occurrence_old_ids,"NEW":occurrence_new_ids}})
#     # # remove old entries from core file
#     # removeOldEntriesCoreFile(self,{"tenement": tenement_old_ids,"occurrence": occurrence_old_ids})
#     # # correct the foreign key value for required new files
#     # correctNewForeignKeyValues(self)
#     # # add new entries in core file
#     # addNewEntriesCoreFile(self,{"tenement": tenement_new_ids,"occurrence": occurrence_new_ids})

#     return update_lst


# def getOldAndNewIdLists(path):
#     df = pd.read_csv(path)
#     old_ids_lst = convertSingleColumnDfToList(df[df["ACTION"] == "REMOVE"].loc[:,["NEW_ID"]])
#     new_ids_lst = convertSingleColumnDfToList(df[df["ACTION"] == "NEW"].loc[:,["NEW_ID"]])
#     change_ids_lst = convertSingleColumnDfToList(df[df["ACTION"] == "CHANGE"].loc[:,["NEW_ID"]])
#     return old_ids_lst, new_ids_lst, change_ids_lst


# def removeOldAddNewToCoreAndDb(self,update_lst):
#     # get the new and old ids for the tenement and occurrence datasets.
#     occurrence_old_ids, occurrence_new_ids, occurrence_change_ids = getOldAndNewIdLists(self.occurrence_update_path)
#     tenement_old_ids, tenement_new_ids, tenement_change_ids = getOldAndNewIdLists(self.tenement_update_path)
#     # # remove all old entries and changed entries from db
#     # removeOldAndChangeEntriesDb(self,{"tenement": tenement_old_ids + tenement_change_ids,"occurrence": occurrence_old_ids + occurrence_change_ids})
#     # add all new and changed entries to db (need to move this within the atlas app to map shapefiles to db)

#     # add new and old entries to the update list
#     update_lst = addNewAndOldEntriesToChangeLst(update_lst,{"tenement":{"REMOVE":tenement_old_ids,"NEW":tenement_new_ids},"occurrence":{"REMOVE":occurrence_old_ids,"NEW":occurrence_new_ids}})
#     # remove old entries from core file
#     removeOldEntriesCoreFile(self,{"tenement": tenement_old_ids,"occurrence": occurrence_old_ids})
#     # correct the foreign key value for required new files
#     correctNewForeignKeyValues(self)
#     # add new entries in core file
#     addNewEntriesCoreFile(self,{"tenement": tenement_new_ids,"occurrence": occurrence_new_ids})

#     return update_lst

# def addNewAndOldEntriesToChangeLst(update_lst,ids_dic):
#     for category in ['occurrence','tenement']:
#         for action in ['REMOVE','NEW']:
#             for ind in ids_dic[category][action]:
#                 update_lst.append([action,category,ind])
#     return update_lst

# def removeOldEntriesCoreFile(self,old_ids_dic):
#     configs = self.configs
#     for category in [2,1]:
#         for file_name in configs:
#             record_changes = configs[file_name]['record_changes']
#             if record_changes != "" and configs[file_name]['add_category'] == category:
#                 remove_ids = old_ids_dic[record_changes['data_group']]
#                 core_file = "%s%s.csv"%(self.core_directory,file_name)
#                 core_df = pd.read_csv(core_file,engine='python')
#                 result_df = core_df[~core_df[record_changes['key']].isin(remove_ids)]
#                 result_df.to_csv(core_file,index=False)

# def addNewEntriesCoreFile(self,new_ids_dic):
#     configs = self.configs
#     for category in [1,2]:
#         for file_name in configs:
#             record_changes = configs[file_name]['record_changes']
#             if record_changes != "" and configs[file_name]['add_category'] == category:
#                 add_ids = new_ids_dic[record_changes['data_group']]
#                 core_file = "%s%s.csv"%(self.core_directory,file_name)
#                 new_file = "%s%s.csv"%(self.core_directory,file_name)
#                 core_df = pd.read_csv(core_file,engine='python')
#                 new_df = pd.read_csv(new_file,engine='python')
#                 to_add_df = new_df[new_df[record_changes['key']].isin(add_ids)]
#                 result_df.to_csv(core_file,index=False)


# def getOldAndNewIdLists(path):
#     df = pd.read_csv(path)
#     old_ids_lst = convertSingleColumnDfToList(df[df["ACTION"] == "REMOVE"].loc[:,["NEW_ID"]])
#     new_ids_lst = convertSingleColumnDfToList(df[df["ACTION"] == "NEW"].loc[:,["NEW_ID"]])
#     change_ids_lst = convertSingleColumnDfToList(df[df["ACTION"] == "CHANGE"].loc[:,["NEW_ID"]])
#     return old_ids_lst, new_ids_lst, change_ids_lst

# def convertSingleColumnDfToList(df):
#     return [x[0] for x in df.values.tolist()]


# def compareOutputCoreToNew(self,changes_lst):
#     print('Comparing the output new files to the output core files and building the change and update files.')
#     configs = self.configs
#     for file_name in configs:
#         record_changes = configs[file_name]['record_changes']
#         if record_changes != "":
#             # if file_name == 'occurrence_majmat':
#             comparison_type = record_changes['comparison_type']
#             drop_fields = record_changes['drop_fields']
#             key = record_changes['key']
#             data_group = record_changes['data_group']
#             value_field = record_changes['value_field']
#             removal_ids = updates[data_group]
#             new_df, core_df = readAndDropNecessaryColumnsDf(["%s%s.csv"%(self.new_directory,file_name), "%s%s.csv"%(self.core_directory,file_name)],drop_fields)
#             merged_df = core_df.merge(new_df,indicator=True,how='outer')
#             # merged_df = merged_df[merged_df['_merge'] == "right_only"].drop(columns=['_merge'])
#             # filter core_df for keys in the remove list
#             # remove_df = core_df[core_df[key].isin(removal_ids)]
#             if comparison_type == "MULTIPLE": # merged_df was right_only
#                 changes_lst = getMultipleChanges(self,merged_df,changes_lst,core_df,key,file_name,data_group)
#             elif comparison_type == "SINGLE":
#                 changes_lst = getSingleChanges(self,merged_df,changes_lst,key,file_name,data_group,value_field)
#             else:
#                 print("%s: does not exist"%(comparison_type))
#     return changes_lst
    # writeToFile(self.update_directory + 'update.csv', update_lst) # do this in a separate macro
    # print('Done!')

    # I need to filter for individual ids and then compare for what is being added and what is being removed
# def getSingleChanges(self,merged_df,changes_lst,core_df,key,file_name,data_group,value_field):
#     # all values in the merged_df will be NEW entries. There are no CHANGE values and the REMOVE values will be updated next
#     # ['TYPE','GROUP','TABLE','KEY_VALUE','CHANGE_FIELD','VALUE']
#     # type = DROP or ADD
#     drop_df = merged_df[merged_df['_merge'] == "left_only"].drop(columns=['_merge'])
#     add_df = merged_df[merged_df['_merge'] == "right_only"].drop(columns=['_merge'])

#     for i, line in drop_df.iterrows():
#         changes_lst.append(["DROP",data_group,file_name,line[key],value_field,line[value_field]])

#     for i, line in add_df.iterrows():
#         changes_lst.append(["ADD",data_group,file_name,line[key],value_field,line[value_field]])

#     return changes_lst
    # key_lst = merged_df.loc[:,[key]].values.tolist()
    # value_lst = merged_df.loc[:,[value_field]].values.tolist()
    # for i, row in enumerate(key_lst):
    #     changes_lst.append(["NEW",data_group,file_name,key,row[0],value_field,"",value_lst[i][0]])
    # # add the removal keys and values to the change list
    # key_lst = remove_df.loc[:,[key]].values.tolist()
    # value_lst = remove_df.loc[:,[value_field]].values.tolist()
    # for i, row in enumerate(key_lst):
    #     changes_lst.append(["REMOVE",data_group,file_name,key,key_lst[i][0],value_field,value_lst[i][0],""])
    # return changes_lst

    # just need to add a control to no add for fields like WKT
# def getMultipleChanges(self,merged_df,changes_lst,core_df,key,file_name,data_group):
#     merged_df = merged_df[merged_df['_merge'] == "right_only"].drop(columns=['_merge'])
#     merged_index_df = merged_df.loc[:,[key]]
#     core_index_df = core_df.loc[:,[key]]
#     compare_index = merged_index_df.merge(core_index_df,indicator=True,how='outer')
#     # new_index_lst = [x[0] for x in compare_index[compare_index['_merge'] == "left_only"].drop(columns=['_merge']).values.tolist()]
#     change_index_lst = [x[0] for x in compare_index[compare_index['_merge'] == "both"].drop(columns=['_merge']).values.tolist()]
#     # removal_lst = remove_df.values.tolist()
#     headers = core_df.columns
#     # key_index = list(remove_df.columns).index(key)
#     # get the changes
#     for ind in change_index_lst:
#         new_row_lst = merged_df[merged_df[key] == ind].values.tolist()[0]
#         core_row_lst = core_df[core_df[key] == ind].values.tolist()[0]
#         for i, header in enumerate(headers):
#             if new_row_lst[i] != core_row_lst[i]:
#                 changes_lst.append(["DROP",data_group,file_name,ind,header,core_row_lst[i]])
#                 changes_lst.append(["ADD",data_group,file_name,ind,header,new_row_lst[i]])
#                 # changes_lst.append(["CHANGE",data_group,file_name,key,ind,header,core_row_lst[i],new_row_lst[i]]) # need to add "CHANGE"
#                 # ['TYPE','GROUP','TABLE','KEY_VALUE','CHANGE_FIELD','VALUE']
#     # # get the new
#     # for ind in new_index_lst:
#     #     new_row_lst = merged_df[merged_df[key] == ind].values.tolist()[0]
#     #     for i, header in enumerate(headers):
#     #         changes_lst.append(["NEW",data_group,file_name,key,ind,header,"",new_row_lst[i]])
#     # # get the removals. loops through the removal_df that has been filetered for all the keys from the update list in the datagroup folders.
#     # for line in removal_lst:
#     #     for i, header in enumerate(headers):
#     #         changes_lst.append(["REMOVE",data_group,file_name,key,line[key_index],header,line[i],""])
#     return changes_lst
    

# def correctNewForeignKeyValues(self):
#     # this only applies to the occurrence_name and its related OccName table. All other foreign key tables are created equally each time
#     configs = self.configs
#     for file_name in configs:
#         if file_name == 'tenement_holder':
#             update_foreignkey = configs[file_name]['update_foreignkey']
#             if update_foreignkey != "":
#                 field_to_replace = update_foreignkey['field_to_replace']
#                 lookup_field = update_foreignkey['lookup_field']
#                 new_file = "%s%s.csv"%(self.new_directory,file_name)
#                 related_file = "%s%s.csv"%(self.core_directory,update_foreignkey['related_file'])
#                 lookup_file = "%s%s.csv"%(self.core_directory,update_foreignkey['related_file'])
#                 new_df, related_df, lookup_df = [pd.read_csv(file,engine='python') for file in [new_file,related_file,lookup_file]]
#                 columns = related_df.columns
#                 lookup_column = columns.get_loc(lookup_field) # get the index of the lookupfield to get the fields after it to drop
#                 drop_columns = [c for j, c in enumerate(related_df.columns) if j > lookup_column] # get list of fields to drop
#                 related_df.drop(drop_columns,axis=1,inplace=True) # drop fields so the last is the lookup_field
#                 merged_df = pd.merge(new_df,related_df,left_on=field_to_replace,right_on=related_df.columns[0]).iloc[:,[-1]] # merge new and related dfs to get all the foreign key values. The create a df with just the foreign key values
#                 col_df = pd.merge(merged_df,lookup_df,left_on=merged_df.columns[0],right_on=lookup_field) # merge with the lookup df to align it with its true foreign key index from the core file.
#                 new_df.drop(field_to_replace,axis=1,inplace=True) #drop the old foreign key index field
#                 new_df = pd.concat((new_df,col_df),axis='columns') # add true foreign key values
#                 new_df.to_csv(new_file,index=False)
#                 # nothing needs to added to the db here





# def changeHeadersAndLoadToDatabase(db_configs,file_directory,csv_name,table_name,headers_dic):
#     print("Starting to load csv '%s' to model '%s'" %(csv_name,table_name))
#     file_path = file_directory + csv_name
#     engine = sqlalchemy.create_engine('postgresql://%s:%s@%s/%s' %(db_configs['user'], db_configs['password'], db_configs['host'], db_configs['dbname']))
#     con = engine.connect()
#     df = pd.read_csv(file_path)
#     df = df.rename(columns=headers_dic)
#     df.to_sql(table_name, con, if_exists='append', index=False, method='multi')
#     con.close()
#     print('wkts_tenement_occurrence table in shapefiles db!')