import geopandas as gpd
from shapely import wkt
import sqlalchemy
import pandas as pd
import numpy as np
import os
import psycopg2
from datetime import datetime, date, timedelta
import ctypes
import csv
import sys

from functions.common.directory_files import copy_directory, get_json, file_exist, delete_file, copy_directory_in_list
from functions.common.db_functions import sqlalchemy_engine, connect_psycopg2, orderTables, clearDatabaseTable
from functions.common.backup import complete_script__restore

from functions.common.timer import Timer
from ..setup import SetUp, Logger
from .migrate_files_db import DatabaseManagement



class ChangesAndUpdate:

    def __init__(self):
        # directories
        self.core_dir = os.path.join(SetUp.output_dir,'core')
        self.ss_dir = os.path.join(SetUp.output_dir,'ss')
        self.new_dir = os.path.join(SetUp.output_dir,'new')
        self.onew_dir = os.path.join(SetUp.output_dir,'onew')
        self.update_dir = os.path.join(SetUp.output_dir,'update')
        self.change_dir = os.path.join(SetUp.output_dir,'change')
        # paths
        self.updates_path = os.path.join(self.update_dir,"update.csv")
        self.changes_path = os.path.join(self.update_dir,"change.csv")
        self.core_updates_path = os.path.join(self.core_dir,'update.csv')
        self.core_changes_path = os.path.join(self.core_dir,'change.csv')
        # configs
        self.update_configs = get_json(os.path.join(SetUp.configs_dir,'db_update_configs.json'))
        self.access_configs = get_json(os.path.join(SetUp.configs_dir,'db_access_configs.json'))



    def find_changes_update_core_and_database(self):
        ''' If the Tenement.csv file doesn't exist in the core folder then isUpdate is False. This will clear all the rows in each table in the database and 
            reload it with a new batch of data, generally used to insert the initial data.
            if isUpdate is True then only the changes need to be added to the database.
        '''        
        timer = Timer()
        Logger.logger.info(f"\n\n{Logger.hashed}\nRecord Changes & Migrate to Database\n{Logger.hashed}")

        # get congif files
        # If less than three files, then all new files will be pushed to db
        if not SetUp.isUpdate:
            ''' Copy the files from the new folder to the core & change folder. Then copy the new files to the core folder
                and add the user_input, valid & modified columns. Copy these files to the us_change folder which will then be loaded into the database.
                lastly, create an empty change.csv file in the change folder.
            '''
            Logger.logger.info(f"\n{Logger.dashed} Initial run. Migrating all data to core directory & database {Logger.dashed}")
            # delete files from core that should exist yet. these include change & update files
            self.delete_unrequired_core_files()
            # copy relevant files from the new folder to the core and ss folders
            self.copy_new_files_to_core()
            # create qgis compatible files for tenement & occurrence files
            self.create_qgis_spatial_files()
            # delete all content in tables and copy all files to the db
            self.commit_all_files_to_db() 
            # create empty change file. This will tell the script to update rather than renew everything the next time it is run.
            self.create_empty_change_file()
        else:
            print('#'*100)
            ''' This deals with updating the core and database tables on subsequent downloads. The core files are maintained rather than replaced and the relevant rows of each file/table
                that requires updates are updated, while new rows are added. All changes are recorded and this is also updated in the core files and the database.
            '''
            Logger.logger.info(f"\n{Logger.dashed} Update run. Migrating new & updated data to the core directory & database {Logger.dashed}")
            try:
                # add core data to new csv file for Tenement values that were added in the tenement_occurrence relation step
                self.add_relation_core_rows_to_new_file()
                # copy new files that will be updated with user edits to a separate folder. This will only be used to compare the new file updated with the user edits and the original
                self.backup_new_useredit_file()
                # Compare all the files that don't have changes recorded. Add new rows to db.
                ''' adds values to OccTenOriginalID tables. this needs to be combined with the other db migration methods '''
                self.compare_base_tables_add_new() 
                # makes the changes to the ss files, then update the new files with the user changes from the Change tables/files
                self.update_ss_files_apply_user_edits_to_new_files()
                # builds the files (update and change) that record the additions, removals and changes made for the relevant ids
                self.build_update_and_change_files()
                # create qgis compatible files for tenement & occurrence files
                self.create_qgis_spatial_files()
            except:
                complete_script__restore()
                raise

            try:
                # delete the rows from the change, addition and remove files for both data_groups. This will prevent an error in the next step when their foreign keys may be deleted.
                #   these rows will be added again later.
                self.delete_updating_rows_from_updating_db_tables()
                # makes the changes to the core file and the db
                self.make_core_file_and_db_changes() 
                # create the change, add and remove tables and update them in the core files and database 
                self.build_update_tables_update_db()
            except:
                complete_script__restore()
                raise

        complete_script__restore()

        Logger.logger.info('Changes & Updates duration: %s' %(timer.time_past()))


    def delete_unrequired_core_files(self):
        ''' delete all the files in the core folder that are not required for the initial run. These files may exist if the core folder 
            was copied from past runs which have created them. Files such as the TenementChange/Addition are only created on the second
            and maintained after when there are changes to be recorded.
            ??? the 'update.csv' and 'change.csv' files are not deleted but this could possible create a problem later 
        '''
        # list of files to delete if they exist in the core file
        files = [f'{x}.csv' for x in self.update_configs if self.update_configs[x]['delete_on_initial_run']]
        for file in files:
            path = os.path.join(self.core_dir,f'{file}.csv')
            delete_file(path)


    def commit_all_files_to_db(self):
        # clear database and load tables in change directory
        DatabaseManagement().clear_db_tables_and_remigrate(src_dir=self.change_dir)

    

    def create_empty_change_file(self):
        ''' create empty update file only on the first download so the next download knows it is an update only '''
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


        for data_group in SetUp.data_groups:
            # Find the change ids and look for differences between the core and new. I will need to loop through all the related files. 
            # If there are changes, then add the id to the update file and change file
            datagroup_update_df = pd.read_csv(os.path.join(SetUp.input_dir,data_group,'update','update.csv'))
            # Gets the df of the ids to be removed.
            datagroup_remove_df = datagroup_update_df[datagroup_update_df["ACTION"] == "REMOVE"][["NEW_ID"]]
            date
            # list of files to record the changes in the change file for
            file_lst = [x for x in update_configs if update_configs[x]["record_changes"] and update_configs[x]["record_changes"]["data_group"] == data_group and update_configs[x]["record_changes"]["track_changes"]]

            # loop through db update configs and only act on tables/files that are associated with the data_group i.e. have an ind field.
            for file in file_lst:
                record_changes = update_configs[file]["record_changes"]
                # if record_changes != None and record_changes["data_group"] == data_group:
                # set path of the file in the output/new directory
                new_path = os.path.join(self.new_dir,"%s.csv"%(file))
                # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
                if file_exist(new_path):
                    Logger.logger.info(f"Build update and change files for {file}")
                    # set the key (index) column and the fields to drop from the db_update_configs file
                    key = record_changes["key"]
                    drop_fields = record_changes["drop_fields"]
                    # get the core file and filter for the change ids
                    core_df = pd.read_csv(os.path.join(self.core_dir,"%s.csv"%(file)))
                    new_df = pd.read_csv(new_path)

                    # if empty then there is nothing to add
                    if not new_df.empty:
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
        self.add_id_date_fields_and_write_files(final_changes_df,self.changes_path,self.core_changes_path)
        self.add_id_date_fields_and_write_files(final_updates_df,self.updates_path,self.core_updates_path)



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
        df["DATE"] = SetUp.pyDate
        # concat to core file
        if file_exist(core_file_path):
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





    def update_ss_files_apply_user_edits_to_new_files(self):
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
        # Configs
        update_configs = self.update_configs

        # list of all files that have their changes tracked
        all_file_lst = [x for x in update_configs if update_configs[x]['record_changes'] and update_configs[x]['record_changes']['user_edits']]

        # loop through the datagroups i.e. occurrence and tenements
        for data_group in SetUp.data_groups:
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
                    # print("Working on: %s, Field: %s"%(file,field))
                    Logger.logger.info(f"Making ss file and db changes for field '{field}' in table '{file}'")
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

                                # # # # df of only the new values that don't exist in the core file
                                # # # new_only_df = new_df[~new_df[key].isin(t_core_df[key])]
                                # # # # concat new values with the core values
                                # # # concat_df = pd.concat((t_core_df,new_only_df))
                                # # # # update the new df
                                # # # new_df = pd.merge(new_df,concat_df,on=key,suffixes=("", "_x")).drop(field, 1).rename(columns={"%s_x"%(field): field})
                        
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
                        # can't merge with an empty df
                        if not sfinal_df.empty:
                            new_df = sfinal_df.merge(user_drop_df,how='left').drop_duplicates()
                        else:
                            new_df = sfinal_df
                        # # new_df.drop_duplicates(ignore_index=True) # this converts the integer field to float and replaced a non-duplicate value to Nan

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
                        sfinal_df = pd.concat((ah_new_df,add_df,user_add_df,final_common_df,ss_update_df)).drop_duplicates()
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
                inactive_ids: only applies to Occurrence & Tenement files. holds the configs on how to deal with entries that have been dropped from the datasets
        '''
        # Configs
        update_configs = self.update_configs
        access_configs = self.access_configs
        # db connections
        sqlalchemy_con = sqlalchemy_engine(access_configs[SetUp.active_atlas_directory_name]).connect()
        psycopg2_con = connect_psycopg2(access_configs[SetUp.active_atlas_directory_name])

        # this needs to be reversed so the 'tenement_occurrence' table is entered after the 'Tenement' table
        for data_group in SetUp.data_groups[::-1]:
            Logger.logger.info(f"\n{Logger.dashed} {data_group} {Logger.dashed}")
            self.data_group = data_group
            # initialise dictionary to hold the change file dfs
            change_dic = {}

            # create a list of the add, change & remove values from the updates.csv and changes.csv files.
            remove_keys_lst, add_keys_lst, change_keys_lst = self.getRemoveAddChangeIdLists()

            # gets the list of files that are part of the current data_group and have their changes recorded or are tables that hold these recorded changes.
            file_lst = get_file_lst(update_configs,data_group)

            #orders the tables so there are no conflicts when entering into or removing from the database
            ordered_tables, temp_lst = orderTables(update_configs,file_lst,[],[]) 
            # I only want the tables that are in file_lst
            ordered_file_lst = [x for x in ordered_tables if x in file_lst]

            for file in ordered_file_lst[::-1]:
                # set path of the file in the output/new directory
                new_path = os.path.join(self.new_dir,"%s.csv"%(file))
                core_path = os.path.join(self.core_dir,"%s.csv"%(file))

                # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
                if not file_exist(new_path):
                    Logger.logger.info(f"file '{file}' does not exist in the new directory")
                    continue

                Logger.logger.info(f"Creating change file & updating core file for '{file}'")

                # set the paths
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
                    clear_db_table_rows_in_lst(psycopg2_con, table, field, db_remove_lst) 

                # Place change_file_df into a dictionary. Tables need to be appended in the db in order. Therefore, they need to be saved in a dictionary and added in the next step
                change_dic[file] = to_add_df


            # write dfs to db in order
            for file in ordered_file_lst:
                Logger.logger.info(f"Appending data to database for table '{file}'")
                table = "gp_%s"%(file.lower())
                df = change_dic[file]

                # rows to add to the tenement_occurrence tables comes from both related tables, these need to be merged and entered as one to prevent the situation where a new occurrence is without it 
                #   having being entered in to the Occurrence table first 
                if file == 'tenement_occurrence' and data_group == 'tenement':
                    ten_occ_df = df
                    continue
                if file == 'tenement_occurrence' and data_group == 'occurrence':
                    df = ten_occ_df.merge(df,how='outer')

                # load rows into the database
                df.to_sql(table, sqlalchemy_con, if_exists='append', index=False, method='multi') 




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



    def compare_base_tables_add_new(self):
        ''' This compares the new files, produced from the vba macros, to the core files. It filters out the ids from the core_df that 
            exist in the new_df and then concatenates the two df's together.
            comparing 'Holder' will always return 0 as new names are added to the core file before running the vba macro. The new names are added 
            to the db in the previous step.
            Base tables are those that are not related to either the Occurrence or Tenement tables. They are tables like Listed, HolderType, OccType etc that when
            updated don't require the Tenement or Occurrence to be updated first to prevent a non existing id error.
            Also excludes the Change, Addition & Removal tables
        '''
        # print("Finding new entries for the base tables and updating the core file and database.")
        Logger.logger.info(f"\n{Logger.dashed} Find new entries {Logger.dashed}")
        
        # Configs
        update_configs = self.update_configs
        access_configs = self.access_configs

        # db connections
        sqlalchemy_con = sqlalchemy_engine(access_configs[SetUp.active_atlas_directory_name]).connect()

        file_lst = [x for x in update_configs if update_configs[x]["is_base_table"]]
        
        for file in file_lst:
            # set path of the file in the output/new directory
            new_path = os.path.join(self.new_dir,"%s.csv"%(file))
            # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
            if not file_exist(new_path):
                Logger.logger.info(f"'{file}' doesn't exist. No new entires could be found")
                continue

            Logger.logger.info(f"Searching for new entries in '{file}'")
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
                # This use to renumber index fields with '_id' name, but I havve since changed all index fields to '_id' so this no longer works
                if not file in ['OccOriginalID','TenOriginalID']:
                # if "_id" in new_ids_df.columns:
                    next_id = core_df["_id"].max() + 1
                    new_ids_df["_id"] = np.arange(next_id, len(new_ids_df) + next_id)

                # concat new rows to the core_df & save
                new_core_df = pd.concat((core_df,new_ids_df))
                new_core_df.to_csv(core_path,index=False)

                # append new rows to the database table
                table = "gp_%s"%(file.lower())

                new_ids_df.to_sql(table, sqlalchemy_con, if_exists='append', index=False, method='multi')

        sqlalchemy_con.close()




    # builds the change file for the required datagroup, updates its equivalent core file and adds it to the database
    def build_group_change_file(self):
        ''' This builds the TenementChange & OccurrenceChange dataframes which are saved in the 'update' directory, appended to the core file and 
            appended to the database table. It loops through the rows in 'change.csv' that was created earlier and separates the fields into their 
            own column so when they are loaded into the database they are treated as foreign keys to their respective models.
            When loading the df to the database it is split by field and loaded separately so the foreign key fields can be converted to integers first. Without splitting
            fields pandas will not allow the conversion of a mixture of Nonetypes/Nans and strings to integers and therefore will throw an error when appending to the db table. 
        '''
        # print("Creating the database ready change file for data group %s"%(self.data_group))
        Logger.logger.info(f"\n{Logger.dashed} Build {self.data_group} Change record tables {Logger.dashed}")

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
        final_df = self.export_as_csv_to_update_and_core(df,self.group_change_path,self.group_change_core_path,file_name)
        # append to the database table
        self.append_to_db_by_field(final_df,file_name,config)


    def append_to_db_by_field(self,df,file_name,config):
        ''' Split the 'Change' df by field and convert the relevant fields to integer type and push to the database '''
        table = "gp_%s"%(file_name.lower())
        # list of the fields that need to be converted to integer type before being appended to the database
        int_fields = self.update_configs[file_name]['int_types']
        # list of change fields that exist. There are the model fields, so if they are a foreign key then they are missing the '_id' suffix
        field_lst = df['field'].drop_duplicates().values.tolist()
        # loop through the fields and append to the db. convert the interger fields to integer type. The df needs to be split and entered one change group at a time so the nulls can be included.
        #   trying convert a columns dtype that includes nulls to integer will throw an error
        for group in field_lst:
            # filter for only the current field type
            select_df = df[df['field'] == group].copy()
            # add '_id' suffix so the model field is equal to the raw db field
            db_field = group + '_id'
            # convert relevant fields to integer type
            if db_field in int_fields:
                select_df[db_field] = select_df[db_field].astype(float).astype(int)
            select_df.to_sql(table, self.sqlalchemy_con, if_exists='append', index=False, method='multi')
        Logger.logger.info("Successfully appended '%s' rows to '%s'"%(len(df.index),table))



    # build the addition and removal database ready files
    def build_group_addition_remove_files(self):
        ''' The 'update.csv' created earlier has all the Tenement & Occurrence ind values that were either added or removed from the state source data.
            This data is then split between datagroup and whether it was added or deleted and then added to the core file and the db table.
        '''
        data_group = self.data_group
        Logger.logger.info(f"\n{Logger.dashed} Create database ready addition & removal files for {data_group} {Logger.dashed}")

        # filter the df for the current data group
        add_remove_df = self.update_df[self.update_df["GROUP"] == data_group]
        # removal is only required in Tenement dataset, No occurrences are by states dataset changes
        action_type_lst = [["ADD","Addition"]]
        if data_group == 'tenement':
            action_type_lst.append(["REMOVE","Removal"])
        # 2 types of actions for indexes that have been removed and those that have been added. the second value is suffix of the model name.
        for action in action_type_lst:
            df = add_remove_df[add_remove_df["TYPE"] == action[0]][["KEY_VALUE","DATE"]]
            df["_ID"] = np.arange(1, len(df) + 1)
            df.rename(columns={"_ID": "_id", "KEY_VALUE": "ind_id", "DATE": "date"},inplace=True)
            # set the paths
            file_name = "%s%s"%(data_group.capitalize(),action[1])
            self.group_action_path = os.path.join(self.update_dir,"%s.csv"%(file_name))
            self.group_action_core_path = os.path.join(self.core_dir,"%s.csv"%(file_name))
            # if the core file exists then concat latest df to it, otherwise export the new df to the update and core directories.
            final_df = self.export_as_csv_to_update_and_core(df,self.group_action_path,self.group_action_core_path,file_name)
            # format date column and append to database
            # final_df = format_date_columns(final_df) # delete this
            table = "gp_%s"%(file_name.lower())
            final_df.to_sql(table, self.sqlalchemy_con, if_exists='append', index=False, method='multi')
            Logger.logger.info("Successfully added '%s' rows to '%s'"%(len(final_df.index),table))
        


    # if the core file exists then concat latest df to it, otherwise export the new df to the update and core directories.
    def export_as_csv_to_update_and_core(self,df,update_dir,core_dir,file_name):

        # create update file and add entries to the core file
        if file_exist(core_dir):
            # read the core equivalent file
            core_df = pd.read_csv(core_dir)

            # correct the ids so they continue on from the max of the core file
            next_id = core_df["_id"].max() + 1 if len(core_df.index) > 0 else 1
            df["_id"] = np.arange(next_id, len(df) + next_id)

            # This part will get the core rows that were deleted before updating the other tables. This needs to be re-added to the database. 
            # create a list of the add, change & remove values from the updates.csv and changes.csv files.
            remove_keys_lst, add_keys_lst, change_keys_lst = self.getRemoveAddChangeIdLists()
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
        Logger.logger.info(f"\n{Logger.dashed} Delete all change & remove id rows from database {Logger.dashed}")

        # Configs
        update_configs = self.update_configs
        access_configs = self.access_configs
        # db connections
        psycopg2_con = connect_psycopg2(access_configs[SetUp.active_atlas_directory_name])

        # loop through the datagroups i.e. occurrence and tenements
        for data_group in SetUp.data_groups:
        # for data_group in ['tenement']:
            self.data_group = data_group
            # create a list of the add, change & remove values from the updates.csv and changes.csv files.
            remove_keys_lst, add_keys_lst, change_keys_lst = self.getRemoveAddChangeIdLists()

            keys_to_remove_lst = stringifyIntLst(remove_keys_lst + change_keys_lst)

            # list of tables/files that contain the updating tables data for the currect data_group
            file_lst = [ x for x in update_configs if update_configs[x]["update_table"] and update_configs[x]["update_table"]["data_group"] == data_group]

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
        self.sqlalchemy_con = sqlalchemy_engine(self.access_configs[SetUp.active_atlas_directory_name]).connect()
        
        for data_group in SetUp.data_groups:
            self.data_group = data_group
            # builds the change file for the required datagroup, updates its equivalent core file and adds it to the database
            self.build_group_change_file()
            # build the addition and removal database ready files
            self.build_group_addition_remove_files()

        self.sqlalchemy_con.close()



    def copy_new_files_to_core(self):
        ''' Copy entire files from the new directory to the core directory when in the db_update_configs file_type = update or replace.
            This is only used on the initial creation on of tables, after, the core files are update with changes only.
        '''
        Logger.logger.info("Copy required files from new to core, change & ss directories")
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
        ''' This is designed to copy the new files to the onew folder before user edits are applied. This will provide a set of data to compare to the post user edit new data '''
        Logger.logger.info(f"\n{Logger.dashed} Copy required files from new to onew directory before user edits are applied {Logger.dashed}")
        update_configs = self.update_configs
        # files to copy from new directory to core directory
        file_lst = ["%s.csv"%(x) for x in update_configs if update_configs[x]['record_changes'] and update_configs[x]['record_changes']['user_edits']]
        # copy files from the new directory to the onew directory. 
        copy_directory_in_list(file_lst,self.new_dir,self.onew_dir)







    def add_relation_core_rows_to_new_file(self):
        ''' When the relation files are created on update, ind values are added to the change file in the 'update' directory, but the data for these ind values are not
            in the 'new' files. So later on, these rows are removed from the database but are not reinstated again as it does not exist in the 'new' file. 
            This method finds these 'ind' values, loops through all relevant files and adds all the rows of data with the ind value.
        '''
        Logger.logger.info(f"\n{Logger.dashed} Add spatial additions to new files {Logger.dashed}")
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
        file_lst = [x for x in update_configs if update_configs[x]['record_changes'] and update_configs[x]['record_changes']['relation_update']]

        for file in file_lst:
            Logger.logger.info(f"Working on: {file}")

            new_path = os.path.join(self.new_dir,'%s.csv'%(file))
            new_df = pd.read_csv(new_path)
            core_df = pd.read_csv(os.path.join(self.core_dir,'%s.csv'%(file)))

            key = update_configs[file]['record_changes']['key']

            filtered_core_df = core_df[core_df[key].isin(ten_lst)]
            final_new_df = pd.concat((new_df,filtered_core_df))

            final_new_df.to_csv(new_path,index=False)



    def create_qgis_spatial_files(self):
        ''' create the qgis compatible files for the tenement & occurrence files '''
        Logger.logger.info(f"Creating qgis compatible files")

        for directory in [self.core_dir,self.new_dir]:
            for file in ['Occurrence','Tenement']:
                df = pd.read_csv(os.path.join(directory,'%s.csv'%(file)))
                df['geom'] = df['geom'].apply(lambda x: x.replace("SRID=4202;",""))
                df.rename(columns={'geom': 'WKT'}, inplace=True)
                df.to_csv(os.path.join(directory,'qgis_%s.csv'%(file)),index=False)














# def sqlalchemy_engine(db_configs):
#     return sqlalchemy.create_engine('postgresql://%s:%s@%s/%s' %(db_configs['user'], db_configs['password'], db_configs['host'], db_configs['dbname']))

# def connect_psycopg2(db_keys):
#     return psycopg2.connect("dbname='%s' user='%s' password='%s' host='%s'" %(db_keys['dbname'],db_keys['user'],db_keys['password'],db_keys['host']))


# def clearDatabaseTable(conn, table_name):
#     cur = conn.cursor()
#     cur.execute("DELETE FROM %s"%(table_name))
#     rows_deleted = cur.rowcount
#     conn.commit()
#     Logger.logger.info(f"'{rows_deleted}' rows cleared from '{table_name}'")


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
    Logger.logger.info(f"'{rows_deleted}' rows cleared from '{table}'")


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
    Logger.logger.info("'%s' rows updated for '%s'"%(len(dic['lst']),table_name))




# def orderTables(configs,input_lst,carry_lst,temp_lst):
#     for table in input_lst:
#         temp_lst = []
#         if not table in carry_lst:

#             sub_tables = configs[table]['related_tables']
            
#             if not len(sub_tables) == 0:
#                 carry_lst, temp_lst = orderTables(configs,sub_tables,carry_lst,temp_lst)
#             temp_lst.insert(0,table)

#         carry_lst = carry_lst + [x for x in temp_lst[::-1] if not x in carry_lst ]

#     return carry_lst, temp_lst





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
        if record_changes:
            if record_changes["data_group"] == data_group:
                lst.append(file)
            elif record_changes["second_data_group"] and record_changes["second_data_group"]["data_group"] == data_group:
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




def stringifyIntLst(lst):
    ''' get the ids that will be deleted from the db table. convert to int to remove the decimal and then to string as that is how it is stored in the db '''
    return [str(int(x)) for x in lst]


def drop_id(val):
    if val[len(val)-3:] == '_id':
        return val[:-3]
    else:
        return val


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

