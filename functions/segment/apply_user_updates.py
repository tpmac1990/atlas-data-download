import time
import os
import ctypes
import geopandas as gpd
import pandas as pd
import numpy as np
import csv

from functions.common.directory_files import get_json, file_exist
from functions.common.backup import complete_script__restore
# from .db_update import sqlalchemy_engine
from functions.common.db_functions import sqlalchemy_engine
from functions.common.timer import Timer

from ..setup import SetUp
from .data_download import geoDfToDf_wkt
from functions.logging.logger import logger



class ExtractUserEdits:
    ''' This extracts the user edits made to the data and applies them to the core dataset so the core dataset is a 
        clone of database. This is required before running either of the two tasks; the manual update or the state data source
        update.
    '''

    def __init__(self):
        # configs
        self.update_configs = get_json(os.path.join(SetUp.configs_dir,'db_update_configs.json'))
        self.access_configs = get_json(os.path.join(SetUp.configs_dir,'db_access_configs.json'))
        # directories
        self.core_dir = os.path.join(SetUp.output_dir,'core')
        self.edit_dir = os.path.join(SetUp.output_dir,'edit')
        # db connections
        self.sqlalchemy_con = sqlalchemy_engine(self.access_configs[SetUp.active_atlas_directory_name]).connect()

        

    def extract_user_edits(self):
        timer = Timer()
        logger(message="Extract User Edits", category=1)
        # only relevant if updating
        if not SetUp.isUpdate:
            logger(message="This is the initial download. No User edits to extract", category=4)
            return

        try:
            # update the core files for db tables which the user can create new instances e.g. Holder, OccName
            self.transfer_user_creations_to_core()
            # compare the three main group files and their related files and update the core files so they mimic the db
            self.transfer_user_edits_to_core()
            # add user changes from the db TenementChange & OccurrenceChange tables to their core equivalents
            self.transfer_changes_to_core()
        except:
            # revert files back to last backup stage
            complete_script__restore()
            raise

        self.sqlalchemy_con.close()
        logger(message="Total user edits extraction run time: %s" %(timer.time_past()), category=4)



    def transfer_user_creations_to_core(self):
        ''' Copy the user created instances from db tables and add to the core file '''
        logger(message="Copy user created instances to their core file", category=1)
        for table in [OCCNAME, OCCORIGINALID, TENORIGINALID, LISTED, HOLDER]:
            _transfer_user_creations_to_core_table(self, table)
            
            
    def _transfer_user_creations_to_core_table(self, table):
        """ 
        collect all user edits from the database table and add them to the edit and core file.
        A newly created value should never have a duplicate id to another as _id are assigned 
        in the front-end with an id using max() + 1.
        Duplicate _ids will occurr if something has been altered e.g. date_created or valid_instance.
        If there is a duplicate, the value from the core table in the database will overwrite the
        value in the core and edit file.
        """
        core_dir = self.core_dir 
        edit_dir = self.edit_dir
        sqlalchemy_con = self.sqlalchemy_con
        
        logger(message=f"Updating table '{table}'", category=4)
        core_path = os.path.join(core_dir,"%s.csv"%(table))
        edit_path = os.path.join(edit_dir,"%s.csv"%(table))
        core_df = pd.read_csv(core_path,engine='python')
        temp_df = core_df.copy()
        last_date = temp_df[['date_created']].max()
        f_date = last_date[0]
        # Get the latest rows entered by users by filtering from the latest date in the core file and where user_name is 'user'
        sql = "SELECT * FROM gp_%s WHERE user_name = 'user' AND CAST(date_created as date) >= '%s'"%(table.lower(),f_date)
        # convert the blanks to Nan
        df = pd.read_sql(sql, sqlalchemy_con).fillna(value=np.nan)
        # no need to continue if there are no new rows from the db
        if len(df.index) > 0:
            # concat user edits to the edit file. create new file if it doesn't exist 
            if file_exist(edit_path):
                edit_df = pd.read_csv(edit_path,engine='python')
                # see doc string on why we drop by _id
                final_edit = pd.concat((edit_df,df)).drop_duplicates(subset=['_id'], keep='last')
            else:
                final_edit = df
            final_edit.to_csv(edit_path,index=False)
            # concat the new rows from the db table to the core file and drop the duplicates
            final_core_df = pd.concat((core_df,df)).drop_duplicates(subset=['_id'], keep='last')
            # overwrite the core file with the updates
            final_core_df.to_csv(core_path,index=False)
            


    def transfer_user_edits_to_core(self):
        ''' This function loops over the three groups; Holder, Tenement & Occurrence. It filters for rows where 'user_edit=True' which means that a user 
            has made an edit to the group file or one of its related files. If there are rows returned, then two steps follow; 
                1. This group file is updated by filtering out the edited rows and replacing with their updated rows from the db table and saved as the core file.
                2. Each of the related tables are looped and the edited ids are filtered in both the db and core files. These df's are then compared and 
                    if there are differences then the changes are applied to the core file and saved
            There are no recording of changes in the 'Change' file here as this is done in the application when a change is made. These tables changes are copied over in 
            the next step.
        '''
        logger(message="Updating core files with changes in db", category=2)
        csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

        update_configs = self.update_configs 
        # access_configs = self.access_configs 
        core_dir = self.core_dir 
        edit_dir = self.edit_dir
        sqlalchemy_con = self.sqlalchemy_con

        # this will create a list with 'Holder','Occurrence','Tenement'
        creation_tables = [x for x in update_configs if update_configs[x]['db_to_core_transfer'] != None]

        for group in creation_tables:
            logger(message=f"Working on Group {group}", category=4)

            dic = update_configs[group]['db_to_core_transfer']
            pk = dic['pk']
            related_field = dic['related_field']
            is_geospatial = dic['is_geospatial']
            table_lst = dic['table_lst']

            # if there are no user edit rows then get all user edits from the db, otherwise find the date of the last user_edit from the core file 
            # and get the new user edits from this date
            core_path = os.path.join(core_dir,"%s.csv"%(group))
            core_df = pd.read_csv(core_path,engine='python')
            user_edit_df = core_df[core_df['user_edit'] == True]

            if user_edit_df.empty:
                sql = "SELECT * FROM gp_%s WHERE user_edit = True"%(group.lower())
            else:
                # if values already exist in the edit file, then get only values from the db from the latest 'date_modified'. No need getting all values
                latest_date = user_edit_df['date_modified'].max()
                sql = "SELECT * FROM gp_%s WHERE user_edit = True AND date_modified >= '%s'"%(group.lower(), latest_date)

            # get the ids of only the latest 'user_edit' values from the db
            group_db_df = gpd.GeoDataFrame.from_postgis(sql, sqlalchemy_con) if is_geospatial else pd.read_sql(sql, sqlalchemy_con)
            # these are the keys of the instances that have had at least one update made by the user in either the group or its related tables
            keys_df = group_db_df[pk].values.tolist()

            # If there are no new rows with user_edits then no need to progress, the core file is already up to date
            if len(keys_df) > 0:

                # filter out these keys from the core_df, they will be replaced with the rows from the db
                reduced_core_df = core_df[~core_df[pk].isin(keys_df)]

                # filter the db data for the necessary ids, format the date columns & add the crs prefix to the geom column
                group_db_df = group_db_df[group_db_df[pk].isin(keys_df)]

                if is_geospatial:
                    group_db_df = geoDfToDf_wkt(group_db_df).drop(columns=['geometry'])
                    group_db_df['geom'] = group_db_df['geom'].apply(lambda x: "{}{}".format('SRID=4202;',x))

                # join the two df's to create the updated df and save
                final_df = pd.concat((reduced_core_df,group_db_df))

                final_df.to_csv(core_path,index=False) # uncomment this

                # update the edit file. this file contains only the user edits
                edit_path = os.path.join(edit_dir,"%s.csv"%(group))
                if file_exist(edit_path):
                    edit_df = pd.read_csv(edit_path,engine='python')
                    final_edit = pd.concat((edit_df,group_db_df)).drop_duplicates(ignore_index=True)
                else:
                    final_edit = group_db_df

                final_edit.to_csv(edit_path,index=False)

                for table in table_lst:
                    logger(message=f"Working on related table: {table}", category=4)
                    # get the values from the core file
                    core_path = os.path.join(core_dir,"%s.csv"%(table))
                    # loop through all the possible fields. 
                    for target_field in related_field:
                        # read the core_df. It needs to be re-read after each loop to include updates from the previous loop
                        core_df = pd.read_csv(core_path)
                        # only take action if the field exists in the df. This is the case for the 'Holder' group only
                        if target_field in core_df.columns:
                            filtered_core_df = core_df[core_df[target_field].isin(keys_df)].copy()
                            # convert ind values to 'str' format. Currently, they are an 'object' in the db_df and 'int64' in the core file which will cause an error on merge
                            convert_columns = [x for x in filtered_core_df.columns if x in ['tenement_id','occurrence_id']]
                            for col in convert_columns:
                                filtered_core_df[col] = filtered_core_df[col].astype(str)

                            # get the values from the db table
                            if len(keys_df) == 1:
                                if is_geospatial:
                                    # I have to cast to text otherwise i get an error even if i pass the key as a string
                                    if type(keys_df[0]) == int:
                                        sql = "SELECT * FROM gp_%s WHERE %s = %s"%(table.lower(),target_field,keys_df[0])
                                    else:
                                        sql = "SELECT * FROM gp_%s WHERE %s = '%s'"%(table.lower(),target_field,keys_df[0])
                                        
                                else:
                                    sql = "SELECT * FROM gp_%s WHERE %s = %s"%(table.lower(),target_field,keys_df[0])
                            else:
                                sql = "SELECT * FROM gp_%s WHERE %s in %s"%(table.lower(),target_field,tuple(keys_df))
                            db_df = pd.read_sql(sql, sqlalchemy_con)

                            # drop the 'id' field of the m2m fields. these are automatically populated
                            if 'id' in db_df.columns: 
                                db_df.drop(columns=['id'],inplace=True)

                            # merge the db & core values to find if there are any differences between the two. Firstly, convert db_df dtypes to that of the core_df
                            db_df = db_df.astype(filtered_core_df.dtypes.to_dict())
                            merge_df = filtered_core_df.merge(db_df,indicator=True,how='outer')
                            diff_df = merge_df[merge_df["_merge"] != 'both']

                            # if the diff_df length is more than 0 then the core file needs to be updated. remove the rows with the filtered ids, concat the updated rows and save.
                            if len(diff_df.index) > 0:
                                reduced_core_df = core_df[~core_df[target_field].isin(keys_df)]
                                final_df = pd.concat((reduced_core_df,db_df))
                                final_df.to_csv(core_path,index=False)
                                # save edits to the edit file. This is only used to view the edits separatly
                                edit_path = os.path.join(edit_dir,"%s.csv"%(table))
                                if file_exist(edit_path):
                                    edit_df = pd.read_csv(edit_path,engine='python')
                                    final_edit = pd.concat((edit_df,db_df)).drop_duplicates(ignore_index=True)
                                else:
                                    final_edit = db_df
                                final_edit.to_csv(edit_path,index=False)



    def transfer_changes_to_core(self):
        ''' Copy the Change tables from the db and concatenate the user additions to the core file '''
        logger(message="Updating the core Change files with the latest db updates", category=2)

        for table in ['OccurrenceChange','TenementChange','HolderChange']:
            logger(message=f"Working on: '{table}'", category=4)
            self._transfer_changes_to_core_table(table)
            
    
    def _transfer_changes_to_core_table(self, table):
        core_dir = self.core_dir 
        sqlalchemy_con = self.sqlalchemy_con
        
        # get the date of the last user entry. This is the date the db search will filter from 
        core_path = os.path.join(core_dir,"%s.csv"%(table))
        # random date in the past
        f_date = '2000-02-01' 
        # empty df incase core_df doesn't exist yet
        core_df = pd.DataFrame() 
        if file_exist(core_path):
            core_df = pd.read_csv(core_path)
            if len(core_df.index) > 0:
                temp_df = core_df.copy()
                last_date = temp_df[['date_created']].max()
                f_date = last_date[0]           

        # Get the latest rows from the Change tables by filtering rows after the last date in the core file. I was not able to query for user = 'user', so i used pandas as below
        sql = "SELECT * FROM gp_%s WHERE CAST(date_created as date) >= '%s'"%(table.lower(),f_date)
        # drop id column and convert all None to Nan
        df = pd.read_sql(sql, sqlalchemy_con).fillna(value=np.nan)
        df = df[df['user'] != 'ss']
        # convert all types to string so there are no differences between types when dropping duplicates, then replace 'nan'
        df = df.astype(str).replace('nan', np.nan, regex=True)
        core_df = core_df.astype(str).replace('nan', np.nan, regex=True)
        # if the core_df is empty then create core with the db df, otherwise concat the db table with the core file. If there are duplicates, the first will be kept
        #  and the others deleted
        final_core_df = df if core_df.empty else pd.concat((core_df,df)).drop_duplicates(ignore_index=True)
        # only create file if the df is not empty. An empty df might cause issues later when trying to find the max _id value that would't exist
        if not final_core_df.empty:
            final_core_df.to_csv(core_path,index=False)
                
