import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, date, timedelta
import ctypes
import csv

from .db_update import clear_db_table_rows_in_lst, sqlalchemy_engine, connect_psycopg2, append_df_to_db_table, update_db_table_by_index_field_and_value_lst


def get_path(directory,file_name):
    return os.path.join(directory,'%s.csv'%(file_name)) if file_name else None

# def get_core_path(self,file_name):
#     return os.path.join(self.core_dir,'%s.csv'%(file_name)) if file_name else None

# def get_raw_path(self,file_name):
#     return os.path.join(self.convert_dir, '%s.csv'%(file_name)) if file_name else None

def open_file_as_df(path):
    return pd.read_csv(path,engine='python') if path else None

def insert_date_in_date_fields(dic):
    ''' replace dic value 'date' with todays date. This is required as the date can not be stored in json config file '''
    for x in dic:
        dic[x] = date.today() if dic[x] == 'date' else dic[x]
    return dic

def add_missing_rows_to_id_df(id_df,to_add_df,to_remove_df,configs):
    ''' if it is permitted then add the new values to the id_df so the next time the data is updated this value will have an _id
        return the to_add_df as well as this will be inserted into the db table
    ''' 
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
    table_name = 'gp_%s'%(file_name.lower())
    append_df_to_db_table(con,table_name,df)

def clear_db_rows(conn,file_name,table_index,index_lst):
    table_name = 'gp_%s'%(file_name.lower())
    clear_db_table_rows_in_lst(conn, table_name, table_index, index_lst)


def commit_fields_updated_data(self,field,configs):
    '''
        raw_file: file that contains the raw values and the formatted match
            dataset: the directory it lives in inside the convert folder
            name: name of the raw file that has prefix '_R'
        raw_update: configs to update the raw file
            style: either 'simple' or 'name_to_id'
                simple: gets the original value and manually formatted file and adds it to the raw file
                name_to_id: gets the original value and formatted value and then finds the index value of the formatted value and adds that to the raw file
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

    if reduced_miss_df.empty:
        return
    else:

        config = configs['update_core']
        raw_path = get_path(self.convert_dir,config['raw_file'])
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

        existing_ids_df = reduced_miss_df.merge(id_df,left_on=left_on,right_on=right_on,how='left')
        # filter for only the rows that don't exist in the id_df. These will be added to the id_df and assigned an _id which will be used in the ind_df later
        to_add_df = existing_ids_df[existing_ids_df[index].isnull()][['LIKELY_MATCH']].rename(columns={'LIKELY_MATCH':right_on})
        # single column of values to remove from the id_df. required for holder where the temp unformatted names are removed and the new fromatted names are added
        to_remove_df = existing_ids_df[['ORIGINAL']].rename(columns={'ORIGINAL':right_on}) if remove_temp else pd.DataFrame([],columns=[right_on])
        # add missing rows to the id_df. 
        id_df, to_add_df, remove_id_df = add_missing_rows_to_id_df(id_df,to_add_df,to_remove_df,config['df_build'])


        # match the missing values with their _id value from the id_df
        ind_merge_df = old_to_new_merge_df.merge(id_df,left_on=left_on,right_on=right_on,how='left')[['IND',index]]
        columns = config['db_update']['columns']
        table_index = config['db_update']['index']
        db_add_style = config['db_update']['type']
        # table_index_dtype = config['db_update']['index_dtype']
        update_process = config['db_update']['proccess']
        index_lst = []

        ind_merge_df.columns = columns

        # update the raw_df file if necessary
        raw_update = config['raw_update']
        style = raw_update['style'] if raw_update else ''
        # if style == 'name_to_id':
        #     # used for materials only. matches the newly formatted values with their ids in the raw file
        #     mfields = raw_update['fields']
        #     sfields = [x for x in mfields]
        #     # print(raw_df.head())
        #     # print(id_df.head())
        #     # raw_add_df = existing_ids_df[sfields].rename(columns=mfields)
        #     # raw_df = pd.concat((raw_df,raw_add_df)).drop_duplicates()

        #     # code_name_df = raw_df.merge(id_df,left_on='code',right_on='',how='left')
        #     # reduced_to_id_df = reduced_miss_df.merge(code_name_df,left_on='LIKELY_MATCH',right_on='name',how='inner')
        #     # raw_add_df = reduced_to_id_df[sfields].rename(columns=mfields)
        #     # raw_df = pd.concat((raw_df,raw_add_df)).drop_duplicates()

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
            # if table_index_dtype == "string":
            #     snippet_ind_df[table_index] = snippet_ind_df[table_index].astype(str)
            update_df = ind_merge_df
            ind_merge_df = snippet_ind_df.merge(ind_merge_df,on=table_index).set_index(table_index,drop=False)
            # use this list to delete the necessary rows from the db
            # # index_lst = [str(x) for x in ind_merge_df[table_index].to_list()] # should be able to delete this
            ind_df.set_index(table_index,drop=False,inplace=True)
            ind_df.update(ind_merge_df)
            # # ind index values need to be converted to string so that they match the database values
            # if table_index_dtype == "string":
            #     update_df[table_index] = update_df[table_index].astype(float).astype(str)
            # create dic to update values in the db. I can't delete all the rows as they have relations in other tables
            update_dic = {'index': columns[0], 'field':columns[1], 'lst':update_df.values.tolist()}

        elif db_add_style == 'append':
            # perform an outer merge to remove any rows in ind_merge_df that already exist in the ind_df. There shouldn't be any duplicates, but if they exist and aren't remove it will throw an error when appending to the db
            ind_merge_df = ind_merge_df.merge(ind_df,how='outer',indicator=True).query('_merge == "left_only"').drop('_merge',1)
            # merge additions to the final ind_df
            ind_df = pd.concat((ind_df,ind_merge_df))


        elif db_add_style == 'holder':
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

















# def commit_missing_holder(self,con,conn,manual_update_df,missing_all_df):
#     ''' commit the missing holder values to the related files and database tables.
#         the 'holder_comparisons.csv' file needs to be checked and names filled out in 'LIKELY_MATCH' where required
#     '''
#     print('Updating Holder related csv files and db tables with the manual corrections')

#     con = self.con
#     conn = self.conn
#     manual_update_df = self.manual_update_df
#     missing_all_df = self.missing_all_df

#     holder_path = os.path.join(self.core_dir,'Holder.csv')
#     tenholder_path = os.path.join(self.core_dir,'TenHolder.csv')
#     Companies_R_path = os.path.join(self.convert_dir,'tenement','Companies_R.csv')

#     # add values to Companies_R_df so next time the name will be found
#     holder_comparisons_df = manual_update_df.query('FIELD == "HOLDER"')
#     Companies_R_df = pd.read_csv(Companies_R_path,engine='python')

#     # add new holder lookup values to the Companies_R file so it is available for next download
#     holder_comparisons_trim_df = holder_comparisons_df[['ORIGINAL','LIKELY_MATCH']].rename(columns={'ORIGINAL':'original_name','LIKELY_MATCH':'new_name'})
#     Companies_R_df = pd.concat((Companies_R_df,holder_comparisons_trim_df))
#     # drop duplicates, convert to string first to prevent the dropping of non-duplicates
#     Companies_R_df.astype(str).drop_duplicates(inplace=True)

#     # remove temporary holder names and add correctly formatted names to Holder_df
#     Holder_df = pd.read_csv(holder_path,engine='python')
#     # get the temporary holder names with their ids from the Holder_df
#     temp_name_ids_df = holder_comparisons_trim_df.merge(Holder_df,left_on='original_name',right_on='name',how='left')
#     # check which of the updated missing values exist in the Holder_df. If they exist then we will be able to retrieve their name_id, if not then one will need to be created and added
#     exist_name_ids_df = holder_comparisons_trim_df.merge(Holder_df,left_on='new_name',right_on='name',how='left')

#     # df of the names that don't exist in the Holder_df. these need to be added Holder_df and in return they will get an _id value
#     dont_exist_df = exist_name_ids_df[exist_name_ids_df['_id'].isnull()][['original_name','new_name']]

#     # drop Holder_df rows of temporary names whose formatted name aready exists
#     Holder_df = Holder_df[~Holder_df['name'].isin(exist_name_ids_df['original_name'])]
#     next_id = Holder_df['_id'].max() + 1
#     new_df = dont_exist_df[['new_name']].rename(columns={'new_name':'name'})
#     new_df['_id'] = np.arange(next_id, len(new_df) + next_id)
#     dic = {'typ_id': 14, 'user_name': 'ss', 'valid_relations': True, 'valid_instance': True, 'user_edit': False, 'date_modified': date.today(), 'date_created': date.today()}
#     for i in dic:
#         new_df[i] = dic[i]
#     Holder_df = pd.concat((Holder_df,new_df))
#     # temporary ids matched with the existing and newly created ids
#     old_to_new_ids = temp_name_ids_df.merge(Holder_df,left_on='new_name',right_on='name',how='left',suffixes=('_temp','_real'))[['original_name','_id_temp','_id_real']]

#     # In the TenHolder_df replace the temporary name_ids with the correct ids
#     TenHolder_df = pd.read_csv(tenholder_path)
#     # get the temporary holder names with their ind value
#     missing_holders_df = missing_all_df.query('FIELD == "HOLDER"')[['IND','VALUE']]
#     # create df with the ind value and the temporary and the correct/new _id
#     old_new_with_ind_df = missing_holders_df.merge(old_to_new_ids,left_on='VALUE',right_on='original_name',how='left').drop(columns=['VALUE','original_name'],axis=1)
#     # replace the name_id value for all the temporary ind values with their correct/new name_ids
#     update_snippet_df = old_new_with_ind_df.merge(TenHolder_df,left_on=['IND','_id_temp'],right_on=['tenement_id','name_id'],how='left')[['_id','tenement_id','_id_real','percown']].rename(columns={'_id_real':'name_id'})
#     update_snippet_df.set_index('_id',drop=False,inplace=True)
#     TenHolder_df.set_index('_id',drop=False,inplace=True)
#     # insert and overwrite the updated section into the TenHolder_df
#     TenHolder_df.update(update_snippet_df)

#     # # remove all TenHolder db rows that will be updated
#     # clear_db_table_rows_in_lst(conn, 'gp_tenholder', '_id', update_snippet_df['_id'].to_list())
#     # # remove all temp rows from the Holder db
#     # clear_db_table_rows_in_lst(conn, 'gp_holder', '_id', temp_name_ids_df['_id'].to_list())
#     # # add new_df to gp_Holder in db
#     # append_df_to_db_table(con,'gp_holder',new_df)
#     # # add updated TenHolder snippet to db
#     # append_df_to_db_table(con,'gp_tenholder',update_snippet_df)

#     # # save dfs to csv
#     # Companies_R_df.to_csv(Companies_R_path,index=False)
#     # Holder_df.to_csv(holder_path,index=False)
#     # TenHolder_df.to_csv(tenholder_path,index=False)

    



# def commit_missing_name(self):
#     ''' Add the newly formatted site names to the csv and db tables. 
#         The 'OccName_R' file is not updated as uniqueness is not so crucial
#         There is no need to delete rows from the db as values where the name had no formatted match in the OccName file are not added in the first place.
#     '''
#     con = self.con
#     conn = self.conn
#     manual_update_df = self.manual_update_df
#     missing_all_df = self.missing_all_df

#     OccName_path = os.path.join(self.core_dir,'OccName.csv')
#     occurrence_name_path = os.path.join(self.core_dir,'occurrence_name.csv')
#     OccName_df = pd.read_csv(OccName_path,engine='python')
#     occurrence_name_df = pd.read_csv(occurrence_name_path,engine='python')
#     # get just the 'NAME' missing values
#     missing_names_df = manual_update_df[manual_update_df['FIELD'] == 'NAME']
#     # determine which formatted names exist or not in the OccName table
#     exist_name_ids_df = missing_names_df.merge(OccName_df,left_on='LIKELY_MATCH',right_on='name',how='left')
#     # add the names that don't exist to the OccName table
#     to_add_df = exist_name_ids_df[exist_name_ids_df['_id'].isnull()][['LIKELY_MATCH']].rename(columns={'LIKELY_MATCH':'name'})
#     next_id = OccName_df['_id'].max() + 1
#     to_add_df['_id'] = np.arange(next_id, len(to_add_df) + next_id)
#     dic = {'user_name': 'ss', 'valid_instance': True, 'date_created': date.today()}
#     for i in dic:
#         to_add_df[i] = dic[i]
#     OccName_df = pd.concat((OccName_df,to_add_df))
#     # match the formatted site names with the 'ind' values and their '_id' values so they can be appended to the csv and db tables
#     all_miss_names_df = missing_all_df.query('FIELD == "NAME"')
#     old_to_new_names_df = all_miss_names_df.merge(missing_names_df,left_on='VALUE',right_on='ORIGINAL',how='left')
#     ind_with_ids_df = old_to_new_names_df.merge(OccName_df,left_on='LIKELY_MATCH',right_on='name',how='left')[['IND','_id']].rename(columns={'IND':'occurrence_id','_id':'occname_id'})
#     occurrence_name_df = pd.concat((occurrence_name_df,ind_with_ids_df))

#     # # append new rows to db table
#     # append_df_to_db_table(con,'gp_occname',to_add_df)
#     # append_df_to_db_table(con,'gp_occurrence_name',ind_with_ids_df)
#     # # save dfs to csv
#     # OccName_df.to_csv(OccName_path,index=False)
#     # occurrence_name_df.to_csv(occurrence_name_path,index=False)



# def commit_missing_materials(self):
#     ''' Add the newly formatted material names to the csv and db tables.
#         this can not add newly created material names because the index value is a 2-3 character string. This can only add existing material names. It is also not possible to manually add a name and add it.
#         this would have to all be done manully.
#         If an 'ORIGINAL' material value has more than one 'LIKELY_MATCH' such as 'OIL & GAS SHOW (STRONG)' which would equal 'Petroleum gas - strong' and 'Oil - strong' then
#         just create a new row and enter it in 'manual_update_required' with the same STATE, ORIGINAL, FIELD name and the new value you want to give to it.
#     '''
#     con = self.con
#     conn = self.conn
#     manual_update_df = self.manual_update_df
#     missing_all_df = self.missing_all_df

#     Material_path = os.path.join(self.core_dir,'Material.csv')
#     materials_R_path = os.path.join(self.convert_dir,'occurrence','Materials_R.csv')
#     Material_df = pd.read_csv(Material_path,engine='python')
#     materials_R_df = pd.read_csv(materials_R_path,engine='python')

#     # get just the 'NAME' missing values
#     missing_names_df = manual_update_df[manual_update_df['FIELD'].str.contains('MATERIAL')]
#     old_with_id_df = missing_names_df.merge(Material_df,left_on='LIKELY_MATCH',right_on='name',how='left')[['STATE','FIELD','ORIGINAL','code']]

#     raw_add_df = pd.DataFrame()

#     for mat in ['maj','min']:
#         field = '%sOR_MATERIAL'%(mat.upper())
#         csv_file_name = 'occurrence_%smat.csv'%(mat)
#         db_table_name = 'gp_occurrence_%smat'%(mat)

#         occurrence_mat_path = os.path.join(self.core_dir,csv_file_name)
#         occurrence_mat_df = pd.read_csv(occurrence_mat_path,engine='python')

#         all_miss_mat_df = missing_all_df.query('FIELD == "%s"'%(field))[['STATE','IND','VALUE']]
#         reduced_miss_df = old_with_id_df.query('FIELD == "%s"'%(field)).drop('FIELD',1)

#         ind_code_val_df = all_miss_mat_df.merge(reduced_miss_df,left_on=['STATE','VALUE'],right_on=['STATE','ORIGINAL'],how='left')

#         for_raw_df = ind_code_val_df[['STATE','VALUE','code']]
#         raw_add_df = pd.concat((raw_add_df,for_raw_df))

#         occ_mat_add_df = ind_code_val_df[['IND','code']].rename(columns={'IND':'occurrence_id','code':'material_id'}).drop_duplicates()
#         # use 'outer' merge to find only rows that don't already exist in the main df
#         occ_mat_add_clean_df = occurrence_mat_df.merge(occ_mat_add_df,how='outer',indicator=True).query('_merge == "right_only"').drop('_merge',1)
#         final_occ_mat_df = pd.concat((occurrence_mat_df,occ_mat_add_clean_df))

#         # # append new rows to db table
#         # append_df_to_db_table(con,db_table_name,occ_mat_add_clean_df)
#         # # save update csv file
#         # final_occ_mat_df.to_csv(occurrence_mat_path,index=False)


#     raw_add_df.drop_duplicates(inplace=True)
#     raw_add_df.rename(columns={'STATE':'State','VALUE':'match'},inplace=True)
#     materials_R_df = pd.concat((materials_R_df,raw_add_df))
#     # # save update csv file
#     # materials_R_df.to_csv(materials_R_path,index=False)


# def commit_missing_status(self):
#     ''' update the missing status values for both the title and sites datasets. It is only possible to apply status values that exist in the core file already, a new
#         status will need to be created manually
#     '''
#     csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

#     con = self.con
#     conn = self.conn
#     manual_update_df = self.manual_update_df
#     missing_all_df = self.missing_all_df

#     lst = [
#         {'dataset':'tenement','raw_file':'Status_R.csv','status_file':'TenStatus.csv','db_table':'gp_tenstatus','field':'TITLE_STATUS'},
#         {'dataset':'occurrence','raw_file':'OccStatus_R.csv','status_file':'OccStatus.csv','db_table':'gp_occstatus','field':'SITE_STATUS'}
#     ]

#     for group in lst:

#         R_path = os.path.join(self.convert_dir,group['dataset'],group['raw_file'])
#         dataset_path = os.path.join(self.core_dir,'%s.csv'%(group['dataset'].capitalize()))
#         status_path = os.path.join(self.core_dir,group['status_file'])
#         R_df = pd.read_csv(R_path,engine='python')
#         dataset_df = pd.read_csv(dataset_path,engine='python')
#         status_df = pd.read_csv(status_path,engine='python')

#         missing_status_df = manual_update_df[manual_update_df['FIELD'] == group['field']]

#         raw_add_df = missing_status_df[['STATE','ORIGINAL','LIKELY_MATCH']].rename(columns={'STATE':'State','ORIGINAL':'Found','LIKELY_MATCH':'Original'})
#         final_raw_df = pd.concat((R_df,raw_add_df))

#         all_miss_status_df = missing_all_df.query('FIELD == "%s"'%(group['field']))
#         old_new_df = all_miss_status_df.merge(final_raw_df,left_on='VALUE',right_on='Found',how='left')[['IND','Original']]
#         ind_id_df = old_new_df.merge(status_df,left_on='Original',right_on='original',how='left')[['IND','_id']].rename(columns={'IND':'ind','_id':'status_id'})

#         dataset_snippet_df = dataset_df[dataset_df['ind'].isin(ind_id_df['ind'])].drop('status_id',1)
#         new_dataset_status_df = dataset_snippet_df.merge(ind_id_df,on='ind').set_index('ind',drop=False)
#         index_lst = new_dataset_status_df['ind'].to_list()
#         dataset_df.set_index('ind',drop=False,inplace=True)
#         dataset_df.update(new_dataset_status_df)

#         # # append new rows to db table
#         # append_df_to_db_table(con,group['db_table'],raw_add_df)
#         # # drop rows from db table and append updated rows
#         # dataset_db_table = 'gp_%s'%(group['dataset'])
#         # clear_db_table_rows_in_lst(conn, dataset_db_table, 'ind', index_lst)
#         # append_df_to_db_table(con,dataset_db_table,new_dataset_status_df)

#         # # save updated csv files
#         # final_raw_df.to_csv(R_path,index=False)
#         # dataset_df.to_csv(dataset_path,index=False)
