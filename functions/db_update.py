from .directory_files import copyDirectory, getJSON, fileExist
from .preformat import singleColumnDfToList

import sqlalchemy
import pandas as pd
import numpy as np
import os
import psycopg2


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


def clearDbTableRowsInLst(conn, table, field, lst):
    cur = conn.cursor()
    cur.execute("DELETE FROM %s WHERE %s IN %s"%(table,field,tuple(lst)))
    rows_deleted = cur.rowcount
    conn.commit()
    print("%s rows cleared from %s"%(rows_deleted, table))


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
    print('Committing all files to Database.')
    db_keys = self.access_configs['local']
    con = sqlalchemy_engine(db_keys).connect()
    conn = connect_psycopg2(db_keys)
    # print(engine.table_names()) # print all tables in the database

    configs = self.update_configs
    orig_lst = [ table for table in configs ] # get a list of all the database tables

    ordered_tables, temp_lst = orderTables(configs,orig_lst,[],[]) #orders the tables so there are no conflicts when entering into the database
    # print(ordered_tables)

    for table in ordered_tables[::-1]: # delete all data in all tables in order
        table_name = "map_%s"%(table.lower())
        clearDatabaseTable(conn,table_name)

    # for table in ['Occurrence']:
    for table in ordered_tables:        
        print(table)
        path = os.path.join(self.change_dir,"%s.csv"%(table))
        table_name = "map_%s"%(table.lower())

        try:
            df = pd.read_csv(path)
        except:
            df = pd.read_csv(path,engine='python')

        df.rename(columns=self.update_configs[table]['columns'],inplace=True)
        df.to_sql(table_name,con,if_exists='append',index=False, method='multi')

    con.close()
    print('Complete.')


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
    # configs
    update_configs = self.update_configs

    # Create the empty dfs for the change and update data
    final_changes_df = pd.DataFrame(columns=['TYPE','GROUP','TABLE','KEY_VALUE','CHANGE_FIELD','VALUE'])
    final_updates_df = pd.DataFrame(columns=['TYPE','GROUP','KEY_VALUE'])

    # do all the occurrence files, then all the tenement files.
    for data_group in self.data_groups:
        # Find the change ids and look for differences between the core and new. I will need to loop through all the related files. 
        # If there are changes, then add the id to the update file and change file
        datagroup_update_df = pd.read_csv(os.path.join(self.input_dir,data_group,'update/update.csv'))
        # Gets the df of the ids to be removed.
        datagroup_remove_df = datagroup_update_df[datagroup_update_df["ACTION"] == "REMOVE"][["NEW_ID"]]

        # loop through db update configs and only act on tables/files that are associated with the data_group i.e. have and occid/tenid field.
        for file in update_configs:
            # if file in ['tenement_majmat']:
            record_changes = update_configs[file]["record_changes"]
            if record_changes != None and record_changes["data_group"] == data_group:
                # set path of the file in the output/new directory
                new_path = os.path.join(self.new_dir,"%s.csv"%(file))
                # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
                if fileExist(new_path):
                    print(file)
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

                    # add ids are those that exist in the new_df and not the core_df
                    add_ids_df = merge_df[merge_df["_merge"] == "left_only"].drop(columns=["_merge"])
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

                        for field in columns:
                            core_field_df = core_change_df[[key,field]].drop_duplicates()
                            new_field_df = new_change_df[[key,field]].drop_duplicates()
                            merge_field_df = core_field_df.merge(new_field_df,indicator=True,how='outer')
                            remove_df = merge_field_df[merge_field_df["_merge"] == "left_only"].drop(columns="_merge")
                            add_df = merge_field_df[merge_field_df["_merge"] == "right_only"].drop(columns="_merge")
                            for type_group in [[remove_df,"REMOVE"],[add_df,"ADD"]]:
                                df = type_group[0]
                                df.columns = ["KEY_VALUE","VALUE"]
                                df["GROUP"] = data_group
                                df["TYPE"] = type_group[1]
                                df["TABLE"] = file
                                df["CHANGE_FIELD"] = field
                                # concat the df to the maintained final_changes_df 
                                final_changes_df = pd.concat((final_changes_df,df))

                    # Add the ADD updates to the update df
                    final_updates_df = concat_to_update_df(final_updates_df,add_ids_df,"ADD",data_group)
                        
        # Add the REMOVE updates to the update df
        final_updates_df = concat_to_update_df(final_updates_df,datagroup_remove_df,"REMOVE",data_group)

    # add the ID and DATE fields, write and append to core file
    add_id_date_fields_and_write_files(self,final_changes_df,self.changes_path,self.core_changes_path)
    add_id_date_fields_and_write_files(self,final_updates_df,self.updates_path,self.core_updates_path)



def add_id_date_fields_and_write_files(self,df,file_path,core_file_path):
    # drop any duplicates
    df.drop_duplicates(inplace=True)
    # add date field
    df["date"] = self.tDate
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


def getfileLst(configs,data_group):
    lst = []
    for file in configs:
        record_changes = configs[file]["record_changes"]
        if record_changes != None:
            if record_changes["data_group"] == data_group:
                lst.append(file)
            elif record_changes["second_data_group"] != None and record_changes["second_data_group"]["data_group"] == data_group:
                lst.append(file)
    return lst


def make_core_file_and_db_changes(self):
    print("Creating change files, updating core files and updating the database.")
    # Configs
    update_configs = self.update_configs
    access_configs = self.access_configs

    # db connections
    sqlalchemy_con = sqlalchemy_engine(access_configs["local"]).connect()
    psycopg2_con = connect_psycopg2(access_configs["local"])

    # Get the update and change file and put them into dfs
    update_records_df = pd.read_csv(self.updates_path)
    change_records_df = pd.read_csv(self.changes_path)

    # loop through the datagroups i.e. occurrence and tenements
    for data_group in self.data_groups:

        # initialise dictionary to hold the change file dfs
        change_dic = {}

        # get the keys of the update df. These will be used to select the data by keys to delete in the core file and select the data by keys to copy from the new file.
        # There is no need to seaparate the core, add and change. the remove keys won't exist in the new_df and the add keys won't exist in the core df.
        update_keys_df = update_records_df[update_records_df["GROUP"] == data_group]["KEY_VALUE"]

        # Make a list of all the files that exist in the update file for the data group
        file_lst = getfileLst(update_configs,data_group)

        #orders the tables so there are no conflicts when entering into or removing from the database
        ordered_tables, temp_lst = orderTables(update_configs,file_lst,[],[]) 
        # I only want the tables that are in file_lst
        ordered_file_lst = [x for x in ordered_tables if x in file_lst]

        for file in ordered_file_lst[::-1]:
            # if file in ["tenement_occurrence"]:
            # set path of the file in the output/new directory
            new_path = os.path.join(self.new_dir,"%s.csv"%(file))
            # only progress if the new file exists. If it doesn't, there may be no changes, or there may be an error i.e. vba macro hasn't been run
            if fileExist(new_path):
                print("Working on: %s"%(file))
                # set the paths
                core_path = os.path.join(self.core_dir,"%s.csv"%(file))
                change_file_path = os.path.join(self.change_dir,"%s.csv"%(file))
                # set the key (index) column and the fields to drop from the db_update_configs file
                if update_configs[file]["record_changes"]["data_group"] == data_group:
                    key = update_configs[file]["record_changes"]["key"]
                else:
                    key = update_configs[file]["record_changes"]["second_data_group"]["key"]

                # get the core file and filter for the change ids
                core_df = pd.read_csv(core_path)
                new_df = pd.read_csv(new_path)

                # Get the relevant rows from the new file
                to_add_df = new_df[new_df[key].isin(update_keys_df)].copy()

                # If the table contains the _ID field then it is a m2m table which require the index field to be calculated from the last of the core_df.
                if "_ID" in to_add_df.columns:
                    next_id = core_df[key].max() + 1
                    to_add_df["_ID"] = np.arange(next_id, len(to_add_df) + next_id)

                # drop the relevant rows from the core df and concat the to_add_df
                edit_core_df = core_df[~core_df[key].isin(update_keys_df)]
                edit_core_df = pd.concat((edit_core_df,to_add_df))

                # Write to core csv and change csv
                edit_core_df.to_csv(core_path,index=False)
                to_add_df.to_csv(change_file_path,index=False)

                # delete all rows from the db
                field = update_configs[file]["columns"][key]
                lst = [str(x) for x in update_keys_df.values.tolist()] # The value is stored in the db as a string
                table = "map_%s"%(file.lower())
                # print(len(update_keys_df.index))
                # print(tuple(update_keys_df.values.tolist()))
                clearDbTableRowsInLst(psycopg2_con, table, field, lst)

                # To add the data to the db, the column names need to be changed to match the db
                to_add_df.rename(columns=update_configs[file]["columns"],inplace=True)
                # Place change_file_df into a dictionary. Tables need to be appended in the db in order. Therefore, they need to be saved in a dictionary and added in the next step
                change_dic[file] = to_add_df

        # write dfs to db in order
        print("Appending data to database for the %s data_group"%(data_group))
        for file in ordered_file_lst:
            print("Working on: %s"%(file))
            table = "map_%s"%(file.lower())
            df = change_dic[file]
            df.to_sql(table, sqlalchemy_con, if_exists='append', index=False, method='multi')
            # print("%s write to db."%(file))
            # print(df.head())




def compare_base_tables_add_new(self):
    print("Finding new entries for the base tables and updating the core file and database.")
    # Configs
    update_configs = self.update_configs
    access_configs = self.access_configs

    # db connections
    sqlalchemy_con = sqlalchemy_engine(access_configs["local"]).connect()
    psycopg2_con = connect_psycopg2(access_configs["local"])

    file_lst = [x for x in update_configs if update_configs[x]["record_changes"] == None]
    
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
                if "_ID" in new_ids_df.columns:
                    next_id = core_df["_ID"].max() + 1
                    new_ids_df["_ID"] = np.arange(next_id, len(new_ids_df) + next_id)

                # concat new rows to the core_df
                new_core_df = pd.concat((core_df,new_ids_df))
                # save new core_df
                new_core_df.to_csv(core_path,index=False)

                # append new rows to the database table
                new_ids_df.rename(columns=update_configs[file]["columns"],inplace=True)
                table = "map_%s"%(file.lower())
                new_ids_df.to_sql(table, sqlalchemy_con, if_exists='append', index=False, method='multi')









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