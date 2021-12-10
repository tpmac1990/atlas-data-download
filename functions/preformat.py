import os
import sys
import csv
import re
import ctypes
import pandas as pd
import numpy as np
import shutil
from collections import Counter

from .directory_files import copy_directory, write_to_file, get_json

from .timer import Timer
from .schedule import Schedule
from .setup import SetUp, Logger
from .backup_data import DataBackup

csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))



class PreformatData:

    def all_preformat(self):
        ''' performs all the formatting on the files before they can be merged together into the database ready tables '''
        timer = Timer()
        Logger.logger.info(f"\n\n{Logger.hashed}\nPreformat Data\n{Logger.hashed}")

        # backup necessary data. This could be split into two
        dbu = DataBackup('preformat')
        dbu.backup_data()
        
        try:
            occ = PreFormatDataGroup('occurrence')
            self.preformat_files(occ)

            ten = PreFormatDataGroup('tenement')
            self.preformat_files(ten)

        except:
            dbu.restore_data()
            raise

        Logger.logger.info('Preformat time: %s' %(timer.time_past()))



    def preformat_files(self,group):        
        # # # # # create necessary folders in the input and output archive directories
        # # # # group.create_required_directories()
        # # # # # archive previous input files
        # # # # group.archive_clear_past_input_files()
        # # # # # archive previous output files
        # # # # group.archive_clear_past_output_files()
        # preformat the files
        group.preformat_grp_files()
        # add the gplore identifier field
        group.add_Identifier_field()
        # create the files that will be combined to make the complete dataset
        group.create_change_files()
        # save the updated schedule config file
        group.save_schedule_configs_to_file()




class PreFormatDataGroup:

    def __init__(self, data_group):
        # set directories and open config files
        self.data_group_dir = os.path.join(SetUp.input_dir,data_group)
        self.core_dir = os.path.join(self.data_group_dir,'core')
        self.plain_dir = os.path.join(self.data_group_dir,'plain')
        self.change_dir = os.path.join(self.data_group_dir,'change')
        self.new_dir = os.path.join(self.data_group_dir,'new')
        self.update_dir = os.path.join(self.data_group_dir,'update')
        # self.archive_dir = os.path.join(self.data_group_dir,'archive',SetUp.tDate)
        self.update_path = os.path.join(self.update_dir,'update.csv')
        self.inactive_path = os.path.join(self.update_dir,'inactive.csv')
        self.reactivated_path = os.path.join(self.update_dir,'reactivated.csv')
        # self.output_archive_dir = os.path.join(SetUp.output_dir,'archive',SetUp.tDate)
        self.configs = get_json(os.path.join(SetUp.configs_dir,'formatting_config.json'))[data_group]
        self.download_configs = configs = get_json(os.path.join(SetUp.configs_dir,'download_config.json'))[data_group]
        self.download_fail_path = os.path.join(self.data_group_dir,'download_fail.csv')
        self.data_group = data_group
        # self.ignore_files = getIgnoreFiles(self)
        self.Schedule = Schedule()
        self.download_schedule_config = self.Schedule.open_schedule()

        Logger.logger.info(f"\n{Logger.dashed} {data_group} {Logger.dashed}")


    # def create_required_directories(self):
    #     ''' create the 'change', 'core', 'update' folders in both the input and output archive directories
    #         if they don't alreay exist.
    #     '''
    #     Logger.logger.info("Creating 'change, core, update' folders in the 'input, output' archive directories")
    #     create_multiple_directories(self.archive_dir,['change','core','update'])
    #     create_multiple_directories(self.output_archive_dir,['change','core','update'])



    def preformat_grp_files(self):
        ''' Loop through each format type within the preformat dictionary in configs to prepare the files to be combined. 
            Formatting includes removing unnecessary rows, order values in a field, etc
        '''
        # df's to create site types from a comments section
        instr_find_df, type_instr_delete = self.create_instr_df()
        # loop through all files in the 'now' schedule. use a 'copy' to prevent the loop skipping after removing a value from the list
        format_files_lst = self.download_schedule_config[self.data_group]['now'].copy()
        # loop
        for key in format_files_lst:
            try:
                config_lst = self.configs[key]['preformat']['format']
            except TypeError:
                # This error can only occur if a file that doesn't require formatting is added to the schedule after running the 'data_download' step. Usually, these files would
                #   be removed from the 'now' schedule in the data_download step
                Logger.logger.error(f"file '{key}' does not require formatting")
                self.download_schedule_config = self.Schedule.unrequired_file(key, self.data_group, self.download_schedule_config)
                continue

            # only format files if formatting is required
            if len(config_lst) > 0:
                Logger.logger.info(f"preformatting '{key}'")
                path = os.path.join(self.new_dir,key+'_WKT.csv')
                try:
                    df = pd.read_csv(path,dtype=str)
                except:
                    try:
                        # remove from schedule and add to today
                        self.download_schedule_config = self.Schedule.failed_file(key, self.data_group, self.download_schedule_config)
                        raise

                    except FileNotFoundError:
                        Logger.logger.warning(f"file '{key}' was not found. Unable to preformat")
                        continue

                    except pd.errors.EmptyDataError:
                        Logger.logger.warning(f"file '{key}' was empty. Unable to preformat")
                        continue

                # loop through all the required formatting methods for the file
                try:
                    for dic in config_lst:
                        format_type = dic['type']
                        config = dic['configs']

                        if format_type == 'combine_ausos_well_duplicates':
                            df = combineSameNameWellsAusOS(df,config)

                        elif format_type == 'combine_polygons':
                            df = combinePolygonsWithSameID_VIC(df,config)

                        elif format_type == 'create_unique_key':
                            df = createUniqueKeyFieldAllFiles(df,config)

                        elif format_type == 'duplicate_rows_key':
                            df = removeDuplicateRowsByKeyAllFiles(df,config)

                        elif format_type == 'merge_file':
                            df = combineFilesAllFiles(df,config,self.new_dir)

                        elif format_type == 'create_type_join_fields':
                            df = getSiteTypeFromJoinedString(df,config,instr_find_df,type_instr_delete)
                        
                        elif format_type == 'clear_value':
                            df = clearFieldIfContainsValueFromList(df,config)

                        elif format_type == 'field_filter':
                            df = filterDataframeForMultiple(df,config)

                        elif format_type == 'multi_blank_filter':
                            df = filterDataframeForMultipleBlanks(df,config)

                        elif format_type == 'keyword_drop':
                            df = filterColKeyword(df,config)

                        elif format_type == 'merge_rows':
                            df = combineHolderWithPercent(df,config)

                        elif format_type == 'sort_values':
                            df = sortMultipleValuesString(df,config)

                        elif format_type == 'no_match_blank':
                            df = convertUnmatchedToNull(df,config)

                        elif format_type == 'run_method_on_different_file':
                            # used to add the unique field in SA_2 so it can be merged with VIC_1
                            runMethodOnDifferentFile(self.new_dir,config)
                        else:
                            Logging.logging.warning(f"'{format_type}' is not a valid formatting method. Check file '{key}' in dataset '{self.data_group}'")
                            self.download_schedule_config = self.Schedule.failed_file(key, self.data_group, self.download_schedule_config)
                            raise Exception

                except Exception:
                    # If an error occurred then don't save as csv. The file is dropped from 'now' and added to 'today' above in the schedule
                    continue

                df.to_csv(path,index=False)




    # Add a unique ID for each tenement/ occurrence
    def add_Identifier_field(self):
        ''' Create a 'NEW_IDENTIFIER' field so the data can be compared between downloads.
            Create 'NEW_ID' so each feature has its own id in the application.
            The code will start from 1 million so all pnts and polys have 7 values 
        '''
        i = 1000000

        format_files_lst = self.download_schedule_config[self.data_group]['now'].copy()
        for key in format_files_lst:
            if not self.configs[key]['preformat']:
                Logger.logger.info(f"No preformatting required for '{key}'")
                continue
            Logger.logger.info(f"Assigning unique identifier for '{key}'")
            try:
                unique_col = self.configs[key]['preformat']['unique_column']
                # if unique_col:
                path = os.path.join(self.new_dir, key + '_WKT.csv')
                df = pd.read_csv(path,dtype=str)
                # copy 'unique_col' field to create a unique column
                df['NEW_IDENTIFIER'] = df[unique_col]
                # create the gplore ind value
                last_ind = len(df.index) + i
                df['NEW_ID'] = np.arange(i, last_ind)
                i = last_ind
                
                df.to_csv(path,index=False)

            except:
                try:
                    # remove from schedule and add to today
                    self.download_schedule_config = self.Schedule.failed_file(key, self.data_group, self.download_schedule_config)
                    raise
            
                except FileNotFoundError:
                    # non-existing files should have already been removed from the schedule config before this point
                    Logger.logger.warning(f"Unable to assign unique identifier. file '{key}' does not exist")
                    
                except KeyError as e:
                    Logger.logger.exception(f"{str(e)} field is missing in table '{key}'")

                except TypeError:
                    Logger.logger.exception(f"Files '{key}' configs in 'formatting_configs' are incorrect")



    def create_change_files(self):
        ''' If this is the initial download then the files from the 'new' directory will be copied to the 'core' and 'change' directories 
            and a blank change.csv file will be created in the 'update' directory.
            If this is an update, then this compares the newly downloaded file to their matching core file, searches for differences and 
            records them in the files in the update directory.
        '''
        # core_files = os.listdir(self.core_dir)
        inactive_headers = ["FILE","NEW_IDENTIFIER","NEW_ID"]
        reactivated_headers = ["FILE","NEW_IDENTIFIER","NEW_ID","DATE"]
        # check if files exist in core
        if not SetUp.isUpdate:
            # need to copy files from new to change and core directory to set the initial files as this is the initial download
            self.copy_new_files_to_core_and_change()
            # create a blank file to hold the inactive unique ids and new_id pairs / reactivated data.
            pd.DataFrame(columns=inactive_headers).to_csv(self.inactive_path,index=False)
            pd.DataFrame(columns=reactivated_headers).to_csv(self.reactivated_path,index=False)
        else:
            # create a blank file to hold the inactive unique ids and new_id pairs.
            pd.DataFrame(columns=inactive_headers).to_csv(self.inactive_path,index=False) # delete me
            pd.DataFrame(columns=reactivated_headers).to_csv(self.reactivated_path,index=False)
            # compare core to new. Creates the change and update files
            self.createUpdateFile_updateCore()

    

    def copy_new_files_to_core_and_change(self):
        ''' this is used on the first run only. The new files are copied to the core directory to set the files to 
            be updated on later updates. The files in the new directory are also copied to the change directory as
            these files will be exported to the db
        '''
        Logger.logger.info("Copying files from the new to the core & change directory. Initial download")
        copy_directory(self.new_dir,self.core_dir)
        copy_directory(self.new_dir,self.change_dir)



    def createUpdateFile_updateCore(self):
        ''' compares the newly downloaded file to their matching core file, searches for differences and records them '''
        Logger.logger.info(f"\nComparing the new and core files to build the change & update files {Logger.dashed}")
        high_value = self.findHighestIdentifier()

        config_file_lst = [f'{x}_WKT.csv' for x in self.configs if self.configs[x]['preformat']]
        new_files = [x for x in os.listdir(self.new_dir) if x in config_file_lst]
        core_files = os.listdir(self.core_dir)

        update_path = os.path.join(self.update_dir,'update.csv')

        inactive_df = pd.read_csv(self.inactive_path)
        reactivated_df = pd.read_csv(self.reactivated_path)
        
        # create the update_list
        update_lst = []
        headers = ['NEW_ID','ACTION','FILE','IDENTIFIER']
        update_lst.append(headers)

        for file in new_files:
            # if file[:-8] in ['WA_1']:
            if file in core_files:
                dic_key = file[:-8]
                Logger.logger.info(f"Comparing new to core for '{file}'")

                new_path = os.path.join(self.new_dir,file)
                core_path = os.path.join(self.core_dir,file)
                change_path = os.path.join(self.change_dir,file)
                required_fields = self.configs[dic_key]['required_fields']

                # read the file to df
                try:
                    new_df = pd.read_csv(new_path,dtype=str,encoding = "ISO-8859-1")
                except pd.errors.EmptyDataError:
                    Logger.logger.warning(f"The dataframe of '{file}' was empty. Unable to compare to core file")
                    continue
                core_df = pd.read_csv(core_path,dtype=str,encoding = "ISO-8859-1")
                
                # create df with only the required fields from configs. this will help find all differences between the new and core file
                new_required_df = new_df[required_fields]
                core_required_df = core_df[required_fields]

                # create df with only the identifier column. This will help find the add and remove ids
                new_index_df = new_df[["NEW_IDENTIFIER"]]
                core_index_df = core_df[["NEW_IDENTIFIER"]]

                merge_df = core_required_df.merge(new_required_df,indicator=True,how='outer')

                diff_df = merge_df[merge_df["_merge"] != "both"]

                id_merge_df = core_index_df.merge(new_index_df,indicator=True,how='outer')

                # remove_lst = singleColumnDfToList(id_merge_df[id_merge_df["_merge"] == "left_only"])
                # add_lst = singleColumnDfToList(id_merge_df[id_merge_df["_merge"] == "right_only"])
                # ???

                remove_lst = id_merge_df[id_merge_df["_merge"] == "left_only"].drop('_merge',1).values.flatten().tolist()
                add_lst = id_merge_df[id_merge_df["_merge"] == "right_only"].drop('_merge',1).values.flatten().tolist()

                # this is used to create a list of only the change rows
                none_change_lst = remove_lst + add_lst

                # remove the dups by converting into a set and back again
                # diff_lst = list(set(singleColumnDfToList(diff_df[["NEW_IDENTIFIER"]]))) ???
                diff_lst = diff_df[["NEW_IDENTIFIER"]].drop_duplicates().values.flatten().tolist()

                # find all the ids to remove. if its not in the remove or add list then it has to be a remove item
                change_lst = [ind for ind in diff_lst if not ind in none_change_lst]

                # use this to see the differences between the two df. Each file will have it's own check file saved in the update folder
                check_file_path = os.path.join(self.update_dir,dic_key + '_check.csv')
                create_check_file(new_required_df,core_required_df,change_lst,check_file_path)

                # create a dictonary of the identifiers from the remove and change list as key and its gplore id found in the core_df. 
                remove_change_lst = change_lst + remove_lst
                index_lookup_df = core_df[["NEW_IDENTIFIER","NEW_ID"]]
                index_lookup_lst = index_lookup_df[index_lookup_df["NEW_IDENTIFIER"].isin(remove_change_lst)].values.tolist()
                dic = {group[0]:group[1] for group in index_lookup_lst}
                # print(dic)

                # adds to the update_lst and builds the temp_lst of the differences between the core and new files. This will be uses to create the change file
                # and make changes in the core file.
                temp_lst = []
                # I will not remove any occurrence rows even if they are removed from the state datasets. Only user data will make updates to occurrence data, new data will still be added.
                if self.data_group == 'Occurrence':
                    remove_lst = []

                for entry in [[add_lst,"ADD"],[remove_lst,"REMOVE"],[change_lst,"CHANGE"]]:
                    for row in entry[0]:
                        action = entry[1]
                        if action == "ADD":
                            high_value += 1
                            g_id = high_value
                        else:
                            g_id = dic[row]
                            # print("%s - %s"%(g_id,row))
                        data = [g_id,action,file,row]
                        temp_lst.append(data)
                        update_lst.append(data)
                # print(temp_lst)

                # correct NEW_IDs in the new file, 
                if len(temp_lst) > 0:
                    fileupdate_df = pd.DataFrame(temp_lst,columns=headers)
                    # print(fileupdate_df)
                    add_df = fileupdate_df[fileupdate_df["ACTION"] == "ADD"]
                    # correct the NEW_IDs in the new file
                    if len(add_df.index) > 0:
                        # # filters the new_df by the identifier values, get the NEW_IDs which will be replaced by the correct NEW_IDs from the update file
                        # newid_lst = singleColumnDfToList(add_df[["NEW_ID"]])
                        # identifier_lst = singleColumnDfToList(add_df[["IDENTIFIER"]]) ???
                        newid_lst = add_df[["NEW_ID"]].values.flatten().tolist()
                        identifier_lst = add_df[["IDENTIFIER"]].values.flatten().tolist()
                        oldid_lst = new_df[new_df["NEW_IDENTIFIER"].isin(identifier_lst)]["NEW_ID"].values.tolist()
                        # replace the old NEW_IDs with the NEW_IDs
                        new_df['NEW_ID'] = new_df['NEW_ID'].replace(oldid_lst,newid_lst)

                
                    # df for the new rows to be added to the core file. 
                    change_add_lst = fileupdate_df[fileupdate_df["ACTION"] == "ADD"]["IDENTIFIER"].values.tolist()
                    change_add_df = new_df[new_df["NEW_IDENTIFIER"].isin(change_add_lst)]
                    # the currently stored inactive ids for this file
                    file_inactive_df = inactive_df[inactive_df["FILE"] == file[:-8]].drop(columns="FILE",axis=1)
                    # check if the NEW_IDENTIFIER already exists in the file_inactive_df. if so, replace its gplore id with the one in file_inactive_df 
                    # and delete the row from file_inactive_df.
                    add_diff_df = change_add_df[["NEW_IDENTIFIER","NEW_ID"]].merge(file_inactive_df,on="NEW_IDENTIFIER",indicator=True,suffixes=("_new","_core"),how='outer')
                    add_diff_df = add_diff_df[add_diff_df["_merge"] == "both"].drop(columns=["_merge","NEW_IDENTIFIER"],axis=1)
                    # if new ids exist in the file_inactive_df then replace the new_id in the change_add_df with the new_id from the add_diff_df
                    if len(add_diff_df.index) != 0:
                        add_dic = change_add_df.set_index('NEW_ID_new')['NEW_ID_core'].to_dict()
                        change_add_df.replace({"NEW_ID": add_dic},inplace=True)
                        # remove NEW_ID_core from file_inactive_df as it will no be added back into the new_df
                        reactivated_core_df = add_diff_df["NEW_ID_core"]
                        # df of the files inactive ids excluding the newly reactivated ids
                        file_inactive_df = file_inactive_df[~file_inactive_df["NEW_ID"].isin(reactivated_core_df)]
                        # concat the newly reactivated ids to the reactivated sheet
                        works_reactivated_df = add_diff_df[add_diff_df["_merge"] == "both"].drop(columns=["_merge","NEW_ID_new"],axis=1).rename(column={"NEW_ID_new": "NEW_ID"})
                        works_reactivated_df["FILE"] = file[:-8]
                        works_reactivated_df["DATE"] = self.pyDate
                        # add the newly reactivated data to the reactivated_df. This keeps track of the reactivated ids and the date
                        reactivated_df = pd.concat([reactivated_df,works_reactivated_df],ignore_index=True)
                    # re-add the file name column
                    file_inactive_df["FILE"] = file[:-8]
                    # get all the inactive values except those for this file. They will be re-added below with the updates
                    inactive_df = inactive_df[inactive_df["FILE"] != file[:-8]]
                    # add remove_lst rows to inactive df so we can find their correct new_id if they are reinstated
                    works_inactive_df = core_df[core_df["NEW_IDENTIFIER"].isin(remove_lst)][["NEW_IDENTIFIER","NEW_ID"]]
                    # add the FILE column. required incase there are two files will the same ids.
                    works_inactive_df["FILE"] = file[:-8]
                    # inactive_df:  maintained df minus this files values, 
                    # file_inactive_df:  this files maintained values minus the values that have been reactivated, 
                    # works_inactive_df: all the values for this file that are currently being removed
                    inactive_df = pd.concat([inactive_df,file_inactive_df,works_inactive_df],ignore_index=True)

                        
                    # df of the rows with changes. These rows need to extract the correct NEW_ID from the fileupdate_df which is from the core_df. This is so we use the 
                    # same NEW_ID that already exists.
                    change_change_lst = fileupdate_df[fileupdate_df["ACTION"] == "CHANGE"]["IDENTIFIER"].values.tolist()
                    # get the rows from the new_df and drop the NEW_ID column. these ids are incorrect
                    change_change_df = new_df[new_df["NEW_IDENTIFIER"].isin(change_change_lst)].drop(columns="NEW_ID",axis=1)
                    # df of the new_ID and IDENTIFIER from the fileupdate_df which is from the core_df.
                    change_core_df = fileupdate_df[fileupdate_df["ACTION"] == "CHANGE"][["IDENTIFIER","NEW_ID"]]
                    # merge the correct NEW_ID with the changed rows df on the IDENTIFIER column to give it the correct NEW_ID and drop the now duplicate IDENFIER column
                    change_core_df = change_change_df.merge(change_core_df,left_on="NEW_IDENTIFIER",right_on="IDENTIFIER").drop(columns="IDENTIFIER",axis=1)
                    # merge the ADD and CHANGE df to create a df with all the new data to be entered into the core_df
                    change_df = pd.concat((change_add_df,change_core_df))


                    # The change file contains new and change rows of data. If change_df has 0 rows then there is no need to create a change file.
                    if len(change_df.index) != 0:
                        change_df.to_csv(change_path,index=False) 

                    # drop all CHANGE, REMOVE update values in the core file and concatenate the change file which holds the new and changed values.
                    update_ident_lst = fileupdate_df[fileupdate_df["ACTION"].isin(["REMOVE","CHANGE"])]["IDENTIFIER"].values.tolist()
                    core_df.set_index("NEW_IDENTIFIER",drop=False,inplace=True)
                    core_df.drop(update_ident_lst,inplace=True)
                    new_core_df = pd.concat((core_df,change_df),ignore_index=True)
                    new_core_df.to_csv(core_path,index=False)

        # write update list to csv
        write_to_file(update_path, update_lst)
        # write updated inactive df to csv. reactivated ids have been removed and 
        inactive_df.to_csv(self.inactive_path,index=False)
        # write reactiveated df to csv file
        reactivated_df.to_csv(self.reactivated_path,index=False)



    def create_instr_df(self):
        ''' This builds the df's that are used to calculate the type of site by looking at a string of values, generally a comments section. Values
            are ranked and only the highest value and others that share the same rank are recorded
        '''
        configs = self.configs
        # files used to create a type from searching for key words in a string made from merging relevant columns
        convert_occ_path = SetUp.convert_dir
        instr_type_find_df = pd.read_csv(os.path.join(convert_occ_path,'instr_type_find.csv'))
        type_rank_df = pd.read_csv(os.path.join(convert_occ_path,'type_rank.csv'))
        # convert list to string with | to use in regex expression
        type_instr_delete = "|".join(pd.read_csv(os.path.join(convert_occ_path,'type_instr_delete.csv'))['value'].tolist())
        instr_find_df = instr_type_find_df.merge(type_rank_df,left_on='replace',right_on='name',how='left').drop('name',1).sort_values('rank').values.tolist()
        return instr_find_df, type_instr_delete



    # finds the highest ind value so the new values can be added on from this.
    def findHighestIdentifier(self):
        high_value = 0
        for file in self.configs:
        # for file in format_files_lst:
            if not self.configs[file]['preformat'] or not self.configs[file]['preformat']["unique_column"]: 
                # files such as SA_2 occurrence are combined with another file and therefore have no extra data. other files might not require a unique id. these can be skipped
                continue

            try:
                df = pd.read_csv(os.path.join(self.core_dir,f'{file}_WKT.csv'), header=0, dtype=str, encoding = "ISO-8859-1")
                file_high = df['NEW_ID'].astype(int).max()
                if file_high > high_value:
                    high_value = file_high
            except pd.errors.EmptyDataError:
                Logger.logger.warning(f"Unable to find the highest identifier in an empty file '{file}'")
        return high_value


    def save_schedule_configs_to_file(self):
        ''' update the schedule configs '''
        self.Schedule.update_schedule(self.download_schedule_config)
            


def runMethodOnDifferentFile(directory,config):
    ''' this will run a formatting method on a file while in the middle of formatting another file. This is useful when something needs 
        to be formatted in a related file in order to format the first file 
    '''
    path = os.path.join(directory,config['file']+'_WKT.csv')
    df = pd.read_csv(path)

    if config['method'] == 'create_unique_key':    
        df = createUniqueKeyFieldAllFiles(df,config['configs'])

    df.to_csv(path)


def convertUnmatchedToNull(df,configs):
    ''' Convert values in a field that don't exist in a given list to np.nan.
        used to clear corrupted values in 'tenement VIC_2'
    '''
    field = configs['field']
    lst = configs['vals']
    df[field] = df[field].apply(lambda x: x if x in lst else np.nan)
    return df


def sortMultipleValuesString(df,configs):
    ''' Sort multiple values in one field. A database does not have an order, so when multiple values are joined and inserted 
        into one field, they will not necessarily be inserted in the same order each time. Ordering them here will allow these fields to be compared between two files. 
        split the values by the separator, sort them and then join them back together
    '''
    separator = configs['separator']
    field = configs['field']
    df[field] = df[field].apply(lambda x: '; '.join(sorted([i.strip() for i in x.split(separator)])))
    return df


def combineHolderWithPercent(df,configs):
    ''' Merge separate 'holder name' and 'percent ownership' fields into one field. This is required to keep 
        the percent with the holder as there may be mulitple holders for the one title 
    '''
    df[configs['out_field']] = df.apply(lambda x: '%s (%s%%)'%(x[configs['fields'][0]],x[configs['fields'][1]]), axis=1)
    return df


def filterColKeyword(df,configs_lst):
    ''' filter for or filter out a list of values in a df for a given field. This is not case sensitive '''
    for dic in configs_lst:
        df[dic['field']] = df[dic['field']].fillna('')
        df['temp_col'] = df[dic['field']].apply(lambda x: any([k.lower() in x.lower() for k in dic['keywords']]))
        df[dic['field']].replace('', np.nan, inplace=True)
        if dic['type'] == 'include':
            df = df[df['temp_col'] == True].copy()
        else:
            df = df[df['temp_col'] == False].copy()
    df.drop(columns=['temp_col'],inplace=True)
    return(df)


def filterDataframeForMultipleBlanks(df,filter_fields):
    df['temp_col'] = df[filter_fields].apply(lambda x: any(["%s"%(i) != "nan" for i in x]), axis=1)
    df = df[df['temp_col'] == True].copy()
    df.drop(columns=["temp_col"],inplace=True)
    return(df)


def clearFieldIfContainsValueFromList(df,configs):
    ''' Clearing fields that contain provided values '''
    field = configs['field']
    values = r"(%s)"%("|".join(configs['values']))
    df[field] = df[field].fillna('').apply(lambda x: '' if re.search(values,x.lower()) else x)
    return df


def getSiteTypeFromJoinedString(df,configs,instr_find_df,type_instr_delete):
    ''' Creating Type field by finding keywords in string '''
    join_fields = configs
    df = df.replace(np.nan,'',regex=True)
    df[['JOIN_TYPE','CLEAN_TYPE','NEW_TYPE']] = df.apply(lambda x: temp_fn(x,join_fields,type_instr_delete,instr_find_df), axis=1, result_type='expand')
    return df


def combineFilesAllFiles(main_df,configs,new_dir):
    ''' merge two files on a given column and overwrite fields of the primary file with the values of the second file. '''

    other_path = os.path.join(new_dir,configs['file'] + '_WKT.csv')
    other_df = pd.read_csv(other_path)

    fields = configs['fields']
    # get list of fields to keep in the other_df and fields to remove in the main_df
    keep_other = [x[1] for x in fields] + [configs['key']['other_file']]
    remove_main = [x[0] for x in fields]

    # drop fields from the main_df that will be replaced by their equal fields in the other_df
    min_main_df = main_df.drop(remove_main,axis=1)
    # keep only the fields in the other_df that are required when concatenating it the main_df
    min_other_df = other_df[other_df.columns.intersection(keep_other)].copy()

    # build the rename dictionary
    rename = { x[1]:x[0] for x in fields }
    # rename fields in other_df so they are allocated to the correct columns when concatenated with the main_df
    min_other_df.rename(rename,inplace=True,axis='columns')
    # merge main_df with other_df to give other_df the required fields from main_df
    other_merge_df = min_other_df.merge(min_main_df,left_on=configs['key']['other_file'],right_on=configs['key']['this_file'],how='left')
    # filter out the rows from the main_df which have keys that exist in the other_df.
    new_main_df = main_df[~main_df[configs['key']['this_file']].isin(other_df[configs['key']['other_file']])]
    # concat the two df's together
    final_df = pd.concat((new_main_df,other_merge_df),ignore_index=True)

    return final_df


def removeDuplicateRowsByKeyAllFiles(df,configs): 
    ''' drops duplicate rows by a given column '''
    df.drop_duplicates(subset=configs,inplace=True)
    return df


def createUniqueKeyFieldAllFiles(df,configs):
    ''' creates a unique key from the geom for those files that don't have a unique value. 'ignore_files' are those that failed to download, check
        the 'download_fail' csv in the input folder.
    '''
    name = configs['name']
    if name == 'join_fields':
        df['UNIQUE_FIELD'] = df.apply(lambda x: "-".join(str(int(float(x[i]))) for i in configs['fields']),axis=1)

    elif name == 'add_multipolygon':
        df['UNIQUE_FIELD'] = df.apply(lambda x: multipolygon_id(x[configs['field']]),axis=1)

    return df


def combinePolygonsWithSameID_VIC(df,configs):
    ''' remove dupicate rows determined by equal 'TNO'(index) and 'geometry' fields. Then merge the geometry of the remaing duplicate 'TNO' 
        rows so there are no duplicate 'TNO' values,
        status_field: column name that contains the status
        status_value: value that determines the title is active
    '''
    index = configs['index']
    status_field = configs['status']['field']
    status_value = configs['status']['value']

    # active only df
    df = df[df[status_field] == status_value].copy()

    # remove duplicate rows by subsets ids and hectares. could use geometry instead of hectares
    # ?? TNO was different for both files
    df.drop_duplicates(subset=[index,'HECTARES'],keep='first',inplace=True)

    # get all the rows that are duplicated
    dups_df = df[df[index].duplicated()]
    # create unique list of duplicated ids
    ids_lst = dups_df[index].drop_duplicates().values.tolist()

    # iterate through each id and then iterate through each of its geometries. Join the geometries together and return them as a list with the id value and the combined geometry
    final_lst = []
    for ind in ids_lst:
        geom_lst = dups_df[dups_df[index] == ind]['geometry'].values.tolist()
        geom_str = geom_lst[:1][0]
        for geom in geom_lst[1:]:
            geom_str = geom_str[-0:-1] + "," + geom
        final_lst.append([ind,geom_str])

    # convert list to df with old headers
    updated_df = pd.DataFrame(final_lst,columns=[index,'geometry'])
    
    # drop all duplicates in the main df
    df.drop_duplicates(subset=index,keep=False,inplace=True)

    # filter the df for only the duplicated rows ids. drop geometry as this will be replaced with the combined geometry value
    new_geom_df = df[df[index].isin(ids_lst)].drop('geometry',1)
    # merge the dfs to apply the combined geometry to one id
    merge_df = new_geom_df.merge(updated_df, on=index, how='left')
    # merge the df with only the unique id rows with the newly create df that contains the id rows with merged geometry
    final_df = pd.concat((df,merge_df))
    return final_df

                

def combineSameNameWellsAusOS(df,configs):
    ''' One well may have multiple hole reasons, but I only want the latest one. This will remove all duplicate rows by ids and keep the most important hole type.
        filter the original df for given hole type and then concat them together. When duplicates are dropped, it will leave only the most import ant hole type.
        configs['vals']: order matters. ordered from most important to least important
    ''' 
    final_df = pd.DataFrame()
    
    for val in configs['vals']:
        temp_df = df[df[configs['field']] == val]
        final_df = pd.concat((final_df,temp_df))

    final_df.drop_duplicates(subset=configs['index'],keep='first',inplace=True)
    return final_df


def filterDataframeForMultiple(df,vals_lst):
    ''' filter by including or excluding a list of values for a given column '''
    for dic in vals_lst:
        if dic['type'] == 'include':
            df = df[df[dic['name']].isin(dic['vals'])]
        else:
            df = df[~df[dic['name']].isin(dic['vals'])]
    return(df)



def temp_fn(x,join_fields,type_instr_delete,instr_find_df):
    ''' combines all fields to create one string. deletes sub strings such as 'no workings' so it is not confused with 'workings'. Looks for substrings and returns
        the correct values.
    '''
    merge_1 = ' - '.join([x[i] for i in join_fields]).lower()
    merge_2 = re.sub(r"\b(%s)\b"%(type_instr_delete),"",merge_1)
    lst = []
    rank = None
    for i in instr_find_df:
        search = re.search(r"\b(%s)\b"%(i[0]),merge_2)
        if rank and rank < i[2]:
            break;
        if search:
            rank = i[2]
            if not i[1] in lst:
                lst.append(i[1])

    return (merge_1,merge_2,';'.join(lst))



def multipolygon_id(x):
    points = x.replace("MULTIPOLYGON ","").replace("(","").replace(")","").split(",")
    valSum = 0
    for pnt in points:
        valSum += float(pnt.strip().split(" ")[0])
    return "C%s" %(str(valSum).replace(".","")[-0:8])


# use this to see the differences between the two df. Each file will have it's own check file saved in the update folder
def create_check_file(new_required_df,core_required_df,change_lst,check_file_path):
    check_new_df = new_required_df[new_required_df["NEW_IDENTIFIER"].isin(change_lst)]
    check_core_df = core_required_df[core_required_df["NEW_IDENTIFIER"].isin(change_lst)]
    check_df = pd.concat((check_core_df,check_new_df),ignore_index=True)
    check_df.sort_values(by=["NEW_IDENTIFIER"],inplace=True)
    if len(check_df.index) > 0:
        check_df.to_csv(check_file_path,index=False)

