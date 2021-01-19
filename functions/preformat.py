import os
import sys
import csv
import ctypes
import pandas as pd
import shutil
import datetime
from collections import Counter

from .directories_files import *
from .csv_manipulation import *


def combinePolygonsWithSameID_VIC(self):
    if self.data_group == 'tenement':

        files = ["VIC_1_WKT","VIC_2_WKT"] 

        for file in files:
            print("Combining polygon data of duplicate ID's in %s" %(file))       

            file_path = os.path.join(self.new_dir,file+'.csv')

            with open(file_path, 'r', encoding="utf8") as inFile:

                # Get the file headers
                with open(file_path, 'r', encoding="utf8") as tFile:
                    tReader = csv.reader(tFile)
                    headers = next(tReader)

                # Filter the dataset for only the active tenements
                if file == "VIC_1_WKT":
                    reader = pd.read_csv(inFile,low_memory=False)
                    fReader = reader[reader['ACTIVE'] == "Current"].values.tolist()
                    hectaresCol = 60
                else:
                    reader = pd.read_csv(inFile,low_memory=False)
                    fReader = reader[reader['DBSOURCE'] == "rram_sf"].values.tolist()
                    hectaresCol = 30

                # Clear all the duplicate ids by duplicate area values
                noDups = []
                seen = set() # set for fast O(1) amortized lookup
                for line in fReader: # Create a set of all the duplicate IDs
                    polyID = "%s%s" %(line[1],line[hectaresCol])
                    if polyID not in seen: 
                        noDups.append(line)
                        seen.add(polyID)

                # Finds the duplicate IDs to Combine The polygon data for the duplicate ID's
                seenTwice = set()
                seen = set() # set for fast O(1) amortized lookup
                for line in noDups: # Create a set of all the duplicate IDs
                    polyID = line[1]
                    if polyID in seen: 
                        seenTwice.add(polyID)
                    else:
                        seen.add(polyID)

                # Creates a list of the duplicate ID's and combines the polygon data
                seen = set()
                data = []
                for line in noDups:
                    polyID = line[1]
                    WKT_val = line[0]
                    if polyID in seenTwice:
                        if polyID in seen:
                            i = 0
                            for data_row in data:
                                if data_row['ID'] == polyID:
                                    WKT = WKT_val
                                    WKT = WKT[14:]
                                    data[i]['WKT'] = data[i]['WKT'][-0:-1] + "," + WKT
                                i += 1
                        else:
                            seen.add(polyID)
                            data.append({'WKT': WKT_val,'ID': polyID})

                # Replace the duplicate IDs WKT field with the combined WKT field from the 'data' list created above
                all = []
                seenTwice = set()
                seen = set() # set for fast O(1) amortized lookup
                for line in noDups: # Create a set of all the duplicate IDs
                    polyID = line[1]
                    if polyID not in seen: 
                        all.append(line)
                        seen.add(polyID)
                    else:
                        if polyID not in seenTwice:
                            for data_row in data:
                                if data_row['ID'] == polyID:
                                    saved_WKT = data_row['WKT']
                                    break

                            i = 0
                            for reader_row in all:
                                if reader_row[1] == polyID:
                                    all[i][0] = saved_WKT
                                i += 1
                        else:
                            seenTwice.add(polyID)

                # Delete values in COVER that aren't strings
                if file == "VIC_2_WKT":
                    x=-1
                    covers = {"petrola","petrolb","petrole"}
                    for cover in all:
                        x += 1
                        if cover[5] not in covers:
                            all[x][5] = ""                        

                all.insert(0,headers)
                df = pd.DataFrame(all).fillna("")
                lOutFile = df.values.tolist()
            

            writeToFile(file_path, lOutFile)
            print("%s completed" %(file))

        print("VIC_1_WKT and VIC_2_WKT files have been formatted successfully!")




# Delete the first row of duplicate ID's
def deleteSecondofDuplicate_QLD_1(self):
    if self.data_group == 'tenement':
        fInput = "QLD_1_WKT"

        print("Deleting duplicate rows in %s" %(fInput))
        
        file_path = os.path.join(self.new_dir,fInput+'.csv')

        # Create a set of all the duplicate ID's
        with open(file_path,'r') as in_file:
            reader = csv.reader(in_file)
            seen = set() # set for fast O(1) amortized lookup
            seenTwice = set()
            for line in reader: # Create a set of all the duplicate IDs
                polyID = line[1]
                if polyID in seen: 
                    seenTwice.add(polyID)
                else:
                    seen.add(polyID)

        # Add all files to the outfile except for the first of the duplicate values (known from the seenTwice created above)
        with open(file_path,'r') as in_file:
            reader = csv.reader(in_file)
            all = []

            dbleDeleted = set()
            for line in reader: # Write all lines to the output file except the first of the dupicate values
                polyID = line[1]
                if polyID not in seenTwice or polyID in dbleDeleted:
                    all.append(line)
                else:
                    dbleDeleted.add(polyID)

        writeToFile(file_path, all)

        print("%s formatting completed" %(fInput))


# Add a unique ID for each tenement
def addIdentifierField(self):
    # the code will start from 1 million so all pnts and polys have 7 values
    i = 1000000

    for file in os.listdir(self.new_dir):
        # print(self.configs[file.replace('_WKT.csv','')]['unique_column'])
        if file.endswith("_WKT.csv"):
            # fName = file.split(".")[0]
            fName = file.replace('_WKT.csv','')
            unique_col = self.configs[fName]['unique_column']
            if unique_col != "":
                # if fName == "NSW_1":
                print("Adding NEW_IDENTIFIER and NEW_ID fields for %s" %(file))
                file_path = os.path.join(self.new_dir,file)
                with open(file_path, 'r') as t1:
                    reader = csv.reader(t1)
                    lst = []
                    row = next(reader)
                    row.append("NEW_IDENTIFIER")
                    row.append("NEW_ID")
                    lst.append(row)
                    for row in reader:
                        if row[0] != 'POLYGON EMPTY':
                            newID = i
                            row.append(row[unique_col])
                            row.append(newID)
                            lst.append(row)
                        i +=1

                writeToFile(file_path, lst)

                print("%s completed successfully" %(fName)) 

    print("NEW_IDENTIFIER and NEW_ID fields have been added successfully for all files!")



def deleteWA_5Blanks(directory):
    print('WA_5: deleting blank rows')
    file_path = os.path.join(directory,'WA_5_WKT.csv')

    with open(file_path,'r') as t1:
        reader = csv.reader(t1)

        all = []
        for line in reader:
            if len(line[5].strip()) != 0:
                all.append(line)

    writeToFile(file_path, all)

    print('complete')


def deletingInvalidWktRowsAllFiles(self):
    print('Deleting rows with invalid WKT.')
    new_files = os.listdir(self.new_dir)

    for file in new_files:
        print('Working on: %s' %(file))
        file_path = os.path.join(self.new_dir,file)

        with open(file_path,'r') as t1:
            reader = csv.reader(t1)
            lst = []
            headers = next(reader)
            lst.append(headers)

            for line in reader:
                wkt = line[0]
                if wkt.find('EMPTY') == -1 and  len(wkt) > 0:
                    lst.append(line)

        writeToFile(file_path, lst)
        print('Complete.')



def createWKTIdentifier(wkt):
    splt = wkt.split('-')
    val_sum = 0
    for val in splt[1:]:
        splt_val = val[3:9]
        if len(splt_val) > 5:
            try:
                val_sum += int(val[3:9])
            except:
                pass
    return val_sum



# def removeUnrequiredFields(reader,unrequired_fields):
#     all = []
#     for line in reader:
#         row = []
#         for ind, elem in enumerate(line):
#             if ind not in unrequired_fields:
#                 row.append(elem)
#         # row[0] = createWKTIdentifier(row[0])
#         all.append(row)
#     return all


def copyDirectoryDropColumn(src_dir,dest_dir,index_column):
        print('Dropping column %s from files.' %(index_column))
        src_files = os.listdir(src_dir)

        for file in src_files:
            print('Working on: %s' %(file))
            destination_path = os.path.join(dest_dir,file.replace("_WKT",""))
            src_path = os.path.join(src_dir,file)
            with open(src_path, 'r', encoding="utf-8", errors='replace') as t1:
                df = pd.read_csv(t1, low_memory=False)
                df = df.drop(df.columns[index_column], axis=1)
                df.to_csv(destination_path, index=False)
        print('Complete')


def archiveRemoveOldFiles(self):
    print('Archiving last files.')
    name_lst = ['change','core','new','update','vba']
    archive_lst = ['change','core','update']
    delete_lst = ['change','update','vba','plain']
    for name in name_lst:
        src_dir = os.path.join(self.data_group_dir,name)
        dest_dir = os.path.join(self.archive_dir,name)
        if name in archive_lst:
            copyDirectory(src_dir,dest_dir)
        if name in delete_lst:
            clearDirectory(src_dir)
    print('Complete.')

def archiveRemoveOutputFiles(self):
    print('Archiving last output files.')
    name_lst = ['change','core','new','update']
    archive_lst = ['change','core','update']
    delete_lst = ['change','update','new']
    for name in name_lst:
        src_dir = os.path.join(self.output_dir,name)
        dest_dir = os.path.join(self.output_archive_dir,name)
        if name in archive_lst:
            copyDirectory(src_dir,dest_dir)
        if name in delete_lst:
            clearDirectory(src_dir,extension=".csv")
    print('Complete.')


def combineSameNameWellsAusOS(self):
    if self.data_group == 'occurrence':
        print('Combining wells with the same name for Aus OS Wells.')
        file_path = os.path.join(self.new_dir,"AUS_OSPET_1_WKT.csv")
        val_lst = ["development","appraisal","exploration"]
        dic = {}

        with open(file_path, 'r') as t1:
            reader = csv.reader(t1)
            next(reader)
            for line in reader:
                key = line[2]
                val = line[10]
                if key in dic:
                    for item in val_lst:
                        if dic[key] != item:
                            if val == item:
                                dic[key] = item
                                break
                        else:
                            break
                else:
                    dic[key] = val


        with open(file_path, 'r') as t1:
            reader = csv.reader(t1)
            headers = next(reader)
            id_dic = {}
            lst = []
            lst.append(headers)
            for line in reader:
                key = line[2]
                if key not in id_dic.keys():
                    id_dic[key] = 1
                    line[10] = dic[key]
                    lst.append(line)
        
        writeToFile(file_path,lst)



def filterDataframeForMultiple(df,filter_vals_lst):
    for dic in filter_vals_lst:
        if dic['type'] == 'include':
            df = df[df[dic['name']].isin(dic['vals'])]
        else:
            df = df[~df[dic['name']].isin(dic['vals'])]
    return(df)

# filter for petroleum & coal seam methane which are for exploration and production
def filterRelevantData(path,filter_vals_lst):
    df1 = pd.read_csv(path,low_memory=False)
    df2 = filterDataframeForMultiple(df1,filter_vals_lst)
    df2.to_csv(path,index=False,encoding='utf-8',line_terminator='\n')

def filterAllFilesForRelevantData(self):
    print('Filtering csv files for relevant data.')
    for key in self.configs.keys():
        filter_vals_lst = self.configs[key]['field_filter']
        if filter_vals_lst != '':
            print('Working on: ' + key)
            path = os.path.join(self.new_dir,key+'_WKT.csv')
            filterRelevantData(path,filter_vals_lst)
    print('Complete.')            


def removeDuplicateRowsByKeyAllFiles(self): 
    print('Removing duplicate rows of data.')  
    for key in self.configs.keys():
        index = self.configs[key]['duplicate_rows_key']
        if index != '':
            print('Working on: ' + key)
            path = os.path.join(self.new_dir,key+'_WKT.csv')
            removeDuplicateRowsByKey(path,index)
            print('Complete.')


def combineFilesAllFiles(self):
    print('Combininig relevant files.')  
    for key in self.configs.keys():
        merge_file = self.configs[key]['merge_file']
        if merge_file != '':
            print('Working on: ' + key)

            file_path = os.path.join(self.new_dir,key+'_WKT.csv')
            with open(os.path.join(self.new_dir,merge_file['file']+'_WKT.csv'), 'r') as t1:
                dic = convertToDic(csv.reader(t1),merge_file['key']['other_file'])
            with open(file_path, 'r') as t2:
                reader = csv.reader(t2)
                headers = next(reader)
                lst = []
                lst.append(headers)
                for line in reader:
                    this_file_key = line[merge_file['key']['this_file']]
                    if this_file_key in dic.keys():
                        for field_index_lst in merge_file['index']:
                            line[field_index_lst[0]] = dic[this_file_key][field_index_lst[1]]
                    lst.append(line)
            writeToFile(file_path, lst)
            print('Complete.')


def createUniqueKeyFieldAllFiles(self):
    print('Creating unique key field.')  
    for key in self.configs.keys():
        unique_dic = self.configs[key]['create_unique_key']
        if unique_dic != '':
            print('Working on: ' + key)
            path = os.path.join(self.new_dir,key+'_WKT.csv')
            createUniqueKeyField(path,unique_dic)
            print('Complete.')


def mergeRowsAllFiles(self):
    print('Merging row data for unique key.')  
    for key in self.configs.keys():
        merge_rows = self.configs[key]['merge_rows']
        if merge_rows != '':
            print('Working on: ' + key)
            path = os.path.join(self.new_dir,key+'_WKT.csv')
            mergeRows(path,merge_rows)
            print('Complete.')


def buildRowMerge(line,merge_rows):
    extras = merge_rows['extra']
    val = line[merge_rows['val_index']]
    if extras == "":
        return val
    else:
        if extras['name'] == "percent":
            e_val = line[extras['index']]
            return val + ' (' + e_val + '%)'
        else:
            print('Error: check extras config.')


def mergeRows(path,merge_rows):
    with open(path,'r') as t1:
        reader = csv.reader(t1)
        next(reader)
        owner_dic = {}

        for line in reader:
            _id = line[merge_rows['id_index']]
            if _id in owner_dic.keys():
                owner_dic[_id] = owner_dic[_id] + '; ' + buildRowMerge(line,merge_rows)
            else:
                owner_dic[_id] = buildRowMerge(line,merge_rows)


    with open(path,'r') as t2:
        reader = csv.reader(t2)
        headers = next(reader)
        lst = []
        lst.append(headers) 

        ids = []
        for line in reader:
            _id = line[merge_rows['id_index']]
            if _id not in ids:
                ids.append(_id)
                line[merge_rows['val_index']] = owner_dic[_id]
                if merge_rows['extra'] != "":
                    line[merge_rows['extra']['index']] = 0
                lst.append(line)

    writeToFile(path, lst)

    print('Complete.')


def findHighestIdentifier(self):
    high_value = 0
    for file in os.listdir(self.core_dir):
        if self.configs[file.replace("_WKT.csv","")]["unique_column"] != "":
            data = pd.read_csv(os.path.join(self.new_dir,file), header=0, low_memory=False)
            if len(data.index) > 0:
                frequencies = Counter(list(data.NEW_ID))
                file_high = max(frequencies, key=frequencies.get)
                if file_high > high_value:
                    high_value = file_high
            else:
                high_value = 0
    return high_value


# def createChangeFiles(self):
#     core_files = os.listdir(self.core_dir)
#     # check if files exist in core
#     if len(core_files) == 0:
#         print('No files in CORE directory... Creating CORE & CHANGE files from NEW directory.')
#         # do this if no files exist in the core directory
#         copyDirectory(self.new_dir,self.core_dir)
#         copyDirectory(self.new_dir,self.change_dir)
#     else:
#         # compare core to new. Creates the change and update files
#         compareCoreToNew(self)
#     #     # update core files with changes from change files
#     #     commitChangesToCore(self)
#     # # remove WKT field
#     # if self.data_group == 'occurrence':
#     #     # clearDirectory(self.vba_directory)
#     #     copyDirectory(self.change_dir,self.vba_dir)
#     # else:
#     #     copyDirectoryDropColumn(self.change_dir,self.vba_dir,0)
#     #     copyDirectoryDropColumn(self.core_dir,self.plain_dir,0)

def createChangeFiles(self):
    core_files = os.listdir(self.core_dir)
    # check if files exist in core
    if len(core_files) == 0:
        print('No files in CORE directory... Creating CORE & CHANGE files from NEW directory.')
        # do this if no files exist in the core directory
        copyDirectory(self.new_dir,self.core_dir)
        copyDirectory(self.new_dir,self.change_dir)
    else:
        # compare core to new. Creates the change and update files
        createUpdateFile_updateCore(self)
    # # remove WKT field
    if self.data_group == 'occurrence':
        # clearDirectory(self.vba_directory)
        copyDirectory(self.change_dir,self.vba_dir)
    else:
        copyDirectoryDropColumn(self.change_dir,self.vba_dir,0)
        copyDirectoryDropColumn(self.core_dir,self.plain_dir,0)

def singleColumnDfToList(df):
    return [line[0] for line in df.values.tolist()]



def createUpdateFile_updateCore(self):
    print('Comparing the new and core files to build the change files and update file for the %s data group.'%(self.data_group))
    high_value = findHighestIdentifier(self)

    new_files = os.listdir(self.new_dir)
    core_files = os.listdir(self.core_dir)

    merged_file_lst = getMergedFiles(self)

    update_path = os.path.join(self.update_dir,'update.csv')

    # create the update_list
    update_lst = []
    headers = ['NEW_ID','ACTION','FILE','IDENTIFIER']
    update_lst.append(headers)

    for file in new_files:
        # if file[:-8] in ['NT_1']:
        # if "NT_" in file:
        if file.replace('_WKT.csv',"") not in merged_file_lst:
            if file in core_files:
                dic_key = file[:-8]
                print('Starting: %s' %(file))

                new_path = os.path.join(self.new_dir,file)
                core_path = os.path.join(self.core_dir,file)
                change_path = os.path.join(self.change_dir,file)
                required_fields = self.configs[dic_key]['required_fields']

                # read the file to df
                new_df = pd.read_csv(new_path,low_memory=False)
                core_df = pd.read_csv(core_path,low_memory=False)
                
                # create df with only the required fields from configs. this will help find all differences between the new and core file
                new_required_df = new_df[required_fields]
                core_required_df = core_df[required_fields]
                # print(new_required_df.head())
                # print(core_required_df.head())
                # create df with only the identifier column. This will help find the add and remove ids
                new_index_df = new_df[["NEW_IDENTIFIER"]]
                core_index_df = core_df[["NEW_IDENTIFIER"]]

                merge_df = core_required_df.merge(new_required_df,indicator=True,how='outer')

                diff_df = merge_df[merge_df["_merge"] != "both"]

                id_merge_df = core_index_df.merge(new_index_df,indicator=True,how='outer')
                remove_lst = singleColumnDfToList(id_merge_df[id_merge_df["_merge"] == "left_only"])
                add_lst = singleColumnDfToList(id_merge_df[id_merge_df["_merge"] == "right_only"])
                # print(len(remove_lst))
                # print(len(add_lst))
                none_change_lst = remove_lst + add_lst

                # remove the dups by converting into a set and back again
                diff_lst = list(set(singleColumnDfToList(diff_df[["NEW_IDENTIFIER"]])))
                # print(len(diff_lst))

                # find all the ids to remove. if its not in the remove or add list then it has to be a remove item
                change_lst = [ind for ind in diff_lst if not ind in none_change_lst]
                # print(change_lst)

                # use this to see the differences between the two df. each file will have it's own check file saved in the update folder
                check_new_df = new_required_df[new_required_df["NEW_IDENTIFIER"].isin(change_lst)]
                check_core_df = core_required_df[core_required_df["NEW_IDENTIFIER"].isin(change_lst)]
                check_df = pd.concat((check_core_df,check_new_df),ignore_index=True)
                check_df.sort_values(by=["NEW_IDENTIFIER"],inplace=True)
                if len(check_df.index) > 0:
                    check_df.to_csv(os.path.join(self.update_dir,dic_key + '_check.csv'),index=False)


                # create a dictonary of the identifiers from the remove and change list as key and its gplore id found in the core_df. 
                remove_change_lst = change_lst + remove_lst
                index_lookup_df = core_df[["NEW_IDENTIFIER","NEW_ID"]]
                index_lookup_lst = index_lookup_df[index_lookup_df["NEW_IDENTIFIER"].isin(remove_change_lst)].values.tolist()
                dic = {group[0]:group[1] for group in index_lookup_lst}
                # print(dic)

                # adds to the update_lst and builds the temp_lst of the differences between the core and new files. This will be uses to create the change file
                # and make changes in the core file.
                temp_lst = []
                for entry in [[add_lst,"ADD"],[remove_lst,"REMOVE"],[change_lst,"CHANGE"]]:
                    for row in entry[0]:
                        action = entry[1]
                        if action == "ADD":
                            high_value += 1
                            g_id = high_value
                        else:
                            g_id = dic[row]
                        data = [g_id,action,file,row]
                        temp_lst.append(data)
                        update_lst.append(data)
                # print(temp_lst)

                # correct NEW_IDs in the new file, 
                if len(temp_lst) > 0:
                    fileupdate_df = pd.DataFrame(temp_lst,columns=headers)
                    add_df = fileupdate_df[fileupdate_df["ACTION"] == "ADD"]
                    # correct the NEW_IDs in the new file
                    if len(add_df.index) > 0:
                        # filters the new_df by the identifier values, get the NEW_IDs which will be replaced by the correct NEW_IDs from the update file
                        newid_lst = singleColumnDfToList(add_df[["NEW_ID"]])
                        identifier_lst = singleColumnDfToList(add_df[["IDENTIFIER"]])
                        oldid_lst = new_df[new_df["NEW_IDENTIFIER"].isin(identifier_lst)]["NEW_ID"].values.tolist()
                        # replace the old NEW_IDs with the NEW_IDs
                        new_df['NEW_ID'] = new_df['NEW_ID'].replace(oldid_lst,newid_lst)


                    # create the change file. filter for ADD and CHANGE values in the new file
                    change_ident_lst = fileupdate_df[fileupdate_df["ACTION"].isin(["ADD","CHANGE"])]["IDENTIFIER"].values.tolist()
                    change_df = new_df[new_df["NEW_IDENTIFIER"].isin(change_ident_lst)]
                    change_df.to_csv(change_path,index=False) # write to file

                    # drop all CHANGE, ADD, REMOVE update values in the core file and concatenate the change file
                    update_ident_lst = fileupdate_df[fileupdate_df["ACTION"].isin(["REMOVE","CHANGE"])]["IDENTIFIER"].values.tolist()
                    core_df.set_index("NEW_IDENTIFIER",drop=False,inplace=True)
                    core_df.drop(update_ident_lst,inplace=True)
                    new_core_df = pd.concat((core_df,change_df),ignore_index=True)
                    new_core_df.to_csv(core_path,index=False)

    print('Complete.')

    writeToFile(update_path, update_lst)






# def compareCoreToNew(self):
#     print('Comparing the new files to the core files and building the change and update files.')
#     high_value = findHighestIdentifier(self)

#     new_files = os.listdir(self.new_dir)
#     core_files = os.listdir(self.core_dir)

#     merged_file_lst = getMergedFiles(self)

#     # create the update_list
#     update_list = []
#     update_headers = ['NEW_ID','ACTION','FILE','IDENTIFIER']
#     update_list.append(update_headers)

#     for file in new_files:
#         if file == 'NT_2_WKT.csv':
#         # if "SA_" in file:
#             if file.replace('_WKT.csv',"") not in merged_file_lst:
#                 if file in core_files:
#                     dic_key = file[:-8]
#                     if self.configs[dic_key]['unique_column'] != "":
#                         print('Starting: %s' %(file))

#                         new_path = os.path.join(self.new_dir,file)
#                         core_path = os.path.join(self.core_dir,file)
#                         change_path = os.path.join(self.change_dir,file)
#                         update_path = os.path.join(self.update_dir,'update.csv')
#                         unrequired_fields = self.configs[dic_key]['unrequired_fields']

#                         with open(new_path, 'r') as t1, open(core_path, 'r') as t2:
#                             new_lst_dic = convertToDic(csv.reader(t1),'last')
#                             core_lst_dic = convertToDic(csv.reader(t2),'last')

#                             # print(core_lst_dic)

#                         # read files, get headers, create list of core_path identifiers
#                         with open(new_path, 'r') as t1, open(core_path, 'r') as t2, open(change_path, 'w') as t4:
                            
#                             new_lst = dropLastColumn(csv.reader(t1))
#                             core_lst = dropLastColumn(csv.reader(t2))

#                             new_reader = removeUnrequiredFields(new_lst,unrequired_fields)
#                             core_reader = removeUnrequiredFields(core_lst,unrequired_fields)

#                             headers = core_lst[0]
#                             headers.append('NEW_ID')
#                             core_reader.pop(0)
#                             new_reader.pop(0)

#                             # dictionary of unique identifiers and gplore ids
#                             core_dic = {}
#                             core_df = pd.read_csv(core_path, header=0)
#                             for ind in core_df.index:
#                                 # print("%s - %s"%(str(core_df['NEW_IDENTIFIER'][ind]),core_df['NEW_ID'][ind]))
#                                 core_dic[str(core_df['NEW_IDENTIFIER'][ind])] = core_df['NEW_ID'][ind]

#                             # create list for db actions, and list for data to be updated in excel
#                             change_all = []
#                             change_all.append(headers)
#                             count = 0
#                             new_identifiers = []
#                             for line in new_reader:
#                                 identifier = str(line[-1])
#                                 new_identifiers.append(str(identifier))
#                                 if line not in core_reader:
#                                     count += 1
#                                     if str(identifier) in core_lst_dic:
#                                         g_id = core_dic[str(identifier)]
#                                         update_list.append([g_id,'CHANGE',file,identifier])
#                                         # # use the next 6 lines to print the comparing lines after one another
#                                         # print(line)
#                                         # select_line = []
#                                         # for i, item in enumerate(core_lst_dic[identifier]):
#                                         #     if i not in unrequired_fields:
#                                         #         select_line.append(item)
#                                         # print(select_line)
#                                     else:
#                                         high_value += 1
#                                         g_id = high_value
#                                         update_list.append([g_id,'NEW',file,identifier])
#                                     original_line = new_lst_dic[str(identifier)]
#                                     original_line.append(g_id)
#                                     change_all.append(original_line)


#                             for line in core_reader:
#                                 identifier = str(line[-1])
#                                 if str(identifier) not in new_identifiers:
#                                     print(identifier)
#                                     g_id = core_dic[str(identifier)]
#                                     print('done')
#                                     update_list.append([g_id,'REMOVE',file,identifier])
#                                     line.append(g_id)
#                                     # change_all.append(line)

#                             # write changes to file
#                             change_writer = csv.writer(t4, lineterminator='\n')
#                             change_writer.writerows(change_all)

#                         # if no chnages were added, delete the file
#                         if count == 0:
#                             os.remove(change_path)
#     print('Complete.')

#     writeToFile(update_path, update_list)


def sortMultipleValuesString(self):
    configs = self.configs
    for key in configs.keys():
        if configs[key]['sort_values'] != "":
            print("Sorting values for " + key)
            separator = configs[key]['sort_values']['separator']
            ind = configs[key]['sort_values']['index']
            file_path = os.path.join(self.new_dir,key+'_WKT.csv')
            with open(file_path) as t1:
                new_file = csv.reader(t1)
                lst = []
                lst.append(next(new_file))
                for line in new_file:
                    splt = [x.strip() for x in line[ind].split(separator)]
                    if len(splt) > 1:
                        new_val = '; '.join(sorted(splt))
                        line[ind] = new_val
                    lst.append(line)
            writeToFile(file_path, lst)
    print('Complete!')



def getMergedFiles(self):
    lst = []
    configs = self.configs
    for key in configs.keys():
        if configs[key]['merge_file'] != "":
            lst.append(configs[key]['merge_file']['file'])
    return lst
        

# def commitChangesToCore(self):
#     print('Commit the changes to the core file.')
#     change_files = os.listdir(self.change_dir)

#     # create a dictionary for update events
#     update_dic = {}
#     with open(self.update_path, 'r') as t1:
#         update_reader = csv.reader(t1)
#         next(update_reader)
#         for line in update_reader:
#             update_dic[line[0]] = line[1]


#     for file in change_files:
#         print('Adding changes: %s' %(file))
#         change_path = os.path.join(self.change_dir,file)
#         core_path = os.path.join(self.core_dir,file)
        
#         # create a dictionary of the change file with gplore_id as the key
#         with open(change_path, 'r') as t1:
#             change_dic = {}
#             change_reader = csv.reader(t1)
#             next(change_reader)
#             for line in change_reader:
#                 change_dic[line[-1]] = line
            

#         # loop though core file and add changes
#         with open(core_path, 'r') as t1:
#             core_reader = csv.reader(t1)
#             core = []
#             headers = next(core_reader)
#             core.append(headers)

#             for line in core_reader:
#                 if line[-1] not in update_dic.keys():
#                     core.append(line)
#                 else:
#                     action = update_dic[line[-1]]
#                     if action == 'CHANGE':
#                         core.append(change_dic[line[-1]])


#         # loop through change_dic and add the new entries
#         for key in change_dic:
#             action = update_dic[key]
#             if action == 'NEW':
#                 core.append(change_dic[key])


#         writeToFile(core_path, core)
#         print('Complete')



def createUniqueKeyField(file_path,unique_dic):
    print('Creating Unique index field.')
    with open(file_path, 'r') as t1:
        reader = csv.reader(t1)
        lst = []
        unique_keys = []
        headers = next(reader)
        headers.append('UNIQUE_FIELD')
        lst.append(headers)
        if unique_dic['name'] == 'add_xy':
            for line in reader:
                index_lst = unique_dic['index']
                line.append(int(float(line[index_lst[0]])) + int(float(line[index_lst[1]])))
                lst.append(line)
        elif unique_dic['name'] == 'add_multipolygon':
            for line in reader:
                wkt = line[0]
                points = wkt.replace("MULTIPOLYGON ","").replace("(","").replace(")","").split(",")
                valSum = 0
                for pnt in points:
                    valSum += float(pnt.strip().split(" ")[0])
                line.append("C%s" %(str(valSum).replace(".","")[-0:8]))
                lst.append(line)
    writeToFile(file_path, lst)
    print('Complete.')


def findNewValuesUpdateCoreAndDb(self):
    configs = self.configs
    for file_name in configs:
        # if file_name == 'OccName':
        if configs[file_name]['core_file']:
            # print(file_name)
            new_file = "%s%s.csv"%(self.new_dir,file_name)
            core_file = "%s%s.csv"%(self.core_dir,file_name)
            new_df = pd.read_csv(new_file,engine='python')
            core_df = pd.read_csv(core_file,engine='python')
            if configs[file_name]['iteration_index']:
                next_index = max(core_df[core_df.columns[0]])
                core_index_df = core_df[core_df.columns[0]]
                new_df.drop(new_df.columns[0],axis=1,inplace=True)
                core_df.drop(core_df.columns[0],axis=1,inplace=True)
            comparison_df = core_df.merge(new_df,indicator=True,how='outer')
            toAdd_df = comparison_df[comparison_df['_merge'] == 'right_only'].drop(columns=['_merge'])
            if len(toAdd_df.index) != 0:
                if configs[file_name]['iteration_index']:
                    new_index_df = pd.DataFrame(list(range(next_index,next_index + len(toAdd_df.index))))
                    index_df = pd.concat((core_index_df,new_index_df),ignore_index=True)
                    core_df = pd.concat((core_df,toAdd_df),ignore_index=True)
                    core_df.insert(0,'_ID',index_df)
                else:
                    core_df = pd.concat((core_df,toAdd_df),ignore_index=True)
                # print(core_df)
                core_df.to_csv(core_file,index=False)
            # # I still need to add these to the db


def readAndDropNecessaryColumnsDf(file_lst,drop_index_lst):
    df1, df2 = readMultipledf(file_lst)
    df1, df2 = dropMultipleColumnsdf([df1, df2],drop_index_lst)
    return df1, df2


def initLstsCoreToNew():
    changes_lst = []
    changes_lst.append(['TYPE','GROUP','TABLE','KEY_VALUE','CHANGE_FIELD','VALUE'])
    update_lst = []
    update_lst.append(['TYPE','GROUP','KEY_VALUE'])
    return update_lst, changes_lst


def differenceBetweenDataframes(df1, df2):
    df = pd.concat([df1, df2])
    df = df.reset_index(drop=True)
    df_gpby = df.groupby(list(df.columns))
    split_index = len(df1.index)
    idx_core = [x[0] for x in df_gpby.groups.values() if len(x) == 1 if x[0] >= split_index]
    idx_new = [x[0] for x in df_gpby.groups.values() if len(x) == 1 if x[0] < split_index]
    df_core = df.reindex(idx_core)
    df_new = df.reindex(idx_new)
    return (df_core,df_new)


def recordDifferenceInDfs(self,iter_df, lookup_df, changes_lst, update_lst, index_fields, file_name, typ=""):
    headers = list(iter_df.columns)
    index_found = False
    # loop through new and find same id, then find the differences and record them to update_lst and changes_lst
    for i, line in iter_df.iterrows():
        # There can be multiple index values, incase one of the indexes have been changed
        for index_field in index_fields:
            if index_found: # If the first index is found, don't worry about the second
                break
            update_lst, changes_lst, index_found = compareTwoRowsByHeader(line,lookup_df,index_field,headers,file_name,update_lst, changes_lst)

        # if the new index is not found in the core dataframe, then add to update_lst as Add
        if typ == "CHANGE":
            val = "NEW"
        else:
            val = "REMOVE"
        if not index_found:
            index_field = index_fields[0]
            index = line[index_field]
            update_lst.append([val,file_name,index_field,index])

    return changes_lst, update_lst


def getFullFileChanges(self,update_lst,changes_lst,new_df,core_df,index_fields,drop_index,file_name):
    df_core, df_new = differenceBetweenDataframes(new_df, core_df)
    changes_lst, update_lst = recordDifferenceInDfs(self,df_new, df_core, changes_lst, update_lst, index_fields, file_name, typ='CHANGE')
    return update_lst, changes_lst


def getByIndexChanges(self,update_lst,changes_lst,new_df,core_df,index_fields,drop_index,file_name):
    headers = list(core_df.columns)
    for i, line in new_df.iterrows():
        update_lst, changes_lst, index_found = compareTwoRowsByHeader(line,core_df,index_fields,headers,file_name,update_lst, changes_lst)
    return update_lst, changes_lst


def compareTwoRowsByHeader(new_series,lookup_df,index_field,headers,file_name,update_lst,changes_lst):
    index = new_series[index_field]
    core_row = lookup_df[lookup_df[index_field] == index]
    index_found = False
    if len(core_row) != 0: # if greater than 0 then the index exists in the new dataframe. continue to find what values have changed.
        index_found = True
        for header in headers:
            core_val = core_row[header].values[0]
            new_val = new_series[header]
            if new_val != core_val:
                changes_lst.append([file_name,index_field,index,header,core_val,new_val])
                update_lst.append(['CHANGE',file_name,index_field,index])
    return update_lst, changes_lst, index_found


def getValExistsChanges(self,update_lst,changes_lst,new_df,core_df,index_fields,drop_index,file_name):
    comparison_df = new_df.merge(core_df,indicator=True,how='outer')
    diff_lst = comparison_df[comparison_df['_merge'] == 'left_only'][index_fields].tolist()
    for item in diff_lst:
        update_lst.append(['NEW',file_name,index_fields,item])
    return update_lst, changes_lst


def removeDataFromCoreFiles(self,update_lst):

    update_df = pd.DataFrame (update_lst[1:],columns=update_lst[0])

    removedf = update_df[update_df['TYPE'] == "REMOVE"]
    for data_group in ['tenement','occurrence']:
        remove_lst = removedf[removedf['TABLE'] == data_group]['KEY_VALUE'].tolist()
        configs = self.configs
        for file_name in configs:
            if configs[file_name]['group'] == data_group:
                core_path = "%s%s.csv" %(self.core_dir,file_name)
                df = pd.read_csv(core_path)
                new_df = df[~df[configs[file_name]['direct_relation']['index']].isin(remove_lst)]
                # new_df.to_csv(core_path, index=False)


def applyChangesToCoreFiles(self,changes_lst):
    # ['TABLE','KEY_FIELD','KEY_VALUE','CHANGE_FIELD','OLD_VALUE','NEW_VALUE']

    change_df = pd.DataFrame (changes_lst[1:],columns=changes_lst[0])

    configs = self.configs

    for file_name in configs:
        core_path = "%s%s.csv" %(self.core_dir,file_name)
        df = change_df[change_df['TABLE'] == file_name]
        if len(df.values.tolist()) != 0:
            core_df = pd.read_csv(core_path)
            for i, line in df.iterrows():
                core_df.set_index(line['KEY_FIELD'],drop=False)
                core_df[line['KEY_VALUE']][line['CHANGE_FIELD']] = core_df[line['KEY_VALUE']][line['CHANGE_FIELD']].replace(line['OLD_VALUE'],line['NEW_VALUE'])
    
        # core_df.to_csv(core_path, index=False)


def addNewToCoreFiles(self,file_name,update_lst):
    update_core_index = self.configs[file_name]['update_core_index']

    core_path = "%s%s.csv" %(self.core_dir,file_name)
    new_path = "%s%s.csv" %(self.new_dir,file_name)

    core_df = pd.read_csv(core_path,engine='python')
    new_df = pd.read_csv(new_path,engine='python')

    # # this attempts to get the update values from the update sheet before they are created.
    # update_df = pd.DataFrame (update_lst[1:],columns=update_lst[0])
    # new_lst = update_df[(update_df['TABLE'] == file_name) & (update_df['TYPE'] == 'CHANGE')].drop(['TYPE','TABLE'],axis=1).values.tolist()


    if update_core_index != "":
        max_index = core_df[update_core_index].max()
        # print(max_index)

    for row in new_lst:
        new_line = new_df[new_df[row[0]] == row[1]]
        # print(new_line)
        if update_core_index != "":
            max_index += 1
            new_line[0][update_core_index] = max_index
        core_df = core_df.append(new_line, ignore_index=True)
        
    # core_df.to_csv(core_path, index=False)
