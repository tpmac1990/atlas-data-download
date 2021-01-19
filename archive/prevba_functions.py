import os
import sys
import csv
import ctypes
import pandas as pd
import shutil
import datetime
from collections import Counter

from functions import *


# def getDataGroupSubDirectory(data_group,directory_name):
#     root_directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
#     tDate = datetime.datetime.now().strftime("%y%m%d")
#     data_group_directory = "%s%s/" %(root_directory,data_group)
#     dic = {
#         "core_directory": data_group_directory + 'core/',
#         "plain_directory": data_group_directory + 'plain/',
#         "change_directory": data_group_directory + 'change/',
#         "new_directory": data_group_directory + 'new/',
#         "update_directory": data_group_directory + 'update/',
#         "root_archive_directory": data_group_directory + 'archive/',
#         "vba_directory": data_group_directory + 'vba/',
#         "today_archive_directory": "%sarchive/%s/" %(data_group_directory,tDate),
#         "change_archive_directory": "%sarchive/%s/change/" %(data_group_directory,tDate),
#         "core_archive_directory": "%sarchive/%s/core/" %(data_group_directory,tDate),
#         "update_archive_directory": "%sarchive/%s/update/" %(data_group_directory,tDate),
#         "update_path": data_group_directory + 'update/update.csv',
#     }


def combinePolygonsWithSameID_VIC(self):
    if self.data_group == 'tenement':

        files = ["VIC_1_WKT","VIC_2_WKT"] 

        for file in files:
            print("Combining polygon data of duplicate ID's in %s" %(file))       

            file_path = self.new_directory + file + '.csv'

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

        file_path = self.new_directory + fInput + '.csv'

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
    # print(self.configs)

    i = 1000000

    for file in os.listdir(self.new_directory):
        # print(self.configs[file.replace('_WKT.csv','')]['unique_column'])
        if file.endswith("_WKT.csv"):
            # fName = file.split(".")[0]
            fName = file.replace('_WKT.csv','')
            unique_col = self.configs[fName]['unique_column']
            if unique_col != "":
                # if fName == "NSW_1":
                print("Adding NEW_IDENTIFIER and NEW_ID fields for %s" %(file))
                file_path = self.new_directory + file
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
    file_path = directory + 'WA_5_WKT.csv'

    with open(file_path,'r') as t1:
        reader = csv.reader(t1)

        all = []
        for line in reader:
            if len(line[5].strip()) != 0:
                all.append(line)

    writeToFile(file_path, all)

    print('complete')



# def deleteAgentEntriesNT(directory):
#     print('deleting agent row in NT datasets.')
#     file_lst = [
#         { 'name': 'NT_1_WKT.csv', 'col': 14, 'val': 'C' },HLD_TY_CD
#         { 'name': 'NT_2_WKT.csv', 'col': 13, 'val': 'A' },HLD_TY_CD
#         { 'name': 'NT_4_WKT.csv', 'col': 13, 'val': 'C' },HLD_TY_CD
#         { 'name': 'NT_5_WKT.csv', 'col': 14, 'val': 'C' },HLD_TY_CD
#         { 'name': 'NT_6_WKT.csv', 'col': 13, 'val': 'A' },HLD_TY_CD
#     ]

#     for group in file_lst:
#         print('Working on: %s' %(group['name']))
#         file_path = directory + group['name']

#         with open(file_path,'r') as t3:
#             reader = csv.reader(t3)
#             headers = next(reader)
#             all = []
#             all.append(headers) 

#             for line in reader:
#                 if line[group['col']] == group['val']:
#                     all.append(line)

#         writeToFile(file_path, all)
#         print('complete')



# def OSAusDeleteNSWSA(directory):
#     print('removing NSW & SA entries. They exist in the state datasets')

#     file_path = directory + 'AUS_OSPET_1_WKT.csv'

#     states = ["Territory of Ashmore and Cartier Islands", "Victoria", "Northern Territory", "Queensland", "Tasmania", "Western Australia"]

#     with open(file_path,'r') as t1:
#         reader = csv.reader(t1)
#         headers = next(reader)
#         all = []
#         all.append(headers) 

#         for line in reader:
#             if line[5] in states:
#                 all.append(line)

#     writeToFile(file_path, all)
#     print('Complete')


# def buildNTHolderPerc(holder,percent):
#     new_holder = holder + ' (' + percent + '%)'
#     return new_holder



def deletingInvalidWktRowsAllFiles(self):
    print('Deleting rows with invalid WKT.')
    new_files = os.listdir(self.new_directory)

    for file in new_files:
        print('Working on: %s' %(file))
        file_path = self.new_directory + file

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



def removeUnrequiredFields(reader,unrequired_fields):
    all = []
    for line in reader:
        row = []
        for ind, elem in enumerate(line):
            if ind not in unrequired_fields:
                row.append(elem)
        # row[0] = createWKTIdentifier(row[0])
        all.append(row)
    return all


def copyDirectoryDropColumn(src_directory,dest_directory,index_column):
        print('Dropping column %s from files.' %(index_column))
        src_files = os.listdir(src_directory)

        for file in src_files:
            print('Working on: %s' %(file))
            destination_path = dest_directory + file.replace("_WKT","")
            src_path = src_directory + file
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
        src_directory = self.data_group_directory + name + '/'
        dest_directory = self.archive_directory + name + '/'
        if name in archive_lst:
            copyDirectory(src_directory,dest_directory)
        if name in delete_lst:
            clearDirectory(src_directory)
    print('Complete.')

def archiveRemoveOutputFiles(self):
    print('Archiving last output files.')
    name_lst = ['change','core','new','update']
    archive_lst = ['change','core','update']
    delete_lst = ['change','update','new']
    for name in name_lst:
        src_directory = self.output_directory + name + '/'
        dest_directory = self.output_archive_directory + name + '/'
        if name in archive_lst:
            copyDirectory(src_directory,dest_directory)
        if name in delete_lst:
            clearDirectory(src_directory,extension=".csv")
    print('Complete.')


def combineSameNameWellsAusOS(self):
    if self.data_group == 'occurrence':
        file_path = self.new_directory + "AUS_OSPET_1_WKT.csv"
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
            path = "%s%s_WKT.csv" %(self.new_directory,key)
            filterRelevantData(path,filter_vals_lst)
    print('Complete.')            


def removeDuplicateRowsByKeyAllFiles(self): 
    print('Removing duplicate rows of data.')  
    for key in self.configs.keys():
        index = self.configs[key]['duplicate_rows_key']
        if index != '':
            print('Working on: ' + key)
            path = "%s%s_WKT.csv" %(self.new_directory,key)
            removeDuplicateRowsByKey(path,index)
            print('Complete.')


def combineFilesAllFiles(self):
    print('Combininig relevant files.')  
    for key in self.configs.keys():
        merge_file = self.configs[key]['merge_file']
        if merge_file != '':
            print('Working on: ' + key)
            file_path = "%s%s_WKT.csv"%(self.new_directory,key)
            with open("%s%s_WKT.csv"%(self.new_directory,merge_file['file']), 'r') as t1:
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
            path = "%s%s_WKT.csv" %(self.new_directory,key)
            createUniqueKeyField(path,unique_dic)
            print('Complete.')


def mergeRowsAllFiles(self):
    print('Merging row data for unique key.')  
    for key in self.configs.keys():
        merge_rows = self.configs[key]['merge_rows']
        if merge_rows != '':
            print('Working on: ' + key)
            path = "%s%s_WKT.csv" %(self.new_directory,key)
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



# def mergeOwnersNT(directory):
#     print('NT: combining the owners into one line.')

#     file_lst = [
#         { 'name': 'NT_1_WKT.csv', 'holder': 12 },
#         { 'name': 'NT_2_WKT.csv', 'holder': 11 },
#         { 'name': 'NT_4_WKT.csv', 'holder': 11 },
#         { 'name': 'NT_5_WKT.csv', 'holder': 12 },
#         { 'name': 'NT_6_WKT.csv', 'holder': 11 },
#     ]

#     for group in file_lst:
#         print('Working on: %s' %(group['name']))
#         file_path = directory + group['name']

        # with open(file_path,'r') as t1:
        #     reader = csv.reader(t1)
        #     next(reader)
        #     owner_dic = {}

        #     for line in reader:
        #         title_id = line[1]
        #         holder = line[group['holder']]
        #         percent = line[group['holder'] + 1]
        #         if title_id in owner_dic.keys():
        #             owner_dic[title_id] = owner_dic[title_id] + '; ' + buildNTHolderPerc(holder,percent)
        #         else:
        #             owner_dic[title_id] = buildNTHolderPerc(holder,percent)


        # with open(file_path,'r') as t2:
        #     reader = csv.reader(t2)
        #     headers = next(reader)
        #     all = []
        #     all.append(headers) 

        #     ids = []
        #     for line in reader:
        #         title_id = line[1]
        #         if title_id not in ids:
        #             ids.append(title_id)
        #             line[group['holder']] = owner_dic[title_id]
        #             line[group['holder']+1] = 0
        #             all.append(line)

        # writeToFile(file_path, all)

        # print('Complete.')



def findHighestIdentifier(self):
    high_value = 0
    for file in os.listdir(self.core_directory):
        if self.configs[file.replace("_WKT.csv","")]["unique_column"] != "":
            data = pd.read_csv(self.new_directory + file, header=0, low_memory=False)
            if len(data.index) > 0:
                frequencies = Counter(list(data.NEW_ID))
                file_high = max(frequencies, key=frequencies.get)
                if file_high > high_value:
                    high_value = file_high
            else:
                high_value = 0
    return high_value


def createChangeFiles(self):
    core_files = os.listdir(self.core_directory)
    # check if files exist in core
    if len(core_files) == 0:
        print('No files in CORE directory... Creating CORE & CHANGE files from NEW directory.')
        # do this if no files exist in the core directory
        copyDirectory(self.new_directory,self.core_directory)
        copyDirectory(self.new_directory,self.change_directory)
    else:
        # compare core to new. Creates the change and update files
        compareCoreToNew(self)
        # update core files with changes from change files
        commitChangesToCore(self)
    # remove WKT field
    if self.data_group == 'occurrence':
        # clearDirectory(self.vba_directory)
        copyDirectory(self.change_directory,self.vba_directory)
    else:
        copyDirectoryDropColumn(self.change_directory,self.vba_directory,0)
        copyDirectoryDropColumn(self.core_directory,self.plain_directory,0)


def compareCoreToNew(self):
    print('Comparing the new files to the core files and building the change and update files.')
    high_value = findHighestIdentifier(self)

    new_files = os.listdir(self.new_directory)
    core_files = os.listdir(self.core_directory)

    merged_file_lst = getMergedFiles(self)

    # create the update_list
    update_list = []
    update_headers = ['NEW_ID','ACTION','FILE','IDENTIFIER']
    update_list.append(update_headers)

    for file in new_files:
        # if file == 'SA_1_WKT.csv':
        # if "SA_" in file:
        if file.replace('_WKT.csv',"") not in merged_file_lst:
            if file in core_files:
                dic_key = file[:-8]
                if self.configs[dic_key]['unique_column'] != "":
                    print('Starting: %s' %(file))

                    new_path = self.new_directory + file
                    core_path = self.core_directory + file
                    change_path = self.change_directory + file
                    update_path = self.update_directory + 'update.csv'
                    unrequired_fields = self.configs[dic_key]['unrequired_fields']

                    with open(new_path, 'r') as t1, open(core_path, 'r') as t2:
                        new_lst_dic = convertToDic(csv.reader(t1),'last')
                        core_lst_dic = convertToDic(csv.reader(t2),'last')

                    # read files, get headers, create list of core_path identifiers
                    with open(new_path, 'r') as t1, open(core_path, 'r') as t2, open(change_path, 'w') as t4:
                        
                        new_lst = dropLastColumn(csv.reader(t1))
                        core_lst = dropLastColumn(csv.reader(t2))

                        new_reader = removeUnrequiredFields(new_lst,unrequired_fields)
                        core_reader = removeUnrequiredFields(core_lst,unrequired_fields)

                        headers = core_lst[0]
                        headers.append('NEW_ID')
                        core_reader.pop(0)
                        new_reader.pop(0)

                        # dictionary of unique identifiers and gplore ids
                        core_dic = {}
                        core_df = pd.read_csv(core_path, header=0)
                        for ind in core_df.index:
                            core_dic[str(core_df['NEW_IDENTIFIER'][ind])] = core_df['NEW_ID'][ind]

                        # create list for db actions, and list for data to be updated in excel
                        change_all = []
                        change_all.append(headers)
                        count = 0
                        new_identifiers = []
                        for line in new_reader:
                            identifier = str(line[-1])
                            new_identifiers.append(str(identifier))
                            if line not in core_reader:
                                count += 1
                                if str(identifier) in core_lst_dic:
                                    g_id = core_dic[str(identifier)]
                                    update_list.append([g_id,'CHANGE',file,identifier])
                                    # # use the next 6 lines to print the comparing lines after one another
                                    # print(line)
                                    # select_line = []
                                    # for i, item in enumerate(core_lst_dic[identifier]):
                                    #     if i not in unrequired_fields:
                                    #         select_line.append(item)
                                    # print(select_line)
                                else:
                                    high_value += 1
                                    g_id = high_value
                                    update_list.append([g_id,'NEW',file,identifier])
                                original_line = new_lst_dic[str(identifier)]
                                original_line.append(g_id)
                                change_all.append(original_line)


                        for line in core_reader:
                            identifier = str(line[-1])
                            if str(identifier) not in new_identifiers:
                                g_id = core_dic[str(identifier)]
                                update_list.append([g_id,'REMOVE',file,identifier])
                                line.append(g_id)
                                # change_all.append(line)

                        # write changes to file
                        change_writer = csv.writer(t4, lineterminator='\n')
                        change_writer.writerows(change_all)

                    # if no chnages were added, delete the file
                    if count == 0:
                        os.remove(change_path)
    print('Complete.')

    writeToFile(update_path, update_list)


def sortMultipleValuesString(self):
    configs = self.configs
    for key in configs.keys():
        if configs[key]['sort_values'] != "":
            print("Sorting values for " + key)
            separator = configs[key]['sort_values']['separator']
            ind = configs[key]['sort_values']['index']
            file_path = "%s%s_WKT.csv"%(self.new_directory,key)
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
        

def commitChangesToCore(self):
    print('Commit the changes to the core file.')
    change_files = os.listdir(self.change_directory)

    # create a dictionary for update events
    update_dic = {}
    with open(self.update_path, 'r') as t1:
        update_reader = csv.reader(t1)
        next(update_reader)
        for line in update_reader:
            update_dic[line[0]] = line[1]


    for file in change_files:
        print('Adding changes: %s' %(file))
        change_path = self.change_directory + file
        core_path = self.core_directory + file
        
        # create a dictionary of the change file with gplore_id as the key
        with open(change_path, 'r') as t1:
            change_dic = {}
            change_reader = csv.reader(t1)
            next(change_reader)
            for line in change_reader:
                change_dic[line[-1]] = line
            

        # loop though core file and add changes
        with open(core_path, 'r') as t1:
            core_reader = csv.reader(t1)
            core = []
            headers = next(core_reader)
            core.append(headers)

            for line in core_reader:
                if line[-1] not in update_dic.keys():
                    core.append(line)
                else:
                    action = update_dic[line[-1]]
                    if action == 'CHANGE':
                        core.append(change_dic[line[-1]])


        # loop through change_dic and add the new entries
        for key in change_dic:
            action = update_dic[key]
            if action == 'NEW':
                core.append(change_dic[key])


        writeToFile(core_path, core)
        print('Complete')



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
                    valSum += float(pnt.split(" ")[0])
                line.append("C%s" %(str(valSum).replace(".","")[-0:8]))
                lst.append(line)
    writeToFile(file_path, lst)
    print('Complete.')


# def createOutputChangeFiles(self):
#     core_files = os.listdir(self.core_directory)
#     # check if files exist in core
#     if len(core_files) < 3:
#         print('No files in CORE directory... Creating CORE & CHANGE files from NEW directory.')
#         # do this if no files exist in the core directory
#         copyDirectory(self.new_directory,self.core_directory)
#         copyDirectory(self.new_directory,self.change_directory)
#     else:
#         # # create the update and change list
#         update_lst, changes_lst = initLstsCoreToNew()
#         update_lst, changes_lst = removeOldAddNewToCoreAndDb(self,update_lst)
#         # compare core to new. Creates the change and update files
#         changes_lst = compareOutputCoreToNew(self,changes_lst)

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

def findNewValuesUpdateCoreAndDb(self):
    configs = self.configs
    for file_name in configs:
        # if file_name == 'OccName':
        if configs[file_name]['core_file']:
            # print(file_name)
            new_file = "%s%s.csv"%(self.new_directory,file_name)
            core_file = "%s%s.csv"%(self.core_directory,file_name)
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



    # updates = {
    #     "tenement": [x[0] for x in tenement_update_df[tenement_update_df["ACTION"] == "REMOVE"].loc[:,["NEW_ID"]].values.tolist()],
    #     "occurrence": [x[0] for x in occurrence_update_df[occurrence_update_df["ACTION"] == "REMOVE"].loc[:,["NEW_ID"]].values.tolist()]
    # }
    # new_file = "%s%s.csv"%(self.new_directory,'Tenement')
    # core_file = "%s%s.csv"%(self.core_directory,'Tenement')
    # new_df = pd.read_csv(new_file,engine='python').loc[:,['TENID']]
    # core_df = pd.read_csv(core_file,engine='python').loc[:,['TENID']]
    # comparison_df = new_df.merge(core_df,indicator=True,how='outer')
    # new_ids_lst = [x[0] for x in comparison_df[comparison_df['_merge'] == 'left_only'].drop(columns=['_merge']).values.tolist()]
    # old_ids_lst = [x[0] for x in comparison_df[comparison_df['_merge'] == 'right_only'].drop(columns=['_merge']).values.tolist()]
    # print(len(old_ids_lst))
    # return update_lst





# def compareOutputCoreToNew(self):
#     print('Comparing the output new files to the output core files and building the change and update files.')
#     configs = self.configs
#     for file_name in configs:
#         # if file_name == 'OccName':
#         print(file_name)
#         if configs[file_name]['fix_val'] != "":
#             fixFileValues(self,file_name)
#         comparison_type = configs[file_name]['comparison_type']
#         index_fields = configs[file_name]['index']
#         drop_index = configs[file_name]['drop']
#         new_df, core_df = readAndDropNecessaryColumnsDf(["%s%s.csv"%(self.new_directory,file_name), "%s%s.csv"%(self.core_directory,file_name)],drop_index)
#         if comparison_type == "FULL_FILE":
#             update_lst, changes_lst = getFullFileChanges(self,update_lst,changes_lst,new_df,core_df,index_fields,drop_index,file_name)
#         elif comparison_type == "BY_INDEX":
#             update_lst, changes_lst = getByIndexChanges(self,update_lst,changes_lst,new_df,core_df,index_fields,drop_index,file_name)
#         elif comparison_type == "VAL_EXISTS":
#             update_lst, changes_lst = getValExistsChanges(self,update_lst,changes_lst,new_df,core_df,index_fields,drop_index,file_name)
#         else:
#             print("%s: does not exist"%(comparison_type))

#         addNewToCoreFiles(self,file_name,update_lst)
            
#     # applyChangesToCoreFiles(self,changes_lst)

#     # update_lst = addKeysToRemove(self,update_lst)
#     # removeDataFromCoreFiles(self,update_lst)

#     # writeToFile(self.update_directory + 'update.csv', update_lst)
#     # writeToFile(self.update_directory + 'change.csv', changes_lst)
#     print('Done!')


# def fixFileValues(self,file_name):
#     fix_val = self.configs[file_name]['fix_val']
#     # current new file with incorrect
#     this_new_file = "%s%s.csv"%(self.new_directory,file_name)
#     other_new_file = "%s%s.csv"%(self.new_directory,fix_val['other_file'])
#     core_file = "%s%s.csv"%(self.core_directory,fix_val['other_file'])

#     this_new_df = pd.read_csv(this_new_file).set_index(fix_val['this_field'],drop=False)
#     other_new_df = pd.read_csv(other_new_file).set_index(fix_val['other_index'],drop=False)
#     core_df = pd.read_csv(core_file).set_index(fix_val['other_field'],drop=False)

#     for i, line in this_new_df.iterrows():
#         # print(line)
#         val = other_new_df.loc[i]["NAME"]
#         actual_index = core_df.loc[val]["_ID"]
#         this_new_df.loc[i]["NAME"] = actual_index
#         # print(this_new_df.loc[i])

#     this_new_df.to_csv(this_new_file,index = False)



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


# def addKeysToRemove(self,update_lst):
#     for file in [self.tenement_update_path,self.occurrence_update_path]:
#         group = file.split('/')[-3]
#         data_group_df = pd.read_csv(file)
#         data_group_df = data_group_df[data_group_df["ACTION"] == "REMOVE"]
#         for i, line in data_group_df.iterrows():
#             update_lst.append(['REMOVE',group,'NEW_ID',line['NEW_ID']])
#     return update_lst


def removeDataFromCoreFiles(self,update_lst):

    update_df = pd.DataFrame (update_lst[1:],columns=update_lst[0])

    removedf = update_df[update_df['TYPE'] == "REMOVE"]
    for data_group in ['tenement','occurrence']:
        remove_lst = removedf[removedf['TABLE'] == data_group]['KEY_VALUE'].tolist()
        configs = self.configs
        for file_name in configs:
            if configs[file_name]['group'] == data_group:
                core_path = "%s%s.csv" %(self.core_directory,file_name)
                df = pd.read_csv(core_path)
                new_df = df[~df[configs[file_name]['direct_relation']['index']].isin(remove_lst)]
                # new_df.to_csv(core_path, index=False)


def applyChangesToCoreFiles(self,changes_lst):
    # ['TABLE','KEY_FIELD','KEY_VALUE','CHANGE_FIELD','OLD_VALUE','NEW_VALUE']

    change_df = pd.DataFrame (changes_lst[1:],columns=changes_lst[0])

    configs = self.configs

    for file_name in configs:
        core_path = "%s%s.csv" %(self.core_directory,file_name)
        df = change_df[change_df['TABLE'] == file_name]
        if len(df.values.tolist()) != 0:
            core_df = pd.read_csv(core_path)
            for i, line in df.iterrows():
                core_df.set_index(line['KEY_FIELD'],drop=False)
                core_df[line['KEY_VALUE']][line['CHANGE_FIELD']] = core_df[line['KEY_VALUE']][line['CHANGE_FIELD']].replace(line['OLD_VALUE'],line['NEW_VALUE'])
    
        # core_df.to_csv(core_path, index=False)


def addNewToCoreFiles(self,file_name,update_lst):
    update_core_index = self.configs[file_name]['update_core_index']

    core_path = "%s%s.csv" %(self.core_directory,file_name)
    new_path = "%s%s.csv" %(self.new_directory,file_name)

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
