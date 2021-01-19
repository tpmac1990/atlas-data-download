# This code just needs python to run.

# Add files to the directory C:\\Django_Projects\\03_geodjango\\Atlas\\datasets\\Raw_datasets\\Tenements with the following format:
#   <STATE>_<FILE-NO>_WKT
#   e.g NSW_1_WKT
#   Errors will occur if the files are not in this format


# moveOldRenameNewFiles:
#   1. Moves the previous OLD files to archive
#   2. Renames previous NEW files as OLD and deletes the files lacking WKT
#   3. Renames the newly created WKT files as NEW

# combinePolygonsWithSameID_VIC: 
#   1. Filter the dataset for active tenements
#   2. Clear all the duplicate ids by duplicate area values
#   3. Combines polygons of the same ID

# deleteSecondofDuplicate_QLD_1:
#   1. Delete the first row of duplicate ID's

# createIDFromWKT_QLD_3:
#   1. Create an ID using the sum of the point values from the WKT field.
#   2. An ID field is required to pair the WKT field back with its appropriate data after the Excel macros have been run.

# addIdentifierField:
#   1. Add New Identifier field and new id field. The Id field will be its unique id in the app, 
#       while the identifier will be used to link the tenement back with its WKT after formatting in excel(vba)

# createCsvWithNoWkt
#   1. creates a new csv without the WKT column so it can be used in excel(vba) macros.
#   2. The files is saved as the same name, less the _WKT.


import os
import sys
import csv
import ctypes
import pandas as pd
import shutil
import datetime
from collections import Counter
import json

from functions import *
from prevba_functions import *


class Functions():

    def __init__(self):
        csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
        root_directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
        self.tenement_directory = root_directory + 'tenement/'
        self.core_directory = self.tenement_directory + 'core/'
        self.plain_directory = self.tenement_directory + 'plain/'
        self.change_directory = self.tenement_directory + 'change/'
        self.new_directory = self.tenement_directory + 'new/'
        self.update_directory = self.tenement_directory + 'update/'
        self.archive_directory = self.tenement_directory + 'archive/'
        self.vba_directory = self.tenement_directory + 'vba/'
        # self.raw_csvs_directory = root_directory + 'download/data/output_csv_files/'
        self.tDate = datetime.datetime.now().strftime("%y%m%d")
        self.archive_directory = "%sarchive/%s/" %(self.tenement_directory,self.tDate)
        self.change_archive_directory = "%sarchive/%s/change/" %(self.tenement_directory,self.tDate)
        self.core_archive_directory = "%sarchive/%s/core/" %(self.tenement_directory,self.tDate)
        self.update_archive_directory = "%sarchive/%s/update/" %(self.tenement_directory,self.tDate)
        self.update_path = self.update_directory + 'update.csv'
        createDirectory(self.archive_directory)
        createDirectory(self.change_archive_directory)
        createDirectory(self.core_archive_directory)
        createDirectory(self.update_archive_directory)
        self.configs = getJSON(root_directory + 'scripts/config.json')['tenement']['Primary_Format']
        # with open(root_directory + 'scripts/tenement_format_config.json') as json_file:
        #     config_file = json.load(json_file)
        #     self.configs = config_file['INPUT_FILES']

        

    # def archiveRemoveOldFiles(self):
    #     print('Archiving last files.')
    #     name_lst = ['change','core','new','update','vba']
    #     archive_lst = ['change','core','update']
    #     delete_lst = ['change','update','vba','plain']
    #     for name in name_lst:
    #         directory = self.tenement_directory + name + '/'
    #         archive = self.archive_directory + name + '/'
    #         if name in archive_lst:
    #             copyDirectory(directory,archive)
    #         if name in delete_lst:
    #             clearDirectory(directory)
    #     print('Complete.')


    # def copyFilesFromDownload(self):
    #     print('Copying files from: %s' %(self.raw_csvs_directory))
    #     copyDirectory(self.raw_csvs_directory,self.new_directory)
    #     print('Files copied to: %s' %(self.new_directory))

    
    # def findHighestIdentifier(self):
    #     high_value = 0
    #     for file in os.listdir(self.core_directory):
    #         if file not in ['QLD_2_WKT.csv']:
    #             data = pd.read_csv(self.new_directory + file, header=0, low_memory=False)
    #             frequencies = Counter(list(data.NEW_ID))
    #             file_high = max(frequencies, key=frequencies.get)
    #             if file_high > high_value:
    #                 high_value = file_high
    #     return high_value


    def preformatTenementFiles(self):
        directory = self.new_directory
        combinePolygonsWithSameID_VIC(directory)
        deleteSecondofDuplicate_QLD_1(directory)
        createIDFromWKT_QLD_3(directory)
        deleteWA_5Blanks(directory)
        deleteAgentEntriesNT(directory)
        mergeOwnersNT(directory)
        deleteEmptyPolygonRows(directory)
        OSAusDeleteNSWSA(directory)
        addIdentifierField(directory,self.configs)



    # def createChangeFiles(self):
    #     core_files = os.listdir(self.core_directory)
    #     # check if files exist in core
    #     if len(core_files) == 0:
    #         print('No files in CORE directory... Creating CORE & CHANGE files from NEW directory.')
    #         # do this if no files exist in the core directory
    #         copyDirectory(src=self.new_directory,dest=self.core_directory)
    #         copyDirectory(src=self.new_directory,dest=self.change_directory)
    #     else:
    #         # compare core to new. Creates the change and update files
    #         self.compareCoreToNew()
    #         # update core files with changes from change files
    #         self.commitChangesToCore()
    #     # remove WKT field
    #     copyDirectoryDropColumn(self.change_directory,self.vba_directory,0)
    #     copyDirectoryDropColumn(self.core_directory,self.plain_directory,0)



    # def compareCoreToNew(self):
    #     print('Comparing the new files to the core files and building the change and update files.')
    #     high_value = self.findHighestIdentifier()

    #     new_files = os.listdir(self.new_directory)
    #     core_files = os.listdir(self.core_directory)

    #     # create the update_list
    #     update_list = []
    #     update_headers = ['NEW_ID','ACTION','FILE','IDENTIFIER']
    #     update_list.append(update_headers)

    #     for file in new_files:
    #         # if file == 'WA_2_WKT.csv':
    #         print('Starting: %s' %(file))
    #         if file in core_files:
    #             new_path = self.new_directory + file
    #             core_path = self.core_directory + file
    #             change_path = self.change_directory + file
    #             update_path = self.update_directory + 'update.csv'
    #             unrequired_fields = self.configs[file[:-4]]['unrequired_fields']


    #             with open(new_path, 'r') as t1, open(core_path, 'r') as t2:
    #                 new_lst_dic = convertToDic(csv.reader(t1),'last')
    #                 core_lst_dic = convertToDic(csv.reader(t2),'last')


    #             # read files, get headers, create list of core_path identifiers
    #             with open(new_path, 'r') as t1, open(core_path, 'r') as t2, open(change_path, 'w') as t4:
                    
    #                 new_lst = dropLastColumn(csv.reader(t1))
    #                 core_lst = dropLastColumn(csv.reader(t2))

    #                 new_reader = removeUnrequiredFields(new_lst,unrequired_fields)
    #                 core_reader = removeUnrequiredFields(core_lst,unrequired_fields)

    #                 headers = core_lst[0]
    #                 headers.append('NEW_ID')
    #                 core_reader.pop(0)
    #                 new_reader.pop(0)

    #                 # dictionary of unique identifiers and gplore ids
    #                 core_dic = {}
    #                 core_df = pd.read_csv(core_path, header=0)
    #                 for ind in core_df.index:
    #                     core_dic[str(core_df['NEW_IDENTIFIER'][ind])] = core_df['NEW_ID'][ind]

    #                 # create list for db actions, and list for data to be updated in excel
    #                 change_all = []
    #                 change_all.append(headers)
    #                 count = 0
    #                 new_identifiers = []
    #                 for line in new_reader:
    #                     identifier = str(line[-1])
    #                     new_identifiers.append(str(identifier))
    #                     if line not in core_reader:
    #                         count += 1
    #                         if str(identifier) in core_lst_dic:
    #                             g_id = core_dic[str(identifier)]
    #                             update_list.append([g_id,'CHANGE',file,identifier])
    #                         else:
    #                             high_value += 1
    #                             g_id = high_value
    #                             update_list.append([g_id,'NEW',file,identifier])
    #                         original_line = new_lst_dic[str(identifier)]
    #                         original_line.append(g_id)
    #                         change_all.append(original_line)


    #                 for line in core_reader:
    #                     identifier = str(line[-1])
    #                     if str(identifier) not in new_identifiers:
    #                         g_id = core_dic[str(identifier)]
    #                         update_list.append([g_id,'REMOVE',file,identifier])
    #                         line.append(g_id)
    #                         # change_all.append(line)

    #                 # write changes to file
    #                 change_writer = csv.writer(t4, lineterminator='\n')
    #                 change_writer.writerows(change_all)

    #             # if no chnages were added, delete the file
    #             if count == 0:
    #                 os.remove(change_path)
    #         print('Complete.')

    #     writeToFile(update_path, update_list)
        


    # def commitChangesToCore(self):
    #     print('Commit the changes to the core file.')
    #     change_files = os.listdir(self.change_directory)
    #     update_path = self.update_path

    #     # create a dictionary for update events
    #     update_dic = {}
    #     with open(update_path, 'r') as t1:
    #         update_reader = csv.reader(t1)
    #         next(update_reader)
    #         for line in update_reader:
    #             update_dic[line[0]] = line[1]


    #     for file in change_files:
    #         print('Adding changes: %s' %(file))
    #         change_path = self.change_directory + file
    #         core_path = self.core_directory + file
            
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




f = Functions()
f.archiveRemoveOldFiles()
f.preformatTenementFiles()
f.createChangeFiles()





# def clearFolders(self,folders):
#         for folder in folders:
#             directory = self.tenement_directory + folder
#             clearDirectory(directory)


#     if count == 0:
#         os.remove(change_path)
# new_file = open(new_path, 'r', encoding="utf-8")
# core_file = open(core_path, 'r', encoding="utf-8")
# new_dic = convertCsvToDic(new_file,self.unique_col_dic[file[0:-4]])
# new_file.close()
# core_file.close()
# new_path = self.tenement_directory + 'file1.csv'
# core_path = self.tenement_directory + 'file2.csv'
# df_new = pd.read_csv(self.new_directory + file)
# df_core = pd.read_csv(self.core_directory + file)
# comparison_df = df_new.merge(df_core,indicator=True,how='outer')
# diff_df = comparison_df[comparison_df['_merge'] != 'both']
# diff_df.to_csv(self.tenement_directory + 'diff.csv')
# new_file = open(new_path, 'r', encoding="utf-8")
# core_file = open(core_path, 'r', encoding="utf-8")
# new_dic = convertCsvToDic(new_file,self.unique_col_dic[file[0:-4]])
# new_file.close()
# core_file.close()




# # read files, get headers, create list of core_path identifiers
# with open(new_path, 'r') as t1, open(core_path, 'r') as t2, open(core_path, 'r') as t3:
#     new_file = dropLastColumn(t1.readlines())
#     core_file = dropLastColumn(t2.readlines())

#     headers = ','.join(next(csv.reader(t3))[:-1])
#     core_identifiers = list(pd.read_csv(core_path, header=0).NEW_IDENTIFIER)

#     core_dic = {}
#     core_df = pd.read_csv(core_path, header=0)
#     for ind in core_df.index:
#         core_dic[core_df['NEW_IDENTIFIER'][ind]] = core_df['NEW_ID'][ind]
#     # headers = 
#     # headers = ','.join(next(csv.reader(t3))).replace('NEW_ID','CHANGE') +  '\n'
    
# # create the update_list
# update_list = []
# update_headers = ['NEW_ID','ACTION']
# update_list.append(update_headers)


# with open(change_path, 'w') as outFile:
#     outFile.write(headers)
#     count = 0
#     new_identifiers = []
#     for line in new_file:
#         identifier = line[line.rfind(',', 0, len(line))+1:].strip()
#         new_identifiers.append(identifier)
#         if line not in core_file:
#             count += 1
#             if identifier in core_identifiers:
#                 g_id = core_dic[identifier]
#                 update_list.append([g_id,'CHANGE'])
#             else:
#                 high_value += 1
#                 g_id = high_value
#                 update_list.append([g_id,'NEW'])
#             outFile.write(line + ',' + str(g_id))

#     # for line in core_files:
#     #     identifier = line[line.rfind(',', 0, len(line))+1:].strip()
#     #     if identifier not in new_identifiers:
#     #         count += 1
#     #         g_id = core_dic[identifier]
#     #         update_list.append([g_id,'REMOVE'])

# if count == 0:
#     os.remove(change_path)

# with open(self.update_directory + 'update.csv','w') as out_file:
#     writer = csv.writer(out_file, lineterminator='\n')
#     writer.writerows(update_list)
