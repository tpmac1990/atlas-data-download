import time
import os
from functions import *
from functions.add_wkt import *
import ctypes
import geopandas as gpd


def download_data_to_csv(self):
    # expected time: 8min 30sec
    func_start = time.time()

    # loops over the occurrence and then tenement datagroups
    for data_group in self.data_groups:
        # set directories and open config files
        self.data_group_dir = os.path.join(self.input_dir,data_group)
        configs = getJSON(os.path.join(self.configs_dir,'download_config.json'))[data_group]
        # self.Data_Import = configs['Data_Import']
        self.Data_Import = configs
        self.temp_links = getJSON(os.path.join(self.configs_dir,'temp_links_config.json'))[data_group]
        self.count = 0 # used when creating the merge file. prevents an error
        self.manual_dir = os.path.join(self.data_group_dir,'manual')
        self.wkt_csv_dir = os.path.join(self.data_group_dir,'new')
        self.unzipped_dir = os.path.join(self.data_group_dir,'unzipped')
        self.merged_file_dir = os.path.join(self.data_group_dir,'merged')
        self.zip_file_path = os.path.join(self.data_group_dir,'spatial_download.zip')
        self.download_fail_path = os.path.join(self.data_group_dir,'download_fail.csv')
        self.data_group = data_group

        # delete all the files in the new directory
        delete_files_in_directory(self.wkt_csv_dir)
        # delete the download_fail file if it exists
        if fileExist(self.download_fail_path):
            os.remove(self.download_fail_path)

        # loops through each of the groups in the configs.json file for the current data_group
        for data_import_group in self.Data_Import:
            # if data_import_group['name'] in ["tas_mineral"]:

            # unzipped_dir = os.path.join(self.unzipped_dir, data_import_group['created_extension']) # gets the location of the required file
            # downloads and extracts the data for all the zip files. If the link fails, then it will be added to the download_fail.csv and the formatting will be skipped.
            if download_unzip_link_manual(self,data_import_group):

                for group in data_import_group['groups']:
                    print("working on: %s"%(group['output']))
                    self.count += 1
                    # Merges the files where necessary and export to csv with WKT
                    merge_and_export_to_csv(self,data_import_group,group)

    print('Data Download time: %s' %(time_past(func_start,time.time())))


def preformat_file(self):
    func_start = time.time()
    csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

    for data_group in self.data_groups:
        # set directories and open config files
        self.data_group_dir = os.path.join(self.input_dir,data_group)
        self.core_dir = os.path.join(self.data_group_dir,'core')
        self.plain_dir = os.path.join(self.data_group_dir,'plain')
        self.change_dir = os.path.join(self.data_group_dir,'change')
        self.new_dir = os.path.join(self.data_group_dir,'new')
        self.update_dir = os.path.join(self.data_group_dir,'update')
        self.vba_dir = os.path.join(self.data_group_dir,'vba')
        self.archive_dir = os.path.join(self.data_group_dir,'archive',self.tDate)
        self.update_path = os.path.join(self.update_dir,'update.csv')
        self.inactive_path = os.path.join(self.update_dir,'inactive.csv')
        self.reactivated_path = os.path.join(self.update_dir,'reactivated.csv')
        createMultipleDirectories(self.archive_dir,['change','core','update'])
        self.output_archive_dir = os.path.join(self.output_dir,'archive',self.tDate)
        createMultipleDirectories(self.output_archive_dir,['change','core','update'])
        self.configs = getJSON(os.path.join(self.configs_dir,'formatting_config.json'))[data_group]
        self.download_configs = configs = getJSON(os.path.join(self.configs_dir,'download_config.json'))[data_group]
        self.download_fail_path = os.path.join(self.data_group_dir,'download_fail.csv')
        self.data_group = data_group
        self.ignore_files = getIgnoreFiles(self)

        archiveRemoveOldFiles(self)
        archiveRemoveOutputFiles(self)
        combineSameNameWellsAusOS(self)
        combinePolygonsWithSameID_VIC(self)
        deleteSecondofDuplicate_QLD_1(self)
        removeDuplicateRowsByKeyAllFiles(self)
        filterAllFilesForRelevantData(self)
        filterOutBlanksForMultipleColumns(self)
        filterOutByKeyWord(self)
        createUniqueKeyFieldAllFiles(self)
        combineFilesAllFiles(self)
        mergeRowsAllFiles(self)
        sortMultipleValuesString(self)
        deletingInvalidWktRowsAllFiles(self)
        addIdentifierField(self)
        createChangeFiles(self)

    print('Preformat time: %s' %(time_past(func_start,time.time())))



def add_wkt_tenement(self):
    ''' This re-assigns the wkt multipolygon data to each row of the Tenement file. It is dropped earlier so it can be processed with
        excel and vba.
    '''
    func_start = time.time()
    csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
    self.change_dir = os.path.join(self.input_dir,'tenement','change')
    wkt_tenement_path = os.path.join(self.output_dir,'new','Tenement.csv')
    
    change_dic = createChangeDict(self)
    tenement_lst_wkt = insert_wkt(self,change_dic)
    writeToFile(wkt_tenement_path, tenement_lst_wkt)

    print('Added WKT to Tenement file: %s' %(time_past(func_start,time.time())))



def create_spatial_relation_files(self):
    ''' This creates all the geospatial relationships for the data. This includes tenement & occurrence relations, government region, local government and geological
        province relations.
    '''
    func_start = time.time()
    # directories
    self.new_dir = os.path.join(self.output_dir,'new')

    # paths
    self.tenement_path = os.path.join(self.new_dir,'Tenement.csv')
    self.occurrence_path = os.path.join(self.new_dir,'Occurrence_pre.csv')
    self.tenement_occurrence_path = os.path.join(self.new_dir,'tenement_occurrence.csv')

    # spatial dataframes
    self.ten_gdf = df_to_geo_df_wkt(pd.read_csv(self.tenement_path))
    self.occ_gdf = df_to_geo_df_wkt(pd.read_csv(self.occurrence_path))

    # shapefiles
    self.local_gov_gdf = gpd.read_file(os.path.join(self.regions_dir,'LocalGovernment.shp'))
    self.gov_region_gdf = gpd.read_file(os.path.join(self.regions_dir,'GovernmentRegion.shp'))
    self.geo_province_gdf = gpd.read_file(os.path.join(self.regions_dir,'GeologicalProvince.shp'))
    self.geo_state_gdf = gpd.read_file(os.path.join(self.regions_dir,'State.shp'))

    # get congif files
    self.region_configs = getJSON(os.path.join(self.configs_dir,'region_configs.json'))

    try:
        # create the occurrence tenement relation file
        create_tenement_occurrence_file(self)
        # create tenement materials files
        create_tenement_materials_files(self)
        # create the tenement and occurrence regions relations files
        create_region_relation_files(self)
        # create region wkt files like GeologicalProvince from the shapefiles. Only done if not an update
        create_regions_files(self)
    except Exception as e:
        print(str(e))

    print('Spatial Relationships: %s' %(time_past(func_start,time.time())))


def add_crs_to_wkt(self):
    ''' This inserts the crs code to the start of the wkt for each geometry feature in the table '''
    print('Adding crs to wkt fields.')
    try:
        self.new_dir = os.path.join(self.output_dir,'new')
        for file in ['Tenement','Occurrence']:
            path = os.path.join(self.new_dir,"%s.csv"%(file))
            df = pd.read_csv(path)
            df['geom'] = ["SRID=4202;%s"%(feature) for feature in df['geom']]
            df.to_csv(path,index=False)
    except Exception as e:
        print(str(e))




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




    # for data_group in self.data_groups:

    #     self.data_group = data_group
    #     self.data_group_config = self.change_config[data_group]

    #     # builds the change file for the required datagroup, updates its equivalent core file and adds it to the database
    #     build_group_change_file(self)
    #     # build the addition and removal database ready files
    #     build_group_addition_remove_files(self)











# def make_database_changes(self):
#     func_start = time.time()
#     # directories
#     self.core_dir = os.path.join(self.output_dir,'core')
#     self.change_dir = os.path.join(self.output_dir,'change')
#     self.update_dir = os.path.join(self.output_dir,'update')
#     # paths
#     self.change_path = os.path.join(self.update_dir,'change.csv')
#     self.update_path = os.path.join(self.update_dir,'update.csv')
#     # configs
#     self.update_configs = getJSON(os.path.join(self.configs_dir,'db_update_configs.json'))
#     self.access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))
    
#     if not fileExist(self.change_path): # If this file doesn't exist then all the files need to be pushed to the db.
#         # delete all content in tables and copy all files to the db
#         commit_all_files_to_db(self) 
#         # create empty change file
#         create_empty_change_file(self)
#     else:
#         print('update db only')
#         # # create the update and change list
#         # update_lst, changes_lst = initLstsCoreToNew()
#         # update_lst, changes_lst = removeOldAddNewToCoreAndDb(self,update_lst)
#         # # compare core to new. Creates the change and update files
#         # changes_lst = compareOutputCoreToNew(self,changes_lst)



    # print('Database update: %s' %(time_past(func_start,time.time())))



def extract_user_edits_to_core(self):
    ''' This gets all the latest user edits from all the necessary tables in the database and updates the core files with this data
        so the core files are maintained as a db copy.
            transfer_user_edits_to_core: transfers user edits to from the db table to its equivalent core file.
            record_user_edits: finds and records the changes the users have made in the application
        There is no need to update the Addition & Removal tables as they are never modified by the user
    '''
    # configs
    self.update_configs = getJSON(os.path.join(self.configs_dir,'db_update_configs.json'))
    self.access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))
    # directories
    self.core_dir = os.path.join(self.output_dir,'core')
    self.edit_dir = os.path.join(self.output_dir,'edit')

    # only relevant if updating
    if self.isUpdate:
        # update the core files for db tables which the user can create new instances e.g. Holder, OccName
        transfer_user_creations_to_core(self)
        # compare the three main group files and their related files and update the core files so they mimic the db
        transfer_user_edits_to_core(self)
        # add user changes from the db TenementChange & OccurrenceChange tables to their core equivalents
        transfer_changes_to_core(self)



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



# # create the change, add and remove tables and update them in the core files and database
# def build_update_tables_update_db(self):
#     func_start = time.time()
#     # directories
#     self.update_dir = os.path.join(self.output_dir,'update')
#     self.core_dir = os.path.join(self.output_dir,'core')
#     # paths
#     self.updates_path = os.path.join(self.update_dir,"update.csv")
#     self.changes_path = os.path.join(self.update_dir,"change.csv")
#     # open as df
#     self.update_df = pd.read_csv(self.updates_path)
#     self.change_df = pd.read_csv(self.changes_path)
#     # configs
#     self.change_config = getJSON(os.path.join(self.configs_dir,'change_output_config.json'))
#     access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))
#     # db connection
#     self.sqlalchemy_con = sqlalchemy_engine(access_configs["local"]).connect()
    
#     for data_group in self.data_groups:

#         self.data_group = data_group
#         self.data_group_config = self.change_config[data_group]

#         # builds the change file for the required datagroup, updates its equivalent core file and adds it to the database
#         build_group_change_file(self)
#         # build the addition and removal database ready files
#         build_group_addition_remove_files(self)

#     self.sqlalchemy_con.close()
#     print('Built update tables: %s' %(time_past(func_start,time.time())))