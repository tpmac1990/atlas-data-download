import time
import os
from functions import *
from functions.add_wkt import *
import ctypes
import datetime
import geopandas as gpd


def download_data_to_csv(self):
    # expected time: 8min 30sec
    func_start = time.time()

    for data_group in self.data_groups:
        # set directories and open config files
        self.data_group_dir = os.path.join(self.input_dir,data_group)
        configs = getJSON(os.path.join(self.configs_dir,'config.json'))[data_group]
        self.Data_Import = configs['Data_Import']
        self.temp_links = getJSON(os.path.join(self.configs_dir,'temp_links_config.json'))[data_group]
        self.count = 0
        self.manual_dir = os.path.join(self.data_group_dir,'manual')
        self.wkt_csv_dir = os.path.join(self.data_group_dir,'new')
        self.unzipped_dir = os.path.join(self.data_group_dir,'unzipped')
        self.merged_file_dir = os.path.join(self.data_group_dir,'merged')
        self.zip_file_path = os.path.join(self.data_group_dir,'spatial_download.zip')
        self.data_group = data_group

        # delete all the files in the new directory
        delete_files_in_directory(self.wkt_csv_dir)

        for data_import_group in self.Data_Import:
            # if data_import_group['name'] in ["vic_petroleum","vic_mineral"]:

            unzipped_dir = os.path.join(self.unzipped_dir, data_import_group['created_extension'])
            download_unzip_link_manual(self,data_import_group)

            for group in data_import_group['groups']:
                print('working: ' + group['output'])
                self.count += 1
                merge_and_export_to_csv(self,data_import_group,group)
                print('complete: ' + group['output'])

    print('Data Download time: %s' %(time_past(func_start,time.time())))


def preformat_file(self):
    func_start = time.time()
    csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
    self.tDate = datetime.datetime.now().strftime("%y%m%d")

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
        createMultipleDirectories(self.archive_dir,['change','core','update'])
        self.output_archive_dir = os.path.join(self.output_dir,'archive',self.tDate)
        createMultipleDirectories(self.output_archive_dir,['change','core','update'])
        self.configs = getJSON(os.path.join(self.configs_dir,'config.json'))[data_group]['Primary_Format']
        self.data_group = data_group

        archiveRemoveOldFiles(self)
        archiveRemoveOutputFiles(self)
        combineSameNameWellsAusOS(self)
        combinePolygonsWithSameID_VIC(self)
        deleteSecondofDuplicate_QLD_1(self)
        removeDuplicateRowsByKeyAllFiles(self)
        filterAllFilesForRelevantData(self)
        createUniqueKeyFieldAllFiles(self)
        combineFilesAllFiles(self)
        mergeRowsAllFiles(self) # this is the one to solve the nt_2 (0%) issue
        sortMultipleValuesString(self)
        deletingInvalidWktRowsAllFiles(self)
        addIdentifierField(self)
        createChangeFiles(self)

    print('Preformat time: %s' %(time_past(func_start,time.time())))



def add_wkt_tenement(self):
    func_start = time.time()
    csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
    self.change_dir = os.path.join(self.input_dir,'tenement','change')
    wkt_tenement_path = os.path.join(self.output_dir,'new','Tenement.csv')

    change_dic = createChangeDict(self)

    tenement_lst_wkt = insertWkt(self,change_dic)
    
    writeToFile(wkt_tenement_path, tenement_lst_wkt)

    print('Add WKT to Tenement file: %s' %(time_past(func_start,time.time())))



def create_spatial_relation_files(self):
    func_start = time.time()
    # directories
    self.new_dir = os.path.join(self.output_dir,'new')

    # paths
    self.tenement_path = os.path.join(self.new_dir,'Tenement.csv')
    self.occurrence_path = os.path.join(self.new_dir,'Occurrence_pre.csv')
    self.tenement_occurrence_path = os.path.join(self.new_dir,'tenement_occurrence.csv')

    # spatial dataframes
    self.ten_gdf = dfToGeoDf_wkt(pd.read_csv(self.tenement_path))
    self.occ_gdf = dfToGeoDf_wkt(pd.read_csv(self.occurrence_path))

    # shapefiles
    self.local_gov_gdf = gpd.read_file(os.path.join(self.regions_dir,'LocalGovernment.shp'))
    self.gov_region_gdf = gpd.read_file(os.path.join(self.regions_dir,'GovernmentRegion.shp'))
    self.geo_province_gdf = gpd.read_file(os.path.join(self.regions_dir,'GeologicalProvince.shp'))

    # get congif files
    self.region_configs = getJSON(os.path.join(self.configs_dir,'region_configs.json'))
    
    # create the occurrence tenement relation file
    create_tenement_occurrence_file(self)
    # create tenement materials files
    create_tenement_materials_files(self)
    # create the tenement and occurrence regions relations files
    create_region_relation_files(self)
    # create region files like GeologicalProvince from the shapefiles. Only done if not an update
    create_regions_files(self)

    print('Spatial Relationships: %s' %(time_past(func_start,time.time())))


def add_crs_to_wkt(self):
    print('Adding crs to wkt fields.')
    self.new_dir = os.path.join(self.output_dir,'new')
    for file in ['Tenement','occurrence']:
        path = os.path.join(self.new_dir,"%s.csv"%(file))
        df = pd.read_csv(path)
        df['WKT'] = ["SRID=4202;%s"%(feature) for feature in df['WKT']]
        # print(df.head(3))
        df.to_csv(path,index=False)
    print('Complete.')


def find_changes_update_core_and_database(self):
    func_start = time.time()
    # directories
    self.core_dir = os.path.join(self.output_dir,'core')
    self.new_dir = os.path.join(self.output_dir,'new')
    self.update_dir = os.path.join(self.output_dir,'update')
    self.change_dir = os.path.join(self.output_dir,'change')
    # paths
    self.updates_path = os.path.join(self.update_dir,"update.csv")
    self.changes_path = os.path.join(self.update_dir,"change.csv")
    # configs
    self.update_configs = getJSON(os.path.join(self.configs_dir,'db_update_configs.json'))
    self.access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))

    # get congif files
    # If less than three files, then all new files will be pushed to db
    if not self.isUpdate:
        print('No files in CORE directory. Creating CORE & CHANGE files from NEW directory.')
        copyDirectory(self.new_dir,self.core_dir)
        copyDirectory(self.new_dir,self.change_dir)
        # delete all content in tables and copy all files to the db
        commit_all_files_to_db(self) 
        # create empty change file. This will tell the script to update rather than renew everything the next time it is run.
        create_empty_change_file(self)
    else:
        print('Core files exist. Creating CHANGE and UPDATE files.')
        # Compare all the files that don't have changes recorded. Add new rows to db.
        compare_base_tables_add_new(self)
        # builds the files (update and change) that record the additions, removals and changes made for the relevant ids
        build_update_and_change_files(self)
        # makes the changes to the core file and the db
        make_core_file_and_db_changes(self)



    print('Find changes and updates: %s' %(time_past(func_start,time.time())))


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



    print('Database update: %s' %(time_past(func_start,time.time())))