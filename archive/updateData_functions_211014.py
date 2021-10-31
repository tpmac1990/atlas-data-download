import time
import os
from functions import *
from functions.add_wkt import *
import ctypes
import geopandas as gpd
import pandas as pd


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
    # for data_group in ['occurrence']:
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

        # archive previous input files
        archiveRemoveOldFiles(self)
        # archive previous output files
        archiveRemoveOutputFiles(self)
        # combine duplicate AUS_OS wells. There are duplicates for exploration, appraisal and production. only AUS_OS occurrence 
        combineSameNameWellsAusOS(self)
        # merge multiple polygons for the same id spread over multiple rows. only tenement VIC 1 & 2
        combinePolygonsWithSameID_VIC(self)
        # remove the duplicate id rows, keeping the last not the first. only tenement QLD_1
        deleteSecondofDuplicate_QLD_1(self)
        # create a unique key field for files that don't have one. occurrence SA_2, SA_1. tenement QLD_3
        createUniqueKeyFieldAllFiles(self)
        # drop duplicate rows by the 'key' column. occurrences SA_1, WA_2
        removeDuplicateRowsByKeyAllFiles(self)
        # merge two files on a given column and overwrite fields of the primary file with the values of the second file. only occurrence SA_1
        combineFilesAllFiles(self)
        # creates a type column for sites by finding keywords from a string composed on multiple joined fields. occurrence SA_1, NSW_1, TAS_1
        getSiteTypeFromJoinedString(self)
        # clear field if it contains any value in a given list. occurrence NSW_1
        clearFieldIfContainsValueFromList(self)
        # filter for or filter out a list of values for given fields. common in both occurrence and tenement
        filterAllFilesForRelevantData(self)
        # filter out rows with blanks for all fields in a given list. only occurrence NSW_1
        filterOutBlanksForMultipleColumns(self)
        # filters out a row if any of the values in a given list exist
        filterOutByKeyWord(self)
        # merges the percentage with the holder name. NT files
        mergeRowsAllFiles(self)
        # sort multiple values separated by ; 
        sortMultipleValuesString(self)
        # filter out rows that have invalid gemoetry
        deletingInvalidWktRowsAllFiles(self)
        # add the gplore identifier field
        addIdentifierField(self)
        # create the files that will be combined to make the complete dataset
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



def combine_title_data(self):
    ''' compile all the separate site files into one dataset '''
    func_start = time.time()
    print('Combine all title data into complete files')

    # convert_dir = os.path.join(self.output_dir,'convert')
    convert_dir = self.convert_dir
    core_dir = os.path.join(self.output_dir,'core')
    new_dir = os.path.join(self.output_dir,'new')
    self.update_dir = os.path.join(self.output_dir,'update')

    related_ids_core_path = os.path.join(core_dir,"TenOriginalID.csv")

    companies_orig_df = pd.read_csv(os.path.join(convert_dir,"Companies_R.csv"),engine='python')
    status_orig_df = pd.read_csv(os.path.join(convert_dir,"TenStatus_R.csv"),engine='python')
    type_orig_df = pd.read_csv(os.path.join(convert_dir,"TenType_R.csv"),engine='python')

    TenType_df = pd.read_csv(os.path.join(core_dir,"TenType.csv"),engine='python')
    TenStatus_df = pd.read_csv(os.path.join(core_dir,"TenStatus.csv"),engine='python')
    self.Holder_df = pd.read_csv(os.path.join(core_dir,"Holder.csv"),engine='python')
    Shore_df = pd.read_csv(os.path.join(core_dir,"Shore.csv"),engine='python')

    self.status_df = status_orig_df.merge(TenStatus_df,left_on='Original',right_on='original',how='left').drop(columns=['original','simple_id'],axis=1)
    self.type_df = type_orig_df.merge(TenType_df,left_on='F_Name',right_on='fname',how='left').drop(columns=['fname','act_id','original','simple_id','Act'],axis=1)
    self.companies_df = companies_orig_df.merge(self.Holder_df,left_on='new_name',right_on='name',how='left').drop(columns=['name','typ_id','user_name','valid_relations','valid_instance','user_edit','date_modified','date_created'],axis=1)
    
    self.status_uk = TenStatus_df.query('original == "Unknown"').iloc[0]['_id']
    self.type_uk = TenType_df.query('original == "Unknown"').iloc[0]['_id']
    self.holder_uk = self.Holder_df.query('name == "Unknown"').iloc[0]['_id']
    self.shore_uk = Shore_df.query('name == "Unknown"').iloc[0]['code']

    self.Tenement_nowkt_df = pd.DataFrame()
    self.tenement_oid_df = pd.DataFrame()
    self.TenHolder_df = pd.DataFrame()

    # save self.Holder_df with suffix '_pre' so it can be referred to later when finding near matches for missing holders. The missing holders will be temporarily added to self.Holder_df, so it can not be used.
    self.Holder_df.to_csv(os.path.join(core_dir,"Holder_pre.csv"),index=False)

    # for data_group in self.data_groups:
    self.configs = getJSON(os.path.join(self.configs_dir,'formatting_config.json'))['tenement']

    # print([x for x in self.configs]) 
    # VIC_1, _2, QLD_1: nan in dates

    for file in self.configs:
        # configs to format and combine the data 
        configs = self.configs[file]['build']
        # directory with the new files
        self.vba_dir = os.path.join(self.input_dir,'tenement','vba')
        self.plain_dir = os.path.join(self.input_dir,'tenement','plain')
        self.state = file[:-2]

        # if file == 'QLD_1':
        # read the csv file to pandas df
        if configs:
            print('tenement: ' + file)
            format_file_to_combine(self,file,configs)

    self.tenement_oid_df.drop_duplicates(inplace=True)
    self.Tenement_nowkt_df.drop_duplicates(inplace=True)
    self.TenHolder_df['_id'] = np.arange(1, len(self.TenHolder_df) + 1)

    add_new_related_ids_to_core(self,self.tenement_oid_df,related_ids_core_path)

    self.Tenement_nowkt_df.to_csv(os.path.join(new_dir,'Tenement_nowkt.csv'),index=False)
    self.tenement_oid_df.to_csv(os.path.join(new_dir,'tenement_oid.csv'),index=False)
    self.TenHolder_df.to_csv(os.path.join(new_dir,'TenHolder.csv'),index=False)
    # Holder_df will have the missing holder names added to it, so it needs to be saved
    self.Holder_df.to_csv(os.path.join(core_dir,"Holder.csv"),index=False) 

    print('Title data compilation: %s' %(time_past(func_start,time.time())))




# nsw_1:
# delete any row that has missing data, is unknown etc
# use 'DEPOSIT_NA' as name. ALL_NAMES includes lodes that exist within the site.
# create one field that joins all relevant fields. look for relevant values in this field. Underground mine overrides, mine. drop words like mineral


from datetime import datetime, date, timedelta

def combine_site_data(self):
    ''' compile all the separate title files into one dataset '''
    func_start = time.time()
    print('Combine all site data into complete files')

    convert_dir = self.convert_dir
    core_dir = os.path.join(self.output_dir,'core')
    new_dir = os.path.join(self.output_dir,'new')

    related_ids_core_path = os.path.join(core_dir,"OccOriginalID.csv")

    # directory with the new files
    self.vba_dir = os.path.join(self.input_dir,'occurrence','vba')
    self.plain_dir = os.path.join(self.input_dir,'occurrence','plain')
    self.update_dir = os.path.join(self.output_dir,'update')

    status_orig_df = pd.read_csv(os.path.join(convert_dir,"OccStatus_R.csv"),engine='python')
    type_orig_df = pd.read_csv(os.path.join(convert_dir,"OccType_R.csv"),engine='python')
    size_orig_df = pd.read_csv(os.path.join(convert_dir,"OccSize_R.csv"),engine='python')
    material_orig_df = pd.read_csv(os.path.join(convert_dir,"Materials_R.csv"),engine='python')

    OccStatus_df = pd.read_csv(os.path.join(core_dir,"OccStatus.csv"),engine='python')
    OccType_df = pd.read_csv(os.path.join(core_dir,"OccType.csv"),engine='python')
    OccSize_df = pd.read_csv(os.path.join(core_dir,"OccSize.csv"),engine='python')
    Material_df = pd.read_csv(os.path.join(core_dir,"Material.csv"),engine='python')

    self.status_df = status_orig_df.merge(OccStatus_df,left_on='Original Version',right_on='original',how='left').drop(columns=['Original Version','original','simple_id'],axis=1)
    self.type_df = type_orig_df.merge(OccType_df,left_on='Original Version',right_on='original',how='left').drop(columns=['Original Version','original','simple_id'],axis=1)
    self.size_df = size_orig_df.merge(OccSize_df,left_on='Formatted',right_on='name',how='left').drop(columns=['Formatted','name'],axis=1)
    self.material_df = material_orig_df.merge(Material_df,on='code',how='left',suffixes=('_x',''))     
    self.related_mat_df = pd.read_csv(os.path.join(convert_dir,"Materials_Related_R.csv"),engine='python')
    self.name_lookup_df = pd.read_csv(os.path.join(convert_dir,"OccName_R.csv"),engine='python')
    self.name_id_df = pd.read_csv(os.path.join(core_dir,"OccName.csv"),engine='python')

    self.status_uk = OccStatus_df.query('original == "Unknown"').iloc[0]['_id']
    self.size_uk = OccSize_df.query('name == "Unspecified"').iloc[0]['code']
    self.type_uk = OccType_df.query('original == "Unspecified"').iloc[0]['_id']
    self.material_uk = Material_df.query('name == "Unknown"').iloc[0]['code']

    self.Occurrence_df = pd.DataFrame()
    self.occurrence_oid_df = pd.DataFrame()
    self.occurrence_name_df = pd.DataFrame()
    self.occurrence_typ_df = pd.DataFrame()
    self.occurrence_majmat_df = pd.DataFrame()
    self.occurrence_minmat_df = pd.DataFrame()

    self.configs = getJSON(os.path.join(self.configs_dir,'formatting_config.json'))['occurrence']

    # # ['AUS_OSPET_1', 'NSW_1', 'NSW_2', 'NT_1', 'NT_2', 'QLD_1', 'QLD_2', 'SA_1', 'SA_2', 'SA_3', 'TAS_1', 'VIC_1', 'VIC_2', 'WA_1', 'WA_2']
    # # print([i for i in self.configs])
    # TAS_1: nan exist in type

    for file in self.configs:
        # configs to format and combine the data 
        configs = self.configs[file]['build']
        self.state = file[:-2]

        # if file == 'NSW_1':
        # read the csv file to pandas df
        if configs:
            print('Occurrence: ' + file)
            format_site_file_to_combine(self,file,configs)
    
    self.occurrence_oid_df.drop_duplicates(inplace=True)
    self.occurrence_name_df.drop_duplicates(inplace=True)
    self.occurrence_majmat_df.drop_duplicates(inplace=True)
    self.occurrence_minmat_df.drop_duplicates(inplace=True)
    self.occurrence_typ_df.drop_duplicates(inplace=True)

    add_new_related_ids_to_core(self,self.occurrence_oid_df,related_ids_core_path)

    self.Occurrence_df.to_csv(os.path.join(new_dir,'Occurrence_pre.csv'),index=False)
    self.occurrence_oid_df.to_csv(os.path.join(new_dir,'occurrence_oid.csv'),index=False)
    self.occurrence_name_df.to_csv(os.path.join(new_dir,'occurrence_name.csv'),index=False)
    self.occurrence_typ_df.to_csv(os.path.join(new_dir,'occurrence_typ.csv'),index=False)
    self.occurrence_majmat_df.to_csv(os.path.join(new_dir,'occurrence_majmat.csv'),index=False)
    self.occurrence_minmat_df.to_csv(os.path.join(new_dir,'occurrence_minmat.csv'),index=False)

    print('Site data compilation: %s' %(time_past(func_start,time.time())))


def output_missing_data(self):
    ''' save missing data in files stored in the output/update directory.
        missing_all: each missing item with its 'ind' value so it can be updated later
        missing_reduced: unique missing values. if a new company name appears for multiple titles, it will only be shown in this list once.
        use fuzzywuzzy to find similar names for missing company names
    '''
    print('Output missing data files.')
    
    finalize_missing_data(self)



def apply_missing_data_updates(self):

    self.core_dir = os.path.join(self.output_dir,'core')
    self.update_dir = os.path.join(self.output_dir,'update')

    access_configs = getJSON(os.path.join(self.configs_dir,'db_access_configs.json'))
    configs = getJSON(os.path.join(self.configs_dir,'commit_updates.json'))

    # update the database tables to match the csv files
    db_keys = access_configs[self.db_location]
    self.con = sqlalchemy_engine(db_keys).connect()
    self.conn = connect_psycopg2(db_keys)

    manual_update_path = os.path.join(self.update_dir,'manual_update_required.csv')
    missing_all_path = os.path.join(self.update_dir,'missing_all.csv')
    manual_update_df = pd.read_csv(manual_update_path)
    self.manual_update_df = manual_update_df[['STATE','GROUP','FIELD','ORIGINAL','LIKELY_MATCH']]
    self.missing_all_df = pd.read_csv(missing_all_path)


    for x in configs:
        print(x)
        # if x == 'SIZE':
        commit_fields_updated_data(self,x,configs[x])

    self.con.close()
    self.conn.close()










    # names_dir = os.path.join(convert_dir,'occurrence')
    # path = os.path.join(os.path.join(convert_dir,'occurrence','OccName_R.csv'))
    # df = pd.read_csv(path).fillna('').drop('NAME',1)

    # # print(df.head())

    # s = df['FINAL'].str.split(';').apply(pd.Series, 1).stack()
    # s.index = s.index.droplevel(-1)
    # s.drop_duplicates(inplace=True)
    # s.name = 'name'
    # s.sort_values(inplace=True)
    # new_df = s.to_frame()
    # new_df['_id'] = np.arange(1, len(new_df) + 1)

    # new_df['user_name'] = 'ss'
    # new_df['valid_instance'] = True
    # new_df['date_created'] = date.today()

    # new_df.to_csv(os.path.join(core_dir,'OccName.csv'),index=False)

    # df = df.join(s)

    # df = df_main.join(s)








    #     df.columns = ['NAME']
    #     if name_df.empty:
    #         name_df = df
    #     else:
    #         name_df = pd.concat((name_df,df))

    # # # print(name_df.head())
    # # # print(len(name_df.index))
    # name_df.drop_duplicates(inplace=True)
    # # # print(len(name_df.index))
    # # name_df.to_csv(os.path.join(self.vba_dir,'a_names.csv'),index=False)

    # names_dir = os.path.join(convert_dir,'occurrence','name')
    # path = os.path.join(names_dir,'orig_2_names.csv')
    # # name_df.to_csv(path,index=False)

    # path = os.path.join(names_dir,'names.csv')

    # df = pd.read_csv(path).fillna('')
    # orig_df = pd.read_csv(os.path.join(names_dir,'orig_2_names.csv'))

    # merg_df = df.merge(orig_df,on='NAME',how='outer',indicator=True)

    # # print(merg_df.query('_merge != "both"').head())
    # # print(len(merg_df.query('_merge != "both"').index))

    # merg_df.to_csv(os.path.join(names_dir,'rejects.csv'),index=False)



    # # print(len(other_df.index))

    # # # # re_str = r"(\?|Unnamed|\]|\=|\-\-|Ã|\\|\/)"
    # # # re_str = r"(\[|\])"   
    # # # 'S part of  delete all Rd   Rd Lot    hwy
    # # # re_str = r"(in Later|remove|approx|Extensions|Also)"
    # re_str = r"(\(.\))"

    # df['SYMBOL'] = df['NAME'].apply(lambda x: True if re.search(re_str,x) else False)
    # # df = df.query('SYMBOL == True').query('FINAL != ""').drop('SYMBOL',1)
    # df = df.query('SYMBOL == False or FINAL == ""').drop('SYMBOL',1)
    # # print(len(df.index))
    

    # df = pd.concat((df,other_df))

    # # df.to_csv(os.path.join(names_dir,'semi.csv'),index=False)
    # df.to_csv(os.path.join(names_dir,'names.csv'),index=False)
    # # df.to_csv(os.path.join(names_dir,'new_names.csv'),index=False)






    # df['FINAL'] = df['NAME']
    # df.to_csv(os.path.join(names_dir,'names.csv'),index=False)












    # companies_orig_df = pd.read_csv(os.path.join(convert_dir,'occurrence',"Companies_R.csv"),engine='python')
    # status_orig_df = pd.read_csv(os.path.join(convert_dir,'occurrence',"Status_R.csv"),engine='python')
    # type_orig_df = pd.read_csv(os.path.join(convert_dir,'occurrence',"Type_R.csv"),engine='python')

    # TenType_df = pd.read_csv(os.path.join(core_dir,"TenType.csv"),engine='python')
    # TenStatus_df = pd.read_csv(os.path.join(core_dir,"TenStatus.csv"),engine='python')
    # Holder_df = pd.read_csv(os.path.join(core_dir,"Holder.csv"),engine='python')

    # self.status_df = status_orig_df.merge(TenStatus_df,left_on='Original',right_on='original',how='left').drop(columns=['Simplified','original','simple_id'],axis=1)
    # self.type_df = type_orig_df.merge(TenType_df,left_on='F_Name',right_on='fname',how='left').drop(columns=['fname','act_id','original','simple_id','Act','Original','Simplified'],axis=1)
    # self.companies_df = companies_orig_df.merge(Holder_df,left_on='FINAL NAME',right_on='name',how='left').drop(columns=['name','typ_id','user_name','valid_relations','valid_instance','user_edit','date_modified','date_created'],axis=1)
