import time
import datetime
import os

from functions import (
        download_data_to_csv,
        create_combined_datasets, 
        extract_user_edits_to_core, 
        preformat_files, 
        create_spatial_relation_files, 
        add_crs_to_wkt, 
        find_changes_update_core_and_database, 
        apply_missing_data_updates,
        fileExist,
        time_past
        )



class UpdateData():

    def __init__(self):
        self.spacial_relations = None
        # BASE_DIR in this instance is the RAW_DATASETS
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # set the date
        self.tDate = datetime.datetime.now().strftime("%y%m%d")
        self.pyDate = datetime.datetime.now().date() # format: yyyy-mm-dd
        # scripts directory
        self.scripts_dir = os.path.join(BASE_DIR,'scripts')
        # configs directory
        self.configs_dir = os.path.join(self.scripts_dir,'configs')
        # input directory. Where the raw data is downloaded to and formatted
        self.input_dir = os.path.join(BASE_DIR,'input')
        # output directory. Where combined data is stored
        self.output_dir = os.path.join(BASE_DIR,'output')
        # conversion directory. Where the files that convert the raw values to formatted values is stored
        self.convert_dir = os.path.join(BASE_DIR,'conversion')
        # reions shapefiles directory
        self.regions_dir = os.path.join(BASE_DIR,'regions')
        # data groups
        self.data_groups = ['occurrence','tenement']
        # update is True if there are no files in the output/core directory
        self.isUpdate = True if fileExist(os.path.join(self.output_dir,'update','change.csv')) else False
        # is this a 'local' or 'remote' database update
        self.db_location = 'local'
        

    def download_format(self):
        func_start = time.time()
        # # download the data and convert it to WKT in csv
        # download_data_to_csv(self)
        # save the frontend user edits to the core file
        extract_user_edits_to_core(self)
        # Format required files, compare with the core and create the change files and update file.
        preformat_files(self)
        # combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later
        create_combined_datasets(self)
        # add spatially related data
        self.spacial_relations = SpacialRelations()
        # add crs to each of the geometires in the WKT field and reduce points in Tenement polygons
        add_crs_to_wkt(self)
        # Find the changes between the new and the core files and update them
        find_changes_update_core_and_database(self)
        print('Total duration: %s' %(time_past(func_start,time.time())))


    def update_missing_data(self):
        # add the missing data updates to the required tables and update the database
        apply_missing_data_updates(self)



UpdateData().download_format()
# UpdateData().update_missing_data()
