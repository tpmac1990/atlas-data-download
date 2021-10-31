import time
import datetime
# import pandas as pd
# from datetime import datetime, date
import os
# from updateData_functions import (download_data_to_csv, add_wkt_tenement, 
#                                 create_spatial_relation_files, find_changes_update_core_and_database, 
#                                 add_crs_to_wkt, previous_core_to_db, fileExist,
#                                 check_csvs_for_errors, apply_missing_data_updates)
# build_local_gov_files, separate_geom_fields, reduce_pnts_in_poly, combine_title_data, combine_site_data, output_missing_data, extract_user_edits_to_core, preformat_file
from functions import (
        create_combined_datasets, 
        extract_user_edits_to_core, 
        preformat_files, 
        create_spatial_relation_files, 
        add_crs_to_wkt, 
        find_changes_update_core_and_database, 
        apply_missing_data_updates,
        fileExist
        )



class UpdateData():

    def __init__(self):
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
        # # download the data and convert it to WKT in csv
        # download_data_to_csv(self)
        # save the frontend user edits to the core file
        extract_user_edits_to_core(self)
        # Format required files, compare with the core and create the change files and update file.
        preformat_files(self)
        # combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later
        create_combined_datasets(self)
        # add spatially related data
        create_spatial_relation_files(self)
        # add crs to each of the geometires in the WKT field and reduce points in Tenement polygons
        add_crs_to_wkt(self)
        # Find the changes between the new and the core files and update them
        find_changes_update_core_and_database(self)


    def update_missing_data(self):
        # add the missing data updates to the required tables and update the database
        apply_missing_data_updates(self)



UpdateData().download_format()
# UpdateData().update_missing_data()





#     # # add the wkt data back to the tenement data
        #     # add_wkt_tenement(self)

# # check the csv files for errors that will be problematic when loading to the db. currently not used
    # check_csvs_for_errors(self)



# def reduce_poly_pnts(self):
    #     # 
    #     reduce_pnts_in_poly(self)

# UpdateData().reduce_poly_pnts()



# # combine all title data in to one dataset
# combine_title_data(self)
# # combine all site data in to one dataset
# combine_site_data(self)
# # save the missing data to files
# output_missing_data(self)




    # def revert_db_to_previous(self):
    #     # delete all db tables and load the previous core files from the archive to the database. This is useful if there is an arror when running the update.
    #     previous_core_to_db(self)

    # def add_user_dummy_user_edits(self):
# UpdateData().add_relations()

# UpdateData().revert_db_to_previous()



    # def build_local_gov_datasets(self):

    #     BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
    #     self.standard = os.path.join(BASE_DIR,'regions','standard')
    #     self.new_local = os.path.join(BASE_DIR,'regions','new_local')
    #     self.region = os.path.join(BASE_DIR,'regions')

    #     self.core = os.path.join(self.output_dir,'core')

    #     build_local_gov_files(self)
    

# UpdateData().build_local_gov_datasets()

    # def update_database(self):
    #     # Find the changes between the new and the core files and update them
    #     create_change_and_update_core_files(self)
        
        
        # # Use the Update and change files to update the db
        # make_database_changes(self)
        # # # Add the Change and Update files to the db



# UpdateData().download_format()
# UpdateData().add_relations()

# UpdateData().update_database()

# "user_name","valid_relations","valid_instance","user_edit","date_modified","date_created"
# "user_name","valid_instance","date_created"
# "valid_relations","user_edit","date_modified"
