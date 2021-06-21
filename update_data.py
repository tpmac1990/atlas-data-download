import time
import datetime
# from datetime import datetime, date
import os
from updateData_functions import (download_data_to_csv, preformat_file, add_wkt_tenement, 
                                create_spatial_relation_files, find_changes_update_core_and_database, 
                                add_crs_to_wkt, previous_core_to_db, fileExist, extract_user_edits_to_core)


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
        # reions shapefiles directory
        self.regions_dir = os.path.join(BASE_DIR,'regions')
        # data groups
        self.data_groups = ['occurrence','tenement']
        # self.data_groups = ['tenement']
        # update is True if there are no files in the output/core directory
        # self.isUpdate = True if len(os.listdir(os.path.join(self.output_dir,'ds_core'))) > 0 else False
        # self.isUpdate = True if fileExist(os.path.join(self.output_dir,'core','Tenement.csv')) else False
        self.isUpdate = True if fileExist(os.path.join(self.output_dir,'update','change.csv')) else False
        

    def download_format(self):
        # download the data and convert it to WKT in csv
        download_data_to_csv(self)
        # # Format required files, compare with the core and create the change files and update file.
        # preformat_file(self)
        # # save the frontend user edits to the core file
        # extract_user_edits_to_core(self)


    def add_relations(self):
        # add the wkt data back to the tenement data
        add_wkt_tenement(self)
        # add spatially related data
        create_spatial_relation_files(self)
        # add crs to each of the geometires in the WKT field
        add_crs_to_wkt(self)
        # Find the changes between the new and the core files and update them
        find_changes_update_core_and_database(self)


    def revert_db_to_previous(self):
        # delete all db tables and load the previous core files from the archive to the database. This is useful if there is an arror when running the update.
        previous_core_to_db(self)

    # def add_user_dummy_user_edits(self):



UpdateData().download_format()
# UpdateData().add_relations()
# UpdateData().revert_db_to_previous()





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
