# import time
import datetime
import os
import logging

from functions.data_download import DataDownload
from functions.apply_user_updates import ExtractUserEdits
from functions.preformat import PreformatData
from functions.build_format import CombineDatasets
from functions.spatial_relationships import SpatialRelations
from functions.db_update import ChangesAndUpdate
from functions.commit_new_values import UpdateMissingData

from functions.timer import time_past, start_time
from functions.setup import Logger



def main():
    func_start = start_time()
    # # add the missing data updates to the required tables and update the database
    # UpdateMissingData()
    # # download the data and convert it to WKT in csv
    # DataDownload()
    # # save the frontend user edits to the core file
    # ExtractUserEdits()
    # # Format required files, compare with the core and create the change files and update file.
    # PreformatData()
    # # combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later
    # CombineDatasets()
    # # add spatially related data. add crs to each of the geometires in the WKT field and reduce points in Tenement polygons
    # SpatialRelations()
    # # Find the changes between the new and the core files and update them
    # ChangesAndUpdate()
    # log the total time
    Logger.logger.info("\n\nTotal duration: %s"%(time_past(func_start)))



if __name__ == "__main__":
    main()










#     def update_missing_data(self):
#         # add the missing data updates to the required tables and update the database
#         apply_missing_data_updates(self)



# class SetUp:
#     # BASE_DIR in this instance is the RAW_DATASETS
#     BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     # set the date
#     tDate = datetime.datetime.now().strftime("%y%m%d")
#     pyDate = datetime.datetime.now().date() # format: yyyy-mm-dd
#     # scripts directory
#     scripts_dir = os.path.join(BASE_DIR,'scripts')
#     # configs directory
#     logs_dir = os.path.join(scripts_dir,'logs')
#     # configs directory
#     configs_dir = os.path.join(scripts_dir,'configs')
#     # input directory. Where the raw data is downloaded to and formatted
#     input_dir = os.path.join(BASE_DIR,'input')
#     # output directory. Where combined data is stored
#     output_dir = os.path.join(BASE_DIR,'output')
#     # conversion directory. Where the files that convert the raw values to formatted values is stored
#     convert_dir = os.path.join(BASE_DIR,'conversion')
#     # reions shapefiles directory
#     regions_dir = os.path.join(BASE_DIR,'regions')
#     # data groups
#     data_groups = ['occurrence','tenement']
#     # logging setup
#     formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s - %(message)s')
#     file_handler = logging.FileHandler(os.path.join(logs_dir,'main.log'))
#     file_handler.setFormatter(formatter)
#     file_handler.setLevel(logging.INFO)
#     hash = '#'*120
#     dash = '-'*30
#     # update is True if there are no files in the output/core directory
#     isUpdate = True if fileExist(os.path.join(output_dir,'update','change.csv')) else False
#     # is this a 'local' or 'remote' database update
#     db_location = 'local'



# class GetInputDir(BaseDirectories):

#     def getInput(self):
#         print(self.input_dir)


# if __name__ == "__main__":
#     # GetInputDir().getInput()

#     base = BaseDirectories()
#     print(base.input_dir)








# class UpdateData():

#     def __init__(self):
#         # BASE_DIR in this instance is the RAW_DATASETS
#         BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#         # set the date
#         self.tDate = datetime.datetime.now().strftime("%y%m%d")
#         self.pyDate = datetime.datetime.now().date() # format: yyyy-mm-dd
#         # scripts directory
#         self.scripts_dir = os.path.join(BASE_DIR,'scripts')
#         # configs directory
#         self.logs_dir = os.path.join(self.scripts_dir,'logs')
#         # configs directory
#         self.configs_dir = os.path.join(self.scripts_dir,'configs')
#         # input directory. Where the raw data is downloaded to and formatted
#         self.input_dir = os.path.join(BASE_DIR,'input')
#         # output directory. Where combined data is stored
#         self.output_dir = os.path.join(BASE_DIR,'output')
#         # conversion directory. Where the files that convert the raw values to formatted values is stored
#         self.convert_dir = os.path.join(BASE_DIR,'conversion')
#         # reions shapefiles directory
#         self.regions_dir = os.path.join(BASE_DIR,'regions')
#         # data groups
#         self.data_groups = ['occurrence','tenement']
#         # logging setup
#         formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(name)s - %(message)s')
#         self.file_handler = logging.FileHandler(os.path.join(self.logs_dir,'main.log'))
#         self.file_handler.setFormatter(formatter)
#         self.file_handler.setLevel(logging.INFO)
#         self.hash = '#'*120
#         self.dash = '-'*30
#         # update is True if there are no files in the output/core directory
#         self.isUpdate = True if fileExist(os.path.join(self.output_dir,'update','change.csv')) else False
#         # is this a 'local' or 'remote' database update
#         self.db_location = 'local'
        

#     def download_format(self):
#         func_start = time.time()
#         # download the data and convert it to WKT in csv
#         download_data_to_csv(self)
#         # # save the frontend user edits to the core file
#         # extract_user_edits_to_core(self)
#         # # Format required files, compare with the core and create the change files and update file.
#         # preformat_files(self)
#         # # combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later
#         # create_combined_datasets(self)
#         # # add spatially related data
#         # create_spatial_relation_files(self)
#         # # add crs to each of the geometires in the WKT field and reduce points in Tenement polygons
#         # add_crs_to_wkt(self)
#         # # Find the changes between the new and the core files and update them
#         # find_changes_update_core_and_database(self)
#         print('Total duration: %s' %(time_past(func_start)))


#     def update_missing_data(self):
#         # add the missing data updates to the required tables and update the database
#         apply_missing_data_updates(self)



# UpdateData().download_format()
# UpdateData().update_missing_data()

