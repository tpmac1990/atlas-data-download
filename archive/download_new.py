import time
import os
from base_functions import *
from project_functions import *

class DataImportAndUpdate():

  def __init__(self,data_group):
    # BASE_DIR in this instance is the RAW_DATASETS
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    self.scripts_dir = os.path.join(BASE_DIR,'scripts')
    self.configs_dir = os.path.join(BASE_DIR,'scripts/configs')
    
    self.data_group_dir = os.path.join(BASE_DIR,'input',data_group)

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


  def downloadAndExportShpToCsv(self):
    # expected time: 8min 10sec (both data groups)
    start = time.time()

    # delete all the files in the new directory
    delete_files_in_directory(self.wkt_csv_dir)

    for data_import_group in self.Data_Import:
      # if data_import_group['name'] in ["OS_petroleum_wells"]:

      unzipped_dir = os.path.join(self.unzipped_dir, data_import_group['created_extension'])
      download_unzip_link_manual(self,data_import_group)

      for group in data_import_group['groups']:
        print('working: ' + group['output'])
        self.count += 1
        merge_and_export_to_csv(self,data_import_group,group)
        print('complete: ' + group['output'])

    print('Macro time: %s' %(time_past(start,time.time())))


DataImportAndUpdate('occurrence').downloadAndExportShpToCsv()