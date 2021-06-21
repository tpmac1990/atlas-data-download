import time
from importlib.machinery import SourceFileLoader
from qgis.core import QgsWkbTypes

my_qgis = SourceFileLoader("pqgis_functions", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/pqgis_functions.py").load_module()
my_func = SourceFileLoader("functions", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/functions.py").load_module()


class Functions():

  def __init__(self,data_group):
    self.root_directory = 'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
    self.scripts_directory = self.root_directory + 'scripts/'
    self.data_group_directory = "%s%s/" %(self.root_directory,data_group)

    configs = my_func.getJSON(self.scripts_directory + 'config.json')[data_group]
    self.Data_Import = configs['Data_Import']
    self.temp_links = my_func.getJSON(self.scripts_directory + 'temp_links_config.json')[data_group]

    self.count = 0

    self.manual_directory = self.data_group_directory + "manual/"
    self.wkt_csv_directory = self.data_group_directory + "new/"
    self.unzipped_directory = self.data_group_directory + "unzipped/"
    self.merged_file_directory = self.data_group_directory + "merged/"
    self.zip_file_path = self.data_group_directory + "spatial_download.zip"

    if data_group == 'occurrence':
      self.WkbTypes = QgsWkbTypes.Point
    else:
      self.WkbTypes = QgsWkbTypes.MultiPolygon


  def downloadAndExportShpToCsv(self):
    # expected time: 4min 20sec
    start = time.time()

    # delete all the files in the new directory
    my_func.delete_files_in_directory(self.wkt_csv_directory)

    for data_import_group in self.Data_Import:
      # if data_import_group['name'] in ["OS_petroleum_titles"]:
        # "wa_current_tenements","wa_Tenements_Release_Pending","wa_Tenements_Amalgamation_Pending","wa_Tenement_Restorations_Pending","wa_waptitle","wa_wapapplication","wa_waprelease","wa_wapspaao"
        # if data_import_group['name'] in ['OS_petroleum_titles','OS_petroleum_wells','qld_titles']:
        #   print('Offshore petroleum WFS needs to be exported to shp file manually.')
        # else:

      unzipped_directory = self.unzipped_directory + data_import_group['created_extension']
      my_func.download_unzip_link_manual(data_import_group,self.temp_links,self.zip_file_path,self.manual_directory,unzipped_directory)

      for group in data_import_group['groups']:
        print('working: ' + group['output'])
        self.count += 1
        layer = my_qgis.merge_and_get_layer(data_import_group,group,self.temp_links,self.count,self.merged_file_directory,self.unzipped_directory)
        my_qgis.export_layer_to_csv(layer,self.wkt_csv_directory + group['output'] + '_WKT',group['crs'],self.WkbTypes)
        print('complete: ' + group['output'])

    print('Macro time: %s' %(my_func.time_past(start,time.time())))


# from importlib.machinery import SourceFileLoader
# run_me = SourceFileLoader("download_data", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/download_data.py").load_module()
# run_me.Functions('occurrence').downloadAndExportShpToCsv()

# from importlib.machinery import SourceFileLoader
# run_me = SourceFileLoader("download_data", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/download_data.py").load_module()
# run_me.Functions('tenement').downloadAndExportShpToCsv()
