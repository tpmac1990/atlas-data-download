import os
import urllib.request
import zipfile
import shutil
import processing
import json
import time


class Functions():

  def __init__(self):
    root_directory = 'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
    self.scripts_directory = root_directory + 'scripts/'
    self.tenements_directory = root_directory + 'Tenements/'
    with open(self.scripts_directory + 'tenement_dl_config.json') as json_file:
      config_file = json.load(json_file)

    self.temp_links = config_file['temp_links']
    self.data_dic = config_file['data_dic']

    self.count = 0

    self.wkt_csv_directory = self.tenements_directory + "new/"
    self.unzipped_directory = self.tenements_directory + "unzipped/"
    self.merged_file_directory = self.tenements_directory + "merged/"
    self.zip_file_path = self.tenements_directory + "spatial_download.zip"



  def set_core_vals(self,dl):
    self.name = dl['name']
    self.data_source = dl['data_source']
    self.import_style = dl['import_style']
    self.link = dl['link']
    self.extension = dl['extension']

  

  def set_group_vals(self,group):
    self.type = group['type']
    self.output = group['output']
    self.crs = QgsCoordinateReferenceSystem(group['crs'])
    self.files = group['files']



  def downloadAndUnzip(self):
    if self.name in ['qld_titles','vic_mineral','vic_petroleum']:
      link = self.temp_links[self.link]
    else: 
      link = self.link

    urllib.request.urlretrieve(link, self.zip_file_path)

    with zipfile.ZipFile(self.zip_file_path, 'r') as zip_ref:
      zip_ref.extractall(self.unzipped_directory)

    os.remove(self.zip_file_path)



  def merge_and_get_layer(self):
    if self.type == 'merge':
      layer = self.create_merge_file()
    elif self.type == 'none':
      layer = self.getLayer(self.files, self.data_source, self.link, self.extension)

    return layer



  def create_merge_file(self):
    LAYERS = []
    merged_file_path = self.create_merge_path()

    if self.data_source == 'ogr':
      for file in self.files:
        layer = self.getLayer(file, self.data_source, '','')
        LAYERS.append(layer)
    
    if self.data_source == 'WFS':
      for file in self.files:
        filename = file.replace(":","_")
        path = self.unzipped_directory + filename
        layer = self.getLayer(file, self.data_source, self.link,'')
        self.export_layer_to_shp(layer,path)
        layer = self.getLayer(filename, 'ogr', '','')
        LAYERS.append(layer)

    parameters = {'LAYERS': LAYERS, 
                  'CRS': 'EPSG:4202', 
                  'OUTPUT': merged_file_path}

    processing.run("qgis:mergevectorlayers", parameters)  

    return QgsVectorLayer(merged_file_path, 'merged_file', 'ogr')



  def create_merge_path(self):
      path = self.merged_file_directory + 'merged_file_' + str(self.count) + '.shp'
      return path



  def getLayer(self, file, data_source, link, extension):
    if data_source == 'ogr':
      path = self.unzipped_directory + extension + file + '.shp'
    elif data_source == 'WFS':
      path = link + file
        
    return QgsVectorLayer(path, file, data_source)



  def export_layer_to_csv(self,layer):
    path = self.wkt_csv_directory + self.output + '_WKT'
    QgsVectorFileWriter.writeAsVectorFormat(layer, 
                                            path, 
                                            "utf-8", 
                                            self.crs, 
                                            "CSV", 
                                            layerOptions=['GEOMETRY=AS_WKT'], 
                                            overrideGeometryType=QgsWkbTypes.MultiPolygon, 
                                            forceMulti = False, 
                                            includeZ = False
                                            )


  def export_layer_to_shp(self,layer,path):
    QgsVectorFileWriter.writeAsVectorFormat(layer, 
                                            path, 
                                            "utf-8",
                                            self.crs,
                                            'ESRI Shapefile', 
                                            )


  def time_past(self,start,end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)


  def delete_files_in_directory(self,directory):
    for file in os.listdir(directory):
        os.remove(directory + file)


class Tenements(Functions):

  def export_csvs(self):
    # expected time: 4min 20sec
    start = time.time()

    # delete all the files in the new directory
    self.delete_files_in_directory(self.wkt_csv_directory)

    for dl in self.data_dic:

      self.set_core_vals(dl)

      if self.import_style == 'link':
        self.downloadAndUnzip()

      for group in dl['groups']:
        print('working: ' + group['output'])
        self.count += 1
        self.set_group_vals(group)
        layer = self.merge_and_get_layer()
        self.export_layer_to_csv(layer)
        print('complete: ' + group['output'])

    print('Macro time: %s' %(self.time_past(start,time.time())))
    # shutil.rmtree(self.unzipped_directory, ignore_errors=True, onerror=None)

Tenements().export_csvs()
