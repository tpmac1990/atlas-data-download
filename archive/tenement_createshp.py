import csv
from importlib.machinery import SourceFileLoader

my_qgis = SourceFileLoader("pqgis_functions", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/pqgis_functions.py").load_module()
my_func = SourceFileLoader("functions", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/functions.py").load_module()

class Functions():

    def __init__(self,data_group):
        root_directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
        self.output_directory = root_directory + 'output/'
        self.data_group = data_group
        self.shp_directory = "%sshapefiles/shapefiles/wkts/data/%s/" %(root_directory,data_group)
        self.crs = "EPSG:4202"


    def convertCsvToShp(self):
        # The Tenement_wkt.csv needs to be converted manually. The date values cannot be outputted using pyqgis
        # There doesn't seem to be any problem with the wkt
        # I could load the problem VIC and OS datasets alone
        # solution may be separate the states, load them separately, and then merge them

        if self.data_group == 'tenement':
            file_path = self.output_directory + 'new/Tenement_wkt.csv'
            unsimplified_path = self.shp_directory + 'tenement_unsimplified.shp'
            simplified_path = self.shp_directory + 'tenement.shp'
        else:
            file_path = self.output_directory + 'new/Occurrence_pre.csv'
            simplified_path = self.shp_directory + 'occurrence.shp'

        uri = "file:///%s?delimiter=%s&crs=epsg:4202&wktField=%s" % (file_path, ",", "WKT")

        # # Add to map and then create layer of it
        # layer = iface.addVectorLayer(uri, 'spatial_data', "delimitedtext")

        if self.data_group == 'tenement':
            # layer = my_qgis.getLayer(uri,'tenement_unsimplified','delimitedtext','','')
            # print(layer.isValid())
            # # QgsProject.instance().addMapLayer(layer)
            # my_qgis.exportAsShp(layer,unsimplified_path,self.crs)
            layer = my_qgis.getLayer(unsimplified_path,'','ogr','','')
            my_qgis.simplifyGeometry(unsimplified_path,simplified_path,'0.000001')
        else:
            layer = my_qgis.getLayer(uri,'occurrence','delimitedtext','','')
            print(layer.isValid())
            my_qgis.exportAsShp(layer,simplified_path,self.crs)

# Functions('occurrence').convertCsvToShp()

Functions('tenement').convertCsvToShp()

# from importlib.machinery import SourceFileLoader
# run_me = SourceFileLoader("download_data", "C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/scripts/tenement_createshp.py").load_module()
# run_me.Functions('tenement').convertCsvToShp()