import os
import urllib.request
import zipfile
import shutil
import processing
import json
import time
import csv
from importlib.machinery import SourceFileLoader
from qgis.core import QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateReferenceSystem


def getLayer(path, file, data_source, link, extension):
    if data_source == 'ogr':
        path = path + extension + file + '.shp'
    elif data_source == 'WFS':
        path = link + file
    elif data_source == 'delimitedtext':
        path = path

    layer = QgsVectorLayer(path, file, data_source)
    print('Layer created containing %s lines.' %(layer.featureCount()))
    return layer



def exportAsShp(layer,output_path,crs):
    crs = QgsCoordinateReferenceSystem(crs)
    QgsVectorFileWriter.writeAsVectorFormat(layer, 
                                            output_path, 
                                            "utf-8",
                                            crs,
                                            'ESRI Shapefile', 
                                            )
    print('Exported %s layer as shape file' %(layer.name()))



def export_layer_to_csv(layer,output_path,crs,geomType):
    crs = QgsCoordinateReferenceSystem(crs)
    QgsVectorFileWriter.writeAsVectorFormat(layer, 
                                            output_path, 
                                            "utf-8", 
                                            crs, 
                                            "CSV", 
                                            layerOptions=['GEOMETRY=AS_WKT'], 
                                            overrideGeometryType=geomType, 
                                            forceMulti = False, 
                                            includeZ = False
                                            )


def simplifyGeometry(input_path, output_path, tolerance):
    print('Simplifying polygons')

    layer = QgsVectorLayer(input_path, 'geom_file', 'ogr')

    parameters = {'INPUT': layer, 
                'TOLERANCE': tolerance, 
                'OUTPUT': output_path}

    processing.run("qgis:simplifygeometries", parameters)
    print('Complete.')

def get_extension(extension,temp_links,data_import_group):
    if extension == 'in_temp_link':
        extension = temp_links[data_import_group['link']].split('/')[-1][:-4] + '/'
    return extension

def merge_and_get_layer(data_import_group,group,temp_links,count,merged_file_directory,unzipped_directory):
    extension = get_extension(data_import_group['extension'],temp_links,data_import_group)
    if group['type'] == 'merge':
        layer = create_merge_file(data_import_group,group,extension,count,merged_file_directory,unzipped_directory)
    elif group['type'] == 'none':
        layer = getLayer(unzipped_directory,group['files'], data_import_group['data_source'], data_import_group['link'], extension)

    return layer


def create_merge_file(data_import_group,group,extension,count,merged_file_directory,unzipped_directory):
    LAYERS = []
    merged_file_path = merged_file_directory + 'merged_file_' + str(count) + '.shp'

    data_source = data_import_group['data_source']
    files = group['files']

    if data_source == 'ogr':
        for file in files:
            layer = getLayer(unzipped_directory,file, data_source, '',extension)
            LAYERS.append(layer)
    
    if data_source == 'WFS':
        for file in files:
            filename = file.replace(":","_")
            path = unzipped_directory + filename
            layer = getLayer('',file, data_source, data_import_group['link'],'')
            exportAsShp(layer,path,group['crs'])
            layer = getLayer(path,'', 'ogr', '','')
            LAYERS.append(layer)

    parameters = {'LAYERS': LAYERS, 
                  'CRS': 'EPSG:4202', 
                  'OUTPUT': merged_file_path}

    processing.run("qgis:mergevectorlayers", parameters)  

    return QgsVectorLayer(merged_file_path, 'merged_file', 'ogr')
