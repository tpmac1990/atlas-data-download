import time
import os
import ctypes
import geopandas as gpd
import pandas as pd
from owslib.wfs import WebFeatureService
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point
from shapely.geometry.collection import GeometryCollection
from shapely import wkt
from shapely.ops import transform
import warnings

from .directory_files import (delete_files_in_directory, fileExist, download_unzip_link_manual, getJSON)



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

            # unzipped_dir = os.path.join(self.unzipped_dir, data_import_group['created_extension']) # gets the location of the required file
            # downloads and extracts the data for all the zip files. If the link fails, then it will be added to the download_fail.csv and the formatting will be skipped.
            if download_unzip_link_manual(self,data_import_group):

                for group in data_import_group['groups']:
                    print("working on: %s"%(group['output']))
                    self.count += 1
                    # Merges the files where necessary and export to csv with WKT
                    merge_and_export_to_csv(self,data_import_group,group)

    print('Data Download time: %s' %(time_past(func_start,time.time())))




def merge_and_export_to_csv(self,data_import_group,group):
    warnings.filterwarnings("ignore")
    gdf1 = None

    for file in group['files']:
        gdf = convert_data_to_df(self,data_import_group,group,file)
        gdf1 = gdf if gdf1 is None else gpd.GeoDataFrame(pd.concat([gdf1, gdf], ignore_index=True))
        print(len(gdf1.index))
    
    # convert Polygon data to Multipolygon format and POINT Z to POINT
    if self.data_group == "tenement":
        lst = []
        for feature in gdf1["geometry"]:
            if type(feature) == Polygon:
                lst.append(MultiPolygon([feature]))
            elif type(feature) == GeometryCollection:
                lst.append(feature[0])
            else:
                lst.append(feature)
        gdf1["geometry"] = lst
        # gdf1["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in gdf1["geometry"]]
    if self.data_group == "occurrence":
        gdf1["geometry"] = [transform(lambda x, y, z: (x, y), feature) if feature.has_z else feature for feature in gdf1["geometry"]]

    # Remove all null or empty geometries
    gdf1 = gdf1[~(gdf1["geometry"].is_empty | gdf1["geometry"].isna())]
    # Convert the geometries field to geom and change the header
    df = geoDfToDf_wkt(gdf1) if "geometry" in list(gdf1.columns) else pd.DataFrame(gdf1)
    
    df.to_csv(os.path.join(self.wkt_csv_dir,group['output'] + '_WKT.csv'),index=False)




def convert_data_to_df(self,data_import_group,group,file):
    url = data_import_group['link']
    if data_import_group['data_source'] == 'ogr':
        extension = get_extension(data_import_group['extension'],self.temp_links,data_import_group)
        file_path = os.path.join(self.unzipped_dir,extension,file+'.shp')
        gdf = gpd.read_file(file_path)
    elif data_import_group['data_source'] == 'WFS':
        wfs = WebFeatureService(url=url,version=data_import_group['wfs_version'])
        response = wfs.getfeature(typename=file)
        xml_data = response.read()
        with open('data.gml', 'wb') as f1:
            f1.write(xml_data)
        gdf = gpd.read_file('data.gml')
    else:
        print('Incorrect Data Source.')
    return gdf


def get_extension(extension,temp_links,data_import_group):
    if extension == 'in_temp_link':
        extension = temp_links[data_import_group['link']].split('/')[-1][:-4] + '/'
    return extension


def geoDfToDf_wkt(gdf):
    df = pd.DataFrame(gdf.assign(geometry=gdf.geometry.apply(wkt.dumps)))
    if 'geometry' in df.columns:
        df.rename(columns={"geometry": "geom"})
    # move the geom to the first column
    cols = list(df.columns)
    if 'geom' in cols:
        cols.remove('geom')
        cols.insert(0,'geom')
        df = df[cols]
    return df