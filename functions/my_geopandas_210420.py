import os
import geopandas as gpd
import pandas as pd 
import numpy as np
from owslib.wfs import WebFeatureService
import requests
import json
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point
from shapely.geometry.collection import GeometryCollection
from shapely import wkt
from shapely.ops import transform
import warnings


def get_extension(extension,temp_links,data_import_group):
    if extension == 'in_temp_link':
        extension = temp_links[data_import_group['link']].split('/')[-1][:-4] + '/'
    return extension


def convert_data_to_df(self,data_import_group,group,file):
    url = data_import_group['link']
    if data_import_group['data_source'] == 'ogr':
        extension = get_extension(data_import_group['extension'],self.temp_links,data_import_group)
        file_path = os.path.join(self.unzipped_dir,extension,file+'.shp')
        gdf = gpd.read_file(file_path)
    elif data_import_group['data_source'] == 'WFS':
        wfs = WebFeatureService(url=url,version='1.1.0')
        response = wfs.getfeature(typename=file)
        xml_data = response.read()
        with open('data.gml', 'wb') as f1:
            f1.write(xml_data)
        gdf = gpd.read_file('data.gml')
    else:
        print('Incorrect Data Source.')
    return gdf


def merge_and_export_to_csv(self,data_import_group,group):
    warnings.filterwarnings("ignore")
    gdf1 = None

    for file in group['files']:
        gdf = convert_data_to_df(self,data_import_group,group,file)
        if gdf1 is None:
            gdf1 = gdf
        else:
            gdf1 = gpd.GeoDataFrame(pd.concat([gdf1, gdf], ignore_index=True))
        # print(gdf1.head(10))
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

    # print(gdf1.head())
    # Remove all null or empty geometries
    gdf1 = gdf1[~(gdf1["geometry"].is_empty | gdf1["geometry"].isna())]
    # Convert the geometries field to WKT and change the header
    if "geometry" in list(gdf1.columns):
        df = geoDfToDf_wkt(gdf1)
    else:
        df = pd.DataFrame(gdf1)
    
    df.to_csv(os.path.join(self.wkt_csv_dir,group['output'] + '_WKT.csv'),index=False)


def dfToGeoDf_wkt(df):
    # convert the wkt to geospatial data
    df['WKT'] = df['WKT'].apply(wkt.loads)
    # convert to geopandas df
    gdf = gpd.GeoDataFrame(df, geometry='WKT',crs="EPSG:4202")
    return gdf


def geoDfToDf_wkt(gdf):
    df = pd.DataFrame(gdf.assign(geometry=gdf.geometry.apply(wkt.dumps))).rename(columns={"geometry": "WKT"})
    # move the WKT to the first column
    cols = list(df.columns)
    if 'WKT' in cols:
        cols.remove('WKT')
        cols.insert(0,'WKT')
        df = df[cols]
    return df


def create_tenement_occurrence_file(self):
    print('Creating tenement_occurrence.csv file.')
    # If update only, then the new occurrence data needs to be concatenated to the core occurrence data so we can find all the occurrences related to the new tenements.
    if self.isUpdate:
        occ_core_path = os.path.join(self.output_dir,'core/Occurrence.csv')
        df = pd.read_csv(occ_core_path)
        # remove the crs that is required to load the WKT into the database
        df['WKT'] = df['WKT'].str.replace('SRID=4202;', '')
        # drop the local gove and gov regions columns so it has the same columns as the new df
        df.drop(columns=["LOCALGOV","GOVREGION"],inplace=True)
        # get a df of all the new entries
        ids_df = self.occ_gdf[~self.occ_gdf["OCCID"].isin(df["OCCID"].values.tolist())]
        # concatenate this to the core df so it contains all the occurrences
        concat_df = pd.concat((df,ids_df),ignore_index=True)
        concat_df['WKT'] = concat_df['WKT'].astype(str) # need make sure the WKT dtype is string.
        concat_df['WKT'] = concat_df['WKT'].apply(wkt.loads)
        self.occ_gdf = gpd.GeoDataFrame(concat_df,geometry="WKT",crs="EPSG:4202")

    # spatial join of the tenement and occurrence file
    jgdf = gpd.sjoin(self.ten_gdf, self.occ_gdf, how="inner", op='intersects')[['TENID','OCCID']]
    jgdf.index = np.arange(1, len(jgdf)+1)
    jgdf.rename_axis('_ID',inplace=True)
    jgdf.to_csv(self.tenement_occurrence_path) # check the occurrence change file. way too many NSW_1
    print('Complete.')
    

def create_tenement_materials_files(self):
    ten_occ_df = pd.read_csv(self.tenement_occurrence_path)
    for category in ['majmat','minmat']:
        occ_material_path = os.path.join(self.new_dir,"occurrence_%s.csv"%(category))
        ten_material_path = os.path.join(self.new_dir,"tenement_%s.csv"%(category))
        occ_mat_df = pd.read_csv(occ_material_path)

        # If only an update, then i need to concatenate the occurrence material to the core file first.
        if self.isUpdate:
            occ_core_material_path = os.path.join(self.output_dir,"core/occurrence_%s.csv"%(category))
            occ_core_mat_df = pd.read_csv(occ_core_material_path)
            # concatenate the core and new occurrence material files. don't worry about duplicates. they will be deleted when dropping duplicates below.
            occ_mat_df = pd.concat((occ_core_mat_df,occ_mat_df),ignore_index=True)
            # reduced_occ_df = occ_core_mat_df[~occ_core_mat_df["OCCID"].isin(occ_mat_df["OCCID"])] # can delete these two lines
            # occ_mat_df = pd.concat((reduced_occ_df,occ_mat_df),ignore_index=True)
            
        df = pd.merge(ten_occ_df, occ_mat_df, left_on='OCCID', right_on='OCCID')[['TENID','MATERIAL']]
        df.drop_duplicates(inplace=True)
        df.columns = ['TEN_ID','CODE']
        df.index = np.arange(1, len(df)+1)
        df.rename_axis('_ID',inplace=True)
        df.to_csv(ten_material_path)


def create_regions_files(self):
    if self.isUpdate:
        print('No need to create region files. This is only an update!')
    else:
        print('Creating regions files.')

        for file in self.region_configs['shapes']:
            file_name = file['file_name']
            if file_name == 'GeologicalProvince':
                gdf = self.geo_province_gdf
            elif file_name == 'GovernmentRegion':
                gdf = self.gov_region_gdf
            elif file_name == 'LocalGovernment':
                gdf = self.local_gov_gdf

            df = pd.DataFrame(gdf[file['columns']])
            df.to_csv(os.path.join(self.output_dir,'ds_new',"%s.csv"%(file_name)),index=False)
        print('Complete.')


def create_region_relation_files(self):
    print('Creating region relation files.')

    for file in self.region_configs['files']:
        # if file['file_name'] == "occurrence":
        print("Working on: %s"%(file['file_name']))

        if file['data_group'] == 'occurrence': # set the required data_group df
            data_group_gdf = self.occ_gdf
        else:
            data_group_gdf = self.ten_gdf

        if file['type'] == 'one2many':
            for group in file['groups']:
                if group['region'] == 'local_government':
                    region_gdf = self.local_gov_gdf
                else:
                    region_gdf = self.gov_region_gdf

                jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_ID',group['merge_on']]]
                jgdf.rename(columns={'_ID': group['header']},inplace=True)
                data_group_gdf = data_group_gdf.merge(jgdf, on=group['merge_on'], how='left')

            df = pd.DataFrame(data_group_gdf)

        elif file['type'] == 'many2many':
            if file['region'] == 'local_government':
                region_gdf = self.local_gov_gdf
            elif file['region'] == 'government_region':
                region_gdf = self.gov_region_gdf 
            elif file['region'] == 'geological_province':
                region_gdf = self.geo_province_gdf

            jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_ID',file['merge_on']]]
            df = pd.DataFrame(jgdf)
            df.columns = file['headers']
            df.index = np.arange(1, len(df)+1)
            df.rename_axis('_ID',inplace=True)

        else:
            print("Relation type in the config file is incorrect.")

        df.to_csv(os.path.join(self.new_dir,"%s.csv"%(file['file_name'])),index=file['index'])

    print('Complete.')




# url = data_import_group['link'] + group['files']

    # # thisxmltodict works here, but not for the aus pretroleum data
    # url = "http://www.mrt.tas.gov.au/web-services/ows"
    # wfs = WebFeatureService(url=url,version='1.1.0')
    # response = wfs.getfeature(typename='mt:MineralTenement',outputFormat='json')
    # data = json.loads(response.read())
    # gdf = gpd.GeoDataFrame.from_features(data['features'])
    # gdf["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in gdf["geometry"]]
    # gdf = gdf[~(gdf["geometry"].is_empty | gdf["geometry"].isna())]
    # df = pd.DataFrame(gdf.assign(geometry=gdf.geometry.apply(wkt.dumps))).rename(columns={"geometry": "WKT"})
    # df.to_csv('./Roads.csv',index=False)


    # # I can get the xml, but get an error when trying to use 'outputFormat='json'' as above.
    # url = "http://www.mrt.tas.gov.au/web-services/ows"
    # wfs = WebFeatureService(url=url,version='1.1.0')
    # response = wfs.getfeature(typename='mt:MineralTenement')
    # xml_data = response.read()
    # out = open('data.gml', 'wb')
    # out.write(xml_data)
    # out.close()
    # gdf = gpd.read_file('data.gml')
    # cols = list(gdf.columns)
    # cols = [cols[-1]] + cols[:-1]
    # gdf = gdf[cols]
    # print(gdf.head(10))
    

    # # convert shapefile to df
    # gdf = gpd.read_file(os.path.join(self.data_group_dir,'merged/merged_file_3.shp'))
    # cols = list(gdf.columns)
    # cols = [cols[-1]] + cols[:-1]
    # gdf = gdf[cols]
    # print(gdf.head(10))

    # https://dasc.dmp.wa.gov.au/DASC/Download/File/2027
