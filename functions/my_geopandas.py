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
    # Convert the geometries field to geom and change the header
    if "geometry" in list(gdf1.columns):
        df = geoDfToDf_wkt(gdf1)
    else:
        df = pd.DataFrame(gdf1)
    
    df.to_csv(os.path.join(self.wkt_csv_dir,group['output'] + '_WKT.csv'),index=False)


def df_to_geo_df_wkt(df):
    # convert the wkt to geospatial data
    # df['geom'] = df['geom'].astype(str)
    df['geom'] = df['geom'].apply(wkt.loads)
    # convert to geopandas df
    gdf = gpd.GeoDataFrame(df, geometry='geom',crs="EPSG:4202")
    return gdf


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


def combine_new_and_core_df(core_df,new_gdf):
    ''' joins the new data and core data to created updated datasets for the tenement_occurrence sjoin '''
    # remove the crs that is required to load the geom into the database
    core_df['geom'] = core_df['geom'].str.replace('SRID=4202;', '')
    # drop the local gov and gov regions columns so it has the same columns as the new df
    if 'localgov_id' in core_df.columns:
        core_df.drop(columns=["localgov_id","govregion_id"],inplace=True)
    # create df with core and new data
    new_df = new_gdf[~new_gdf["ind"].isin(core_df["ind"])]
    updated_df = pd.concat((core_df,new_df),ignore_index=True)
    # load wkt and create geopandas dataframe. we need to make sure the 'geom' type is string
    updated_df['geom'] = updated_df['geom'].astype(str)
    updated_df['geom'] = updated_df['geom'].apply(wkt.loads)
    return (gpd.GeoDataFrame(updated_df,geometry="geom",crs="EPSG:4202"), new_df)



def create_tenement_occurrence_file(self):
    ''' The self.occ_gdf & self.ten_gdf is only the latest update data from the 'new' directory. This data needs to be concatenated
        to the core datasets if it is an update.
        self.occ_gdf: Occurrence_pre.csv from the 'new' directory
        Once there is a complete tenement & occurrence df, they are split by mineral & petroleum. Then, an sjoin is performed to find the related
        mineral tenements with mineral occurrences and the same for petroleum. The two df's are then concatenated.
        Once the sjoin has been performed, the df is filtered for only the new occurrence & tenement ind values which is then saved in the
        'new' directory as tenement_occurrence.csv
    '''
    print('Creating tenement_occurrence.csv file.')
    occ_gdf = self.occ_gdf
    ten_gdf = self.ten_gdf
    if self.isUpdate:
        # if update only then the new data needs to be added to the core data so all new relations are found
        occ_core_df = pd.read_csv(os.path.join(self.output_dir,'core','Occurrence.csv'))
        ten_core_df = pd.read_csv(os.path.join(self.output_dir,'core','Tenement.csv'))
        # combine the new occurrence & tenement data to the core data to create complete datasets to perform an sjoin on
        occ_gdf, new_occ_df = combine_new_and_core_df(occ_core_df,occ_gdf)
        ten_gdf, new_ten_df = combine_new_and_core_df(ten_core_df,ten_gdf)

    # for some reason if there are 144621 rows in the occurrence_pre file then it fill exit the function. temp fix = remove row
    if len(occ_gdf.index) == 144621:
        print('occurrence df has 144621 rows. last row will be dropped to prevent error.')
        occ_gdf.drop(occ_gdf.tail(1).index,inplace=True)

    ''' split the petroleum & material data in the occurrence & tenement df's. This will prevent material occurrences being attributed petroleum tenements '''
    # Use the TenTypeSimp to find the Material Tenements
    tentypesimp_df = pd.read_csv(os.path.join(self.output_dir,'core','TenTypeSimp.csv'),engine='python')
    tentyp_df = pd.read_csv(os.path.join(self.output_dir,'core','TenType.csv'),engine='python')
    
    # split the ten_df into material and petroleum
    min_tentypsimp_df = tentypesimp_df[tentypesimp_df['name'].str.contains("Mineral")]['_id']
    min_tentype_df = tentyp_df[tentyp_df['simple_id'].isin(min_tentypsimp_df)]['_id']

    min_ten_gdf = ten_gdf[ten_gdf['typ_id'].isin(min_tentype_df)]
    pet_ten_gdf = ten_gdf[~ten_gdf['typ_id'].isin(min_tentype_df)]

    # Use the OccTypeSimp to find the Material Occurrences
    occtypesimp_df = pd.read_csv(os.path.join(self.output_dir,'core','OccTypeSimp.csv'),engine='python')
    occtyp_df = pd.read_csv(os.path.join(self.output_dir,'core','OccType.csv'),engine='python')
    occurrence_typ_df = pd.read_csv(os.path.join(self.output_dir,'core','occurrence_typ.csv'),engine='python')

    ''' split the occ_df into material and petroleum '''
    pet_occtypsimp_df = occtypesimp_df[occtypesimp_df['name'].str.contains("Wells")]['_id']
    pet_occtype_df = occtyp_df[occtyp_df['simple_id'].isin(pet_occtypsimp_df)]['_id']
    pet_occ_typ_df = occurrence_typ_df[occurrence_typ_df['occtype_id'].isin(pet_occtype_df)]['occurrence_id']

    pet_occ_gdf = occ_gdf[occ_gdf['ind'].isin(pet_occ_typ_df)]
    min_occ_gdf = occ_gdf[~occ_gdf['ind'].isin(pet_occ_typ_df)]

    # spatial join between material tenement & occurrences and the same for petroleum. Then join them together.
    min_jgdf = gpd.sjoin(min_ten_gdf, min_occ_gdf, how="inner", op="intersects")[['ind_left','ind_right']]
    pet_jgdf = gpd.sjoin(pet_ten_gdf, pet_occ_gdf, how="inner", op="intersects")[['ind_left','ind_right']]

    jgdf = pd.concat([min_jgdf, pet_jgdf], ignore_index=True)
    jgdf.rename(columns={'ind_left': 'tenement_id', 'ind_right': 'occurrence_id'},inplace=True)
    # filter jgdf for only the updating occurrence & tenement ind values
    if self.isUpdate:
        jgdf = jgdf[(jgdf['tenement_id'].isin(new_ten_df['ind']) | jgdf['occurrence_id'].isin(new_occ_df['ind']))]

    jgdf.to_csv(self.tenement_occurrence_path,index=False)




    # # spatial join of the tenement and occurrence file
    # jgdf = gpd.sjoin(ten_gdf, occ_gdf, how="inner", op="intersects")[['ind_left','ind_right']]
    # jgdf.rename(columns={'ind_left': 'tenement_id', 'ind_right': 'occurrence_id'},inplace=True)
    # # filter jgdf for only the updating occurrence & tenement ind values
    # if self.isUpdate:
    #     jgdf = jgdf[(jgdf['tenement_id'].isin(new_ten_df['ind']) | jgdf['occurrence_id'].isin(new_occ_df['ind']))]

    # jgdf.to_csv(self.tenement_occurrence_path,index=False)

    

def create_tenement_materials_files(self):
    ''' This uses the tenement_occurrence relationships to create the tenement materials relationship files. All the materials for all the occurrences in 
        a given tenement are linked to the tenement. No major materials also occur as a minor material for the same tenement.
        The new occurence material data is firstly added to the core data to make a complete up to date dataset before running the join so no 
        data is missed.
    '''
    print('Creating Tenement material files.')
    ten_occ_df = pd.read_csv(self.tenement_occurrence_path)
    for category in ['majmat','minmat']:
        occ_material_path = os.path.join(self.new_dir,"occurrence_%s.csv"%(category))
        ten_material_path = os.path.join(self.new_dir,"tenement_%s.csv"%(category))
        occ_mat_df = pd.read_csv(occ_material_path)

        # If only an update, then I need to concatenate the occurrence material to the core file first.
        if self.isUpdate:
            occ_core_material_path = os.path.join(self.output_dir,"core","occurrence_%s.csv"%(category))
            occ_core_mat_df = pd.read_csv(occ_core_material_path)
            # concatenate the core and new occurrence material files. don't worry about duplicates. they will be deleted when dropping duplicates below.
            occ_mat_df = pd.concat((occ_core_mat_df,occ_mat_df),ignore_index=True)
            
        df = pd.merge(ten_occ_df, occ_mat_df, on='occurrence_id')[['tenement_id','material_id']]
        df.drop_duplicates(inplace=True)
        # store the majmat df to compare to the minmat later and then delete all material codes that already exist in the majmat df
        if category == 'majmat':
            majmat_df = df
        else:
            df = df.merge(majmat_df, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', 1)
            
        df.to_csv(ten_material_path,index=False)


def create_regions_files(self):
    ''' This will convert the 'local government', 'government regions' & 'geological provinces' shapefiles into tables which
        can be pushed to the database
    '''
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
            df.to_csv(os.path.join(self.output_dir,'core',"%s.csv"%(file_name)),index=False)


def create_region_relation_files(self):
    ''' This finds the spatial relations between the two datasest; the Tenement & Occurrence, then finds the relationships between other 
        spatial fields, including government regions, local government & geological provinces.
        self.occ_gdf: new data only
        self.ten_gdf: new data only
    '''
    print('Creating region relation files.')

    for file in self.region_configs['files']:
        print("Working on: %s"%(file['file_name']))

        # set the required data_group df. either 'occurrence' or 'tenement'
        data_group_gdf = self.occ_gdf if file['data_group'] == 'occurrence' else self.ten_gdf
            
        # if file['file_name'] == 'tenement_localgov': print('1')
        # perform the sjoin for the one-2-many fields which are appended to the existing data_group_gdf
        if file['type'] == 'one2many':
            for group in file['groups']:
                if group['region'] == 'local_government':
                    region_gdf = self.local_gov_gdf
                else:
                    region_gdf = self.gov_region_gdf

                jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_id',group['merge_on']]]
                jgdf.rename(columns={'_id': group['header']},inplace=True)
                data_group_gdf = data_group_gdf.merge(jgdf, on=group['merge_on'], how='left')

            df = pd.DataFrame(data_group_gdf)

        # perform an sjoin for the many-2-many fields which will be outputted into another file
        elif file['type'] == 'many2many':
            if file['region'] == 'local_government':
                region_gdf = self.local_gov_gdf
            elif file['region'] == 'government_region':
                region_gdf = self.gov_region_gdf 
            elif file['region'] == 'geological_province':
                region_gdf = self.geo_province_gdf

            if file['file_name'] == 'tenement_localgov': 
                data_group_gdf.drop(data_group_gdf.tail(1).index,inplace=True)
                # print(len(data_group_gdf.index))
            # if file['file_name'] == 'tenement_localgov': print(len(region_gdf.index))
            jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_id',file['merge_on']]]
            # if file['file_name'] == 'tenement_localgov': print('3')
            df = pd.DataFrame(jgdf)
            df.columns = file['headers']

        else:
            print("Relation type in the config file is incorrect.")

        df.to_csv(os.path.join(self.new_dir,"%s.csv"%(file['file_name'])),index=False)




# url = data_import_group['link'] + group['files']

    # # thisxmltodict works here, but not for the aus pretroleum data
    # url = "http://www.mrt.tas.gov.au/web-services/ows"
    # wfs = WebFeatureService(url=url,version='1.1.0')
    # response = wfs.getfeature(typename='mt:MineralTenement',outputFormat='json')
    # data = json.loads(response.read())
    # gdf = gpd.GeoDataFrame.from_features(data['features'])
    # gdf["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in gdf["geometry"]]
    # gdf = gdf[~(gdf["geometry"].is_empty | gdf["geometry"].isna())]
    # df = pd.DataFrame(gdf.assign(geometry=gdf.geometry.apply(wkt.dumps))).rename(columns={"geometry": "geom"})
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
