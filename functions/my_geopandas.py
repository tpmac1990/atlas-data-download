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
        wfs = WebFeatureService(url=url,version=data_import_group['wfs_version'])
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
    ''' convert the 'local government', 'government regions', 'geological provinces' & 'State' shapefiles into wkt files which can be pushed to the 
        database. There a qgis version is also created.
    '''
    if self.isUpdate:
        print('No need to create region files. This is only an update!')
    else:
        print('Creating regions files.')

        dic = {
            'GeologicalProvince': self.geo_province_gdf,
            'GovernmentRegion': self.gov_region_gdf,
            'LocalGovernment': self.local_gov_gdf,
            'State': self.geo_state_gdf
        }

        for file in self.region_configs['shapes']:
            file_name = file['file_name']
            print('Working on: %s'%(file_name))
            gdf = dic[file_name]
            gdf = gdf[file['columns']]
            gdf['geom'] = gdf['geometry'].apply(lambda x: formatGeomCol(x))
            gdf.drop(columns='geometry',inplace=True)
            gdf.to_csv(os.path.join(self.output_dir,'core',"%sSpatial.csv"%(file_name)),index=False)
            # make a qgis compatible copy
            gdf['geom'] = gdf['geom'].apply(lambda x: x.replace('SRID=4202;',''))
            gdf.rename(columns={'geom': 'WKT'},inplace=True)
            gdf.to_csv(os.path.join(self.output_dir,'core',"qgis_%sSpatial.csv"%(file_name)),index=False)
            # save non spatial table. Using teh spatial version in the filter was too slow
            gdf.drop(columns='WKT',inplace=True)
            gdf.to_csv(os.path.join(self.output_dir,'core',"%s.csv"%(file_name)),index=False)

# convert the polygons to multipolygons and add the srid
def formatGeomCol(x):
    if type(x) == Polygon:
        x = MultiPolygon([x])
    return "SRID=4202;%s"%(wkt.dumps(x))


def create_region_relation_files(self):
    ''' This finds the spatial relations between the two datasest; the Tenement & Occurrence, then finds the relationships between other 
        spatial fields, including government regions, local government & geological provinces.
        self.occ_gdf: new data only
        self.ten_gdf: new data only
        The first part of the one2many code is to assign the correct state to the offshore areas which are assigned as 'OS' in the vba macro
        ??? If function terminates silently
        The second half cleans the offshore and onshore regions so they are not mixed.
        ??? solutions to sjoin silently failing ???
        switch the order of the sjoin
        re-order the dics in the region_configs.json file. The failing may have something to do with the occurrence file
    '''
    print('Creating region relation files.')

    ten_df = self.ten_gdf[self.ten_gdf['shore_id']=='OFS']['ind']
    # id of the offshore region from the LocalGovernment and GovernmentRegion table/file
    local = 393
    region = 60

    for file in self.region_configs['files']:
        print("Working on: %s"%(file['file_name']))

        # set the required data_group df. either 'occurrence' or 'tenement'
        data_group_gdf = self.occ_gdf.copy() if file['data_group'] == 'occurrence' else self.ten_gdf.copy()
            
        # perform the sjoin for the one-2-many fields which are appended to the existing data_group_gdf
        if file['type'] == 'one2many':
            for group in file['groups']:
                print('Working on Occurrence sub group: %s'%(group['region']))
                # edit columns = true is the state column to update the os values to their appropriate state
                if group['edit_column']:
                    region_gdf = self.geo_state_gdf.copy()
                    gdf_temp = data_group_gdf[data_group_gdf['state_id'] == 'OS'].drop('state_id',1)
                    gdf_trim = data_group_gdf[data_group_gdf['state_id'] != 'OS']
                    jgdf = gpd.sjoin(gdf_temp, region_gdf, how="inner", op='intersects')[['ind','code']].rename(columns={'code': 'state_id'})
                    gdf_temp = gdf_temp.merge(jgdf,on=group['merge_on'], how='left')
                    data_group_gdf = pd.concat((gdf_trim,gdf_temp),ignore_index=True)
                    
                else:
                    region_gdf = self.local_gov_gdf.copy() if group['region'] == 'local_government' else self.gov_region_gdf.copy()
                    jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_id',group['merge_on']]] 
                    jgdf.rename(columns={'_id': group['header']},inplace=True)
                    data_group_gdf = data_group_gdf.merge(jgdf, on=group['merge_on'], how='left')

            df = pd.DataFrame(data_group_gdf)

        # perform an sjoin for the many-2-many fields which will be outputted into another file
        elif file['type'] == 'many2many':
            if file['region'] == 'local_government':
                region_gdf = self.local_gov_gdf.copy()
            elif file['region'] == 'government_region':
                region_gdf = self.gov_region_gdf.copy() 
            elif file['region'] == 'geological_province':
                region_gdf = self.geo_province_gdf.copy()

            # For some reason I get a silent error unless I remove the tail row from tenement_localgov
            # if file['file_name'] == 'tenement_localgov': 
            #     data_group_gdf.drop(data_group_gdf.tail(20).index,inplace=True)

            jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_id',file['merge_on']]]
            df = pd.DataFrame(jgdf)
            df.columns = file['headers']

        else:
            print("Relation type in the config file is incorrect.")

        # titles on the edge of onshore and offshore sometimes have both in their local government and regions. This fixes this
        if file['check_shore']:
            offshore_id = local if file['region'] == 'local_government' else region
            field = '%s_id'%(file['region'].replace("_",""))
            # get all the keys for offshore titles from the tenement df
            os_reg_keys = df[df[field] == offshore_id]['tenement_id']
            # df of all the rows that don't need any adjustments
            remain_df = df[~df['tenement_id'].isin(os_reg_keys)]
            # all the rows that might need adjustment
            to_fix_df = df[df['tenement_id'].isin(os_reg_keys)]
            # removes all local government & regions tat aren't offshore for offshore titles
            is_os_df = to_fix_df[(to_fix_df['tenement_id'].isin(ten_df)) & (to_fix_df[field] == offshore_id)]
            # removes offshore region for all onshore titles
            not_os_df = to_fix_df[(~to_fix_df['tenement_id'].isin(ten_df)) & (to_fix_df[field] != offshore_id)]
            # concatentates the three df's together to recreate the df to save
            df = pd.concat((remain_df,is_os_df,not_os_df),ignore_index=True)

            # get the area df. use copy so prevent changes being made to the region df's
            area_df = self.local_gov_gdf.copy() if file['region'] == 'local_government' else self.gov_region_gdf.copy()
            area_df.drop(columns=['geometry'],inplace=True)
            # merge with the area df to get the states of each area
            merge_reg = pd.merge(df,area_df,left_on=field,right_on='_id').drop(columns=['name','_id'])
            # merge with the tenement df to get the state the tenement id belongs to
            ten_merge = pd.merge(merge_reg,self.ten_gdf,left_on='tenement_id', right_on='ind',how='left')[[field,'tenement_id','state','state_id']]
            # drop rows where the title has regions or local goverments that are outside the state the title belongs to, but keep all offshore titles
            df = ten_merge[(ten_merge['state'] == ten_merge['state_id']) | (ten_merge['state'] == 'OS')][[field,'tenement_id']]

        df.to_csv(os.path.join(self.new_dir,"%s.csv"%(file['file_name'])),index=False)




def create_qgis_spatial_files(self):
    ''' create the qgis compatible files for the tenement & occurrence files '''
    print("Creating qgis compatible files")

    for directory in [self.core_dir,self.new_dir]:
        for file in ['Occurrence','Tenement']:
            df = pd.read_csv(os.path.join(directory,'%s.csv'%(file)))
            df['geom'] = df['geom'].apply(lambda x: x.replace("SRID=4202;",""))
            df.rename(columns={'geom': 'WKT'}, inplace=True)
            df.to_csv(os.path.join(directory,'qgis_%s.csv'%(file)),index=False)





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



# functions to build the local government shp files
def build_local_gov_files(self):

    # C:\Django_Projects\03_geodjango\Atlas\datasets\Raw_datasets\regions\local_government
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # lst = [ 
#     #     [" Shire", " Shire Council"]
#     # ]

#     # gdf = gpd.read_file(os.path.join(self.region,'local_government','local_33.shp'))
    gdf = gpd.read_file(os.path.join(self.region,'State.shp'))

#     # gdf['name'] = gdf['name'].sort_values(axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last', ignore_index=True, key=None)

    # gdf.sort_values(by=['name'],ascending=False,inplace=True)

    # gdf['_id'] = np.arange(1, len(gdf.index) + 1)

#     # gdf['name'] = gdf['name'].apply(lambda x: update_string(x))
    gdf.rename(columns={'state':'code'},inplace=True)

    gdf.to_file(os.path.join(self.region,'State.shp'))

#     gdf.drop(columns=['geometry'],inplace=True)

    # gdf.to_csv(os.path.join(self.region,'LocalGovernment.csv'),index=False)

    # print(gdf.head())



# def update_string(x):
#     if x.endswith(' Shire'):
#         x = "%s Council"%(x)
#     return x
#     # for i in lst:
#     #     if x.startswith(i[0]):
#     #         x = "%s %s"%(x.replace(i[0],''),i[1])

# #     return x

    # gdf_geom = gpd.read_file(os.path.join(self.standard,'NT_LOCALITY_POLYGON_shp.shp'))[['geometry','LOC_PID']]
    # gdf_name = gpd.read_file(os.path.join(self.standard,'NT_LOCALITY_shp.dbf'))[['NAME','LOC_PID']]

    # merge = gdf_geom.merge(gdf_name, on='LOC_PID').drop('LOC_PID',1)

    # merge['NAME'] = merge['NAME'].apply(lambda x: x.capitalize())

    # # print(merge.head())

    # merge.to_file(os.path.join(self.new_local,'nt_local.shp'))
    

    # df = pd.read_csv(os.path.join(self.core,'Occurrence.csv'))
    # # gdf = gp.read_csv(os.path.join(self.core,'Occurrence.csv'))

    # df.rename(columns={'geom':'WKT'},inplace=True)

    # df['WKT'] = df['WKT'].apply(lambda x: x.replace('SRID=4202;',''))

    # df.to_csv(os.path.join(self.core,'qgis_Occurrence.csv'))


    