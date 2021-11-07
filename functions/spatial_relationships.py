import os
import geopandas as gpd
import pandas as pd 
import json
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point
from shapely import wkt
import time
from .timer import time_past
from .directory_files import getJSON
import unittest
from ..update_data import UpdateData

class SpacialRelations:
    def __init__(self, update_data):
        ''' This creates all the geospatial relationships for the data. This includes tenement & occurrence relations, government region, local government and geological
            province relations.
        '''
        func_start = time.time()
        # directories
        self.new_dir = os.path.join(update_data.output_dir,'new')

        # paths
        self.tenement_path = os.path.join(update_data.new_dir,'Tenement.csv')
        self.occurrence_path = os.path.join(update_data.new_dir,'Occurrence_pre.csv')
        self.tenement_occurrence_path = os.path.join(update_data.new_dir,'tenement_occurrence.csv')

        # spatial dataframes
        self.ten_gdf = df_to_geo_df_wkt(pd.read_csv(update_data.tenement_path))
        self.occ_gdf = df_to_geo_df_wkt(pd.read_csv(update_data.occurrence_path))

        # shapefiles
        self.local_gov_gdf = gpd.read_file(os.path.join(update_data.regions_dir,'LocalGovernment.shp'))
        self.gov_region_gdf = gpd.read_file(os.path.join(update_data.regions_dir,'GovernmentRegion.shp'))
        self.geo_province_gdf = gpd.read_file(os.path.join(update_data.regions_dir,'GeologicalProvince.shp'))
        self.geo_state_gdf = gpd.read_file(os.path.join(update_data.regions_dir,'State.shp'))

        # get congif files
        self.region_configs = getJSON(os.path.join(update_data.configs_dir,'region_configs.json'))

        try:
            # create the occurrence tenement relation file
            self.create_tenement_occurrence_file(update_data)
            # create tenement materials files
            self.create_tenement_materials_files(update_data)
            # create the tenement and occurrence regions relations files
            self.create_region_relation_files(update_data)
            # create region wkt files like GeologicalProvince from the shapefiles. Only done if not an update
            self.create_regions_files(update_data)
        except Exception as e:
            print(str(e))

        print('Spatial Relationships object initialized: %s' %(time_past(func_start,time.time())))



    def df_to_geo_df_wkt(self, df):
        # convert the wkt to geospatial data
        # df['geom'] = df['geom'].astype(str)
        df['geom'] = df['geom'].apply(wkt.loads)
        # convert to geopandas df
        gdf = gpd.GeoDataFrame(df, geometry='geom',crs="EPSG:4202")
        return gdf



    def create_tenement_occurrence_file(self, update_data):
        ''' The self.occ_gdf & self.ten_gdf is only the latest update data from the 'new' directory. This data needs to be concatenated
            to the core datasets if it is an update.
            self.occ_gdf: Occurrence_pre.csv from the 'new' directory
            Once there is a complete tenement & occurrence df, they are split by mineral & petroleum. Then, an sjoin is performed to find the related
            mineral tenements with mineral occurrences and the same for petroleum. The two df's are then concatenated.
            Once the sjoin has been performed, the df is filtered for only the new occurrence & tenement ind values which is then saved in the
            'new' directory as tenement_occurrence.csv
        '''
        print('Creating tenement_occurrence.csv file.')
        occ_gdf = update_data.occ_gdf
        ten_gdf = update_data.ten_gdf
        if update_data.isUpdate:
            # if update only then the new data needs to be added to the core data so all new relations are found
            occ_core_df = pd.read_csv(os.path.join(update_data.output_dir,'core','Occurrence.csv'))
            ten_core_df = pd.read_csv(os.path.join(update_data.output_dir,'core','Tenement.csv'))
            # combine the new occurrence & tenement data to the core data to create complete datasets to perform an sjoin on
            occ_gdf, new_occ_df = combine_new_and_core_df(occ_core_df,occ_gdf)
            ten_gdf, new_ten_df = combine_new_and_core_df(ten_core_df,ten_gdf)

        # for some reason if there are 144621 rows in the occurrence_pre file then it fill exit the function. temp fix = remove row
        if len(occ_gdf.index) == 144621:
            print('occurrence df has 144621 rows. last row will be dropped to prevent error.')
            occ_gdf.drop(occ_gdf.tail(1).index,inplace=True)

        ''' split the petroleum & material data in the occurrence & tenement df's. This will prevent material occurrences being attributed petroleum tenements '''
        # Use the TenTypeSimp to find the Material Tenements
        tentypesimp_df = pd.read_csv(os.path.join(update_data.output_dir,'core','TenTypeSimp.csv'),engine='python')
        tentyp_df = pd.read_csv(os.path.join(update_data.output_dir,'core','TenType.csv'),engine='python')

        # split the ten_df into material and petroleum
        min_tentypsimp_df = tentypesimp_df[tentypesimp_df['name'].str.contains("Mineral")]['_id']
        min_tentype_df = tentyp_df[tentyp_df['simple_id'].isin(min_tentypsimp_df)]['_id']

        min_ten_gdf = ten_gdf[ten_gdf['typ_id'].isin(min_tentype_df)]
        pet_ten_gdf = ten_gdf[~ten_gdf['typ_id'].isin(min_tentype_df)]

        # Use the OccTypeSimp to find the Material Occurrences
        occtypesimp_df = pd.read_csv(os.path.join(update_data.output_dir,'core','OccTypeSimp.csv'),engine='python')
        occtyp_df = pd.read_csv(os.path.join(update_data.output_dir,'core','OccType.csv'),engine='python')
        occurrence_typ_df = pd.read_csv(os.path.join(update_data.output_dir,'core','occurrence_typ.csv'),engine='python')

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
        if update_data.isUpdate:
            jgdf = jgdf[(jgdf['tenement_id'].isin(new_ten_df['ind']) | jgdf['occurrence_id'].isin(new_occ_df['ind']))]

        jgdf.to_csv(update_data.tenement_occurrence_path,index=False)




    def combine_new_and_core_df(self, core_df, new_gdf):
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



    def create_tenement_materials_files(self, update_data):
        ''' This uses the tenement_occurrence relationships to create the tenement materials relationship files. All the materials for all the occurrences in
            a given tenement are linked to the tenement. No major materials also occur as a minor material for the same tenement.
            The new occurence material data is firstly added to the core data to make a complete up to date dataset before running the join so no
            data is missed.
        '''
        print('Creating Tenement material files.')
        ten_occ_df = pd.read_csv(update_data.tenement_occurrence_path)
        for category in ['majmat','minmat']:
            occ_material_path = os.path.join(update_data.new_dir,"occurrence_%s.csv"%(category))
            ten_material_path = os.path.join(update_data.new_dir,"tenement_%s.csv"%(category))
            occ_mat_df = pd.read_csv(occ_material_path)

            # If only an update, then I need to concatenate the occurrence material to the core file first.
            if update_data.isUpdate:
                occ_core_material_path = os.path.join(update_data.output_dir,"core","occurrence_%s.csv"%(category))
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




    def create_region_relation_files(self, update_data):
        ''' This finds the spatial relations between the two datasest; the Tenement & Occurrence, then finds the relationships between other
            spatial fields, including government regions, local government & geological provinces.
            self.occ_gdf: new data only
            self.ten_gdf: new data only
            The first part of the one2many code is to assign the correct state to the offshore areas which are assigned as 'AUS_OSPET' in the vba macro
            ??? If function terminates silently
            The second half cleans the offshore and onshore regions so they are not mixed.
            ??? solutions to sjoin silently failing ???
            switch the order of the sjoin
            re-order the dics in the region_configs.json file. The failing may have something to do with the occurrence file
        '''
        print('Creating region relation files.')

        ten_df = update_data.ten_gdf[update_data.ten_gdf['shore_id']=='OFS']['ind']
        # id of the offshore region from the LocalGovernment and GovernmentRegion table/file
        local = 393
        region = 60

        for file in update_data.region_configs['files']:
            print("Working on: %s"%(file['file_name']))

            # set the required data_group df. either 'occurrence' or 'tenement'
            data_group_gdf = update_data.occ_gdf.copy() if file['data_group'] == 'occurrence' else update_data.ten_gdf.copy()

            # perform the sjoin for the one-2-many fields which are appended to the existing data_group_gdf
            if file['type'] == 'one2many':
                for group in file['groups']:
                    print('Working on Occurrence sub group: %s'%(group['region']))
                    # edit columns = true is the state column to update the os values to their appropriate state
                    if group['edit_column']:
                        region_gdf = update_data.geo_state_gdf.copy()
                        gdf_temp = data_group_gdf[data_group_gdf['state_id'] == 'AUS_OSPET'].drop('state_id',1)
                        gdf_trim = data_group_gdf[data_group_gdf['state_id'] != 'AUS_OSPET']
                        # state spatial file
                        jgdf = gpd.sjoin(gdf_temp, region_gdf, how="inner", op='intersects')[['ind','_id']].rename(columns={'_id': 'state_id'})
                        gdf_temp = gdf_temp.merge(jgdf,on=group['merge_on'], how='left')
                        data_group_gdf = pd.concat((gdf_trim,gdf_temp),ignore_index=True)

                    else:
                        region_gdf = update_data.local_gov_gdf.copy() if group['region'] == 'local_government' else update_data.gov_region_gdf.copy()
                        jgdf = gpd.sjoin(region_gdf, data_group_gdf, how="inner", op='intersects')[['_id',group['merge_on']]]
                        jgdf.rename(columns={'_id': group['header']},inplace=True)
                        data_group_gdf = data_group_gdf.merge(jgdf, on=group['merge_on'], how='left')

                df = pd.DataFrame(data_group_gdf)

            # perform an sjoin for the many-2-many fields which will be outputted into another file
            elif file['type'] == 'many2many':
                if file['region'] == 'local_government':
                    region_gdf = update_data.local_gov_gdf.copy()
                elif file['region'] == 'government_region':
                    region_gdf = update_data.gov_region_gdf.copy()
                elif file['region'] == 'geological_province':
                    region_gdf = update_data.geo_province_gdf.copy()

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
                area_df = update_data.local_gov_gdf.copy() if file['region'] == 'local_government' else update_data.gov_region_gdf.copy()
                area_df.drop(columns=['geometry'],inplace=True)
                # merge with the area df to get the states of each area
                merge_reg = pd.merge(df,area_df,left_on=field,right_on='_id').drop(columns=['name','_id'])
                # merge with the tenement df to get the state the tenement id belongs to
                ten_merge = pd.merge(merge_reg,self.ten_gdf,left_on='tenement_id', right_on='ind',how='left')[[field,'tenement_id','state','state_id']]
                # drop rows where the title has regions or local goverments that are outside the state the title belongs to, but keep all offshore titles
                df = ten_merge[(ten_merge['state'] == ten_merge['state_id']) | (ten_merge['state'] == 'AUS_OSPET')][[field,'tenement_id']]

            df.to_csv(os.path.join(update_data.new_dir,"%s.csv"%(file['file_name'])),index=False)



    def create_regions_files(self, update_data):
        ''' convert the 'local government', 'government regions', 'geological provinces' & 'State' shapefiles into wkt files which can be pushed to the
            database. A qgis version is also created.
        '''
        if update_data.isUpdate:
            print('No need to create region files. This is only an update!')
        else:
            print('Creating regions files.')

            dic = {
                'GeologicalProvince': update_data.geo_province_gdf,
                'GovernmentRegion': update_data.gov_region_gdf,
                'LocalGovernment': update_data.local_gov_gdf,
                'State': update_data.geo_state_gdf
            }

            for file in update_data.region_configs['shapes']:
                file_name = file['file_name']
                print('Working on: %s'%(file_name))
                gdf = dic[file_name]
                gdf = gdf[file['columns']]
                gdf['geom'] = gdf['geometry'].apply(lambda x: formatGeomCol(x))
                gdf.drop(columns='geometry',inplace=True)
                gdf.to_csv(os.path.join(update_data.output_dir,'core',"%sSpatial.csv"%(file_name)),index=False)
                # make a qgis compatible copy
                gdf['geom'] = gdf['geom'].apply(lambda x: x.replace('SRID=4202;',''))
                gdf.rename(columns={'geom': 'WKT'},inplace=True)
                gdf.to_csv(os.path.join(update_data.output_dir,'core',"qgis_%sSpatial.csv"%(file_name)),index=False)
                # save non spatial table. Using the spatial version in the filter was too slow
                gdf.drop(columns='WKT',inplace=True)
                gdf.to_csv(os.path.join(update_data.output_dir,'core',"%s.csv"%(file_name)),index=False)



    def formatGeomCol(self, x):
        ''' convert the polygons to multipolygons and add the srid '''
        if type(x) == Polygon:
            x = MultiPolygon([x])
        return "SRID=4202;%s"%(wkt.dumps(x))




class TestSpacialRelations(unittest.TestCase):

    def test_init_works(self):
        try:
            sp = SpacialRelations(UpdateData())
            self.assertTrue(True)
        except Exception as e:
            self.assertTrue(False)



if __name__ == '__main__':
    exit(unittest.main())