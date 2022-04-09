# import time
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
import logging
from datetime import datetime, timedelta
import shutil
import ssl
import zipfile
import urllib.request
import requests
import fiona

from functions.common.directory_files import delete_files_in_directory, file_exist, get_json, write_json

from functions.common.timer import Timer
from .schedule import Schedule
from ..setup import SetUp, Logger



class DownloadSetUp:
    temp_links = get_json(os.path.join(SetUp.configs_dir,'temp_links_config.json'))
    format_configs = get_json(os.path.join(SetUp.configs_dir,'formatting_config.json'))
    download_schedule_path = os.path.join(SetUp.configs_dir,'download_schedule.json')
    dl_schedule_config = get_json(download_schedule_path)
    download_configs = get_json(os.path.join(SetUp.configs_dir,'download_config.json'))
    
    def __init__(self,data_group):
        self.data_group_dir = os.path.join(SetUp.input_dir,data_group)
        self.Data_Import = self.download_configs[data_group]
        self.grp_format_configs = self.format_configs[data_group]
        self.files_to_format_lst = [x for x in self.grp_format_configs if self.grp_format_configs[x]['preformat']]
        self.grp_schedule_config = self.dl_schedule_config[data_group]

        self.manual_dir = os.path.join(self.data_group_dir,'manual')
        self.wkt_csv_dir = os.path.join(self.data_group_dir,'new')
        self.unzipped_dir = os.path.join(self.data_group_dir,'unzipped')
        self.merged_file_dir = os.path.join(self.data_group_dir,'merged')
        self.zip_file_path = os.path.join(self.data_group_dir,'spatial_download.zip')
        self.archive_dir = os.path.join(SetUp.archive_dir,SetUp.tDate,'input',data_group) # this is the only place that uses the date from setup. use date from run_tracker as this can be maintained using testing
        self.data_group = data_group



class DataDownload:

    def all_data_download(self):
        try:
            self.data_download()
            # update the last_download_date in run_tracker to todays date.
            self.update_download_date()
        except:
            # dbu.restore_data()
            raise


    def data_download(self):
        timer = Timer()
        Logger.logger.info(f"\n\n{Logger.hashed}\nData Download\n{Logger.hashed}")
        self.Schedule = Schedule()
        
        for data_group in SetUp.data_groups:
            Logger.logger.info(f"\n{Logger.dashed} {data_group} {Logger.dashed}")

            grp_setup = DownloadSetUp(data_group)

            dl_funcs = DownloadFunctions(data_group)

            # get the list of groups & files that are required for download and update today
            download_datagroups, file_dl_lst = dl_funcs.get_groups_that_require_downloading(data_group)

            # delete all the files in the new directory
            try:
                delete_files_in_directory(grp_setup.wkt_csv_dir)
            except FileNotFoundError:
                Logger.logger.exception(f'Could not locate directory {grp_setup.wkt_csv_dir} to delete files within')
                raise FileNotFoundError(f'directory does not exist: {grp_setup.wkt_csv_dir}')

            # record the files that failed to load. these will be added to the download_schedule config today list to try them again on next download
            failed_file_lst = []
            failed_count = 0
            # loops through each of the groups in the configs.json file for the current data_group
            # for data_import_group in grp_setup.Data_Import:
            for data_import_group in download_datagroups:    

                # downloads and extracts the data for all the zip files. If the link fails, then it will be added to the download_fail.csv and the formatting will be skipped.
                failed_file_lst += dl_funcs.download_unzip_link_manual(data_import_group)

                for group in data_import_group['groups']:
                    # if group['output'] in ['VIC_1','VIC_2']:
                    file_name = group['output']
                    # Merges the files where necessary and export to csv with WKT
                    try:
                        df = dl_funcs.merge_and_export_to_csv(data_import_group,group)
                    except:
                        # continue to the next if error, the error has been logged during the function above
                        failed_file_lst.append(file_name)
                        continue

                    # only continue if df is not empty
                    if df.empty:
                        # no point in adding empty tables
                        Logger.logger.warning(f"Successfully downloaded '{file_name}', but it contains '0' rows. Not exported to input directory")
                        # remove the file from the schedule so no further steps are attempted on it. It is not added to 'today' as there is no point running it again on next download
                        file_dl_lst.remove(file_name)

                    else:
                        # check if all the required fields exist in the df. If any are missing then raise an exception
                        # if file_name != 'SA_2': # SA_2 doesn't exist in formatting configs
                        required_fields_lst = grp_setup.grp_format_configs[file_name]['required_fields']
                        if file_name in grp_setup.files_to_format_lst:
                            required_fields_lst.remove('NEW_IDENTIFIER')
                        for required_field in required_fields_lst:
                            if not required_field in df.columns:
                                Logger.logger.error(f"Required field '{required_field}' is missing in '{file_name}'")
                                # add file name to the failed_list to add it to the download_schedule today list so it will be attempted again tomorrow
                                failed_file_lst.append(file_name)

                        # save to csv
                        df.to_csv(os.path.join(grp_setup.wkt_csv_dir,file_name + '_WKT.csv'),index=False)
                        Logger.logger.info(f"Successfully downloaded '{file_name}' with '{len(df.index)}' rows")

            # update 'today' key in the download_schedule file. erase the current values and add incorrectly formatted values so they can be attempted again next time
            grp_setup.grp_schedule_config['today'] = failed_file_lst
            # set all the files that will be delt with today to 'now' key. This list is used to determine which files to look for later
            grp_setup.grp_schedule_config['now'] = [i for i in file_dl_lst if i not in failed_file_lst and i in grp_setup.files_to_format_lst]
            # overwrite the data groups download schedule config
            DownloadSetUp.dl_schedule_config[data_group] = grp_setup.grp_schedule_config
            failed_count += len(failed_file_lst)

        # log the files and the count that were not successful
        if failed_count > 0:
            Logger.logger.error(f"'{failed_count}' files failed to download")
        else:
            Logger.logger.info("All files downloaded successfully")

        # update the 'last_run' to todays date
        DownloadSetUp.dl_schedule_config['last_run'] = datetime.now().strftime('%d-%m-%Y')
        # update the 'download_schedule' config file
        self.Schedule.update_schedule(DownloadSetUp.dl_schedule_config)

        Logger.logger.info("Data Download duration: '%s'"%(timer.time_past()))




class DownloadFunctions():

    def __init__(self, data_group):
        self.grp_setup = DownloadSetUp(data_group)


    def get_groups_that_require_downloading(self, data_group):
        ''' find the files that require downloading using the download_schedule. If this is the first download then all files wll be 
            downloaded to create the initial dataset. the download_schedule file contain a 'last_run' config, this function will get all the dates between 
            that date and todays and add the missed downloads from the past days to the download groups list.
            run_only_todays = true: download only the files associated with the list of files in 'today' config. This is useful if you only 
            want to download specific files at any given time.
            run_only_todays = false: download all files that are scheduled. 'today' files, if weekday then download
                the daily files, if tuesday then include the weekly downloads, if first business day of the month then download the 
                monthly data
        '''
        # get the paths and congfigs for target data_group
        grp_setup = self.grp_setup

        # create list of the files to download
        if not SetUp.isUpdate:
            # if first download then download all of the data
            data_import_lst = grp_setup.Data_Import
            # get all the output file names
            file_dl_lst = get_all_ouput_file_names(data_import_lst)
            # file_dl_lst = ["NT_2"]
            # print(file_dl_lst)
        else:
            x = datetime.now()
            # find the first business day, starting from tuesday, of the month. Used to determine if monthly data needs to be downloaded
            for i in [1,2,3,4]:
                business_day = datetime(x.year, x.month, i).weekday()
                if business_day > 0 and business_day < 5:
                    first_business_day = i
                    break

            # configs for the data_group that need to be downloaded
            datagroup_schedule = DownloadSetUp.dl_schedule_config[data_group]
            # add the 'today' values to every download
            file_dl_lst = datagroup_schedule['today']

            # get the dat the download was last run
            last_run = datetime.strptime(DownloadSetUp.dl_schedule_config['last_run'],'%d-%m-%Y').date()
            todays_date = x.date()
            # get list of datetime objects of dates between and including today since last run
            dates_lst = [last_run+timedelta(days=x+1) for x in range((todays_date-last_run).days)]

            # loop through all the dates since the last time run, this will gather all the files that require downloading for today and the days missed
            for req_date in dates_lst:
                if not DownloadSetUp.dl_schedule_config['run_only_todays']:
                    if req_date.weekday() > 0 and req_date.weekday() < 5:
                        file_dl_lst += datagroup_schedule['daily']
                    if req_date.weekday() == 1:
                        file_dl_lst += datagroup_schedule['weekly']
                    if req_date.day == first_business_day:
                        file_dl_lst += datagroup_schedule['monthly']
            # remove any duplicates
            file_dl_lst = list(set(file_dl_lst))

            # if an update then download only the required files recorded in the file_dl_lst file
            data_import_lst = []
            for data_import_group in grp_setup.Data_Import:
                for line in data_import_group['groups']:
                    if line['output'] in file_dl_lst and not data_import_group in data_import_lst:
                        data_import_lst.append(data_import_group)
                        continue

        return data_import_lst, file_dl_lst



    def download_unzip_link_manual(self,data_import_group):
        grp_setup = self.grp_setup

        failed_files_lst = []
        if data_import_group['data_source'] != 'WFS':
            files_lst = [item['output'] for item in data_import_group['groups']]
            try:
                link = data_import_group['link']
                # find the temporary link if it is a temporary link
                if link in list(DownloadSetUp.temp_links.keys()):
                    link = DownloadSetUp.temp_links[link]
                # download and unzip the files from the link
                unzipped_directory = os.path.join(grp_setup.unzipped_dir,data_import_group['created_extension'])
                self.download_and_unzip(link,grp_setup.zip_file_path,unzipped_directory)
                # delete the no longer required zip file
                os.remove(grp_setup.zip_file_path)
                Logger.logger.info("Link was successful for group '%s'. Building files '%s'"%(data_import_group['name'], ','.join(files_lst)))
            except (urllib.error.HTTPError, urllib.error.URLError, ValueError):
                failed_files_lst += files_lst
                Logger.logger.error("Link failed for group '%s'. Not possible to build files '%s'. Check download_config.json file"%(data_import_group['name'], ','.join(files_lst)))
        return failed_files_lst



    def merge_and_export_to_csv(self,data_import_group,group):
        warnings.filterwarnings("ignore")

        grp_setup = self.grp_setup
        gdf1 = None

        for file in group['files']:
            gdf = self.convert_data_to_df(data_import_group,group,file)
            gdf1 = gdf if gdf1 is None else gpd.GeoDataFrame(pd.concat([gdf1, gdf], ignore_index=True))

        # if the df is empty then skip this part
        if gdf1.empty:
            df = pd.DataFrame()
        else:
            # convert Polygon data to Multipolygon format and POINT Z to POINT
            if grp_setup.data_group == "tenement":
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
            if grp_setup.data_group == "occurrence":
                gdf1["geometry"] = [transform(lambda x, y, z: (x, y), feature) if feature.has_z else feature for feature in gdf1["geometry"]]

            # Remove all null or empty geometries
            gdf1 = gdf1[~(gdf1["geometry"].is_empty | gdf1["geometry"].isna())]
            # Convert the geometry field to wkt
            df = geoDfToDf_wkt(gdf1) if "geometry" in list(gdf1.columns) else pd.DataFrame(gdf1)

        return df



    def convert_data_to_df(self,data_import_group,group,file):
        grp_setup = self.grp_setup

        url = data_import_group['link']
        try:
            if data_import_group['data_source'] == 'ogr':
                extension = data_import_group['extension']
                # if the extension is 'in_temp_link' then the extension changes when the temporary link changes. This gets the extension from mthat link
                if extension == 'in_temp_link':
                    extension = grp_setup.temp_links[data_import_group['link']].split('/')[-1][:-4] + '/'

                file_path = os.path.join(grp_setup.unzipped_dir,extension,file+'.shp')
                # read the file to geopandas df
                try:
                    gdf = gpd.read_file(file_path)
                except fiona.errors.DriverError:
                    message = f"No such file or directory: {file_path}. This occurred in group '{data_import_group['name']}'"
                    raise

            elif data_import_group['data_source'] == 'WFS':
                # wfs fail error: requests.exceptions.ReadTimeout: HTTPSConnectionPool(host='www.mrt.tas.gov.au', port=443): Read timed out. (read timeout=30)
                try:
                    wfs = WebFeatureService(url=url,version=data_import_group['wfs_version'])
                    response = wfs.getfeature(typename=file)
                except requests.exceptions.ReadTimeout:
                    message = f"Failed data download for group '{data_import_group['name']}' with data_source {data_import_group['data_source']}"
                    raise

                xml_data = response.read()
                with open('data.gml', 'wb') as f1:
                    f1.write(xml_data)
                gdf = gpd.read_file('data.gml')

                Logger.logger.info(f"WFS was successful for group '{data_import_group['name']}'. File '{file}'")

            else:
                message = f"The 'data_source' value '{{data_import_group['data_source']}}' in the download_configs is incorrect for group {data_import_group['name']}"
                raise

        except Exception:
            Logger.logger.exception(message)
            raise Exception('Failed to save geopandas dataframe')

        return gdf



    # def geoDfToDf_wkt(self,gdf):
    #     df = pd.DataFrame(gdf.assign(geometry=gdf.geometry.apply(wkt.dumps)))
    #     if 'geometry' in df.columns:
    #         df.rename(columns={"geometry": "geom"})
    #     # move the geom to the first column
    #     cols = list(df.columns)
    #     if 'geom' in cols:
    #         cols.remove('geom')
    #         cols.insert(0,'geom')
    #         df = df[cols]
    #     return df



    def download_and_unzip(self,link,file_path,output_directory):
        # temp fix for SSL Certificate error. see here https://pretagteam.com/question/how-to-fix-a-ssl-certificate-error-with-urllib
        ssl._create_default_https_context = ssl._create_unverified_context
        # pass in a header so the request isn't seen as a bot and prevent a 403 error
        # https://stackoverflow.com/questions/45247983/urllib-urlretrieve-with-custom-header
        opener = urllib.request.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib.request.install_opener(opener)
        # retrieve file and save it
        urllib.request.urlretrieve(link, file_path)
        # extract all from zip file and save to output_directory
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(output_directory)
            
            
    def update_download_date(self):
        """ 
        As the download has been successsful, the date in the run_tracker file can be updated to todays date. This is the date used
        in the preformatting stage. Saving it here rather than using todays date is useful for testing where the date used will not be
        todays date but the date the datawas downloaded.
        """
        Logger.logger.info("update the last download date to todays date")
        
        run_tracker_path = os.path.join(SetUp.configs_dir,'run_tracker.json')
        run_tracker_config = get_json(run_tracker_path)
        run_tracker_config['last_download_date'] = datetime.datetime.now().strftime("%d-%m-%Y")
        write_json(run_tracker_path,run_tracker_config)
        
        
        



def get_all_ouput_file_names(data_import_lst):
    lst = []
    for group in data_import_lst:
        for item in group['groups']:
            lst.append(item['output'])
    return list(set(lst))


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
    
