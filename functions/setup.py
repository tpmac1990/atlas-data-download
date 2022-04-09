import datetime
import os
import sys
import logging
import pandas as pd
import re

from .directory_files import file_exist, get_json


class SetUp:    
    # BASE_DIR in this instance is the RAW_DATASETS
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # data groups
    data_groups = ['occurrence','tenement']
    # directories holding the data
    directories = os.path.join(BASE_DIR,'directories')
    # scripts directory
    scripts_dir = os.path.join(BASE_DIR,'scripts')
    # configs directory
    logs_dir = os.path.join(scripts_dir,'logs')
    # configs directory
    configs_dir = os.path.join(scripts_dir,'configs')
    # test data directory
    test_data_dir = os.path.join(BASE_DIR,'test_data')
    
    # # On the first run the directory to use will need to be parsed form the json file and stored in the environ
    # #   to be used on sebsequent uses. ???? These will need to be reset to none if they are edited in "required_tasks"
    # if not os.environ.get("active_atlas_directory_name"): 
    #     run_tracker_configs = get_json(os.path.join(configs_dir,'run_tracker.json'))
    #     active_atlas_directory_name = run_tracker_configs["active_atlas_directory_name"]
    #     active_directory_configs = run_tracker_configs[active_atlas_directory_name]
    #     # set values to eviron so they don't have to be parsed from the .json file again
    #     os.environ["active_atlas_directory_name"] = active_atlas_directory_name
    #     os.environ["last_download_date"] = active_directory_configs["last_download_date"]
    #     os.environ["task"] = active_directory_configs["task"]
    #     os.environ["run_module"] = active_directory_configs["task"]
    
    run_tracker_configs = get_json(os.path.join(configs_dir,'run_tracker.json'))
    active_atlas_directory_name = run_tracker_configs["active_atlas_directory_name"]
    active_directory_configs = run_tracker_configs[active_atlas_directory_name]

    last_download_date = active_directory_configs["last_download_date"]
    task = active_directory_configs["task"]
    run_module = active_directory_configs["run_module"]
    # # values for the selected directory
    # active_atlas_directory_name = os.environ["active_atlas_directory_name"]
    # last_download_date = os.environ["last_download_date"]
    # task = os.environ["active_atlas_directory_name"]
    # run_module = os.environ["run_module"]
    
    # get the date from the config file so all dates are the same. Also useful for testing so the 
    #   date is the date of the last download. This will maintain equal output
    pyDate = datetime.datetime.strptime(last_download_date, "%d-%m-%Y").date()
    tDate = pyDate.strftime("%y%m%d")
    
    # the active directory selected from either local, remote or testing
    active_atlas_dir = os.path.join(BASE_DIR, "directories", active_atlas_directory_name)
    # input directory. Where the raw data is downloaded to and formatted
    input_dir = os.path.join(active_atlas_dir,'input')
    # output directory. Where combined data is stored
    output_dir = os.path.join(active_atlas_dir,'output')
    # conversion directory. Where the files that convert the raw values to formatted values is stored
    convert_dir = os.path.join(active_atlas_dir,'conversion')
    # reions shapefiles directory
    regions_dir = os.path.join(active_atlas_dir,'regions')
    # backup directory
    backup_dir = os.path.join(active_atlas_dir,'backup')
    # backup directory
    archive_dir = os.path.join(active_atlas_dir,'archive')
    # db_dumps directory
    db_dumps_dir = os.path.join(active_atlas_dir,'db_dumps')
    # update is True if there are no files in the output/core directory
    isUpdate = True if file_exist(os.path.join(output_dir,'update','change.csv')) else False
    
    

    # set the date
    # tDate = datetime.datetime.now().strftime("%y%m%d")
    # pyDate = datetime.datetime.now().date() # format: yyyy-mm-dd
    # # scripts directory
    # scripts_dir = os.path.join(BASE_DIR,'scripts')
    # # configs directory
    # logs_dir = os.path.join(scripts_dir,'logs')
    # # configs directory
    # configs_dir = os.path.join(scripts_dir,'configs')
    # # input directory. Where the raw data is downloaded to and formatted
    # input_dir = os.path.join(BASE_DIR,'input')
    # # output directory. Where combined data is stored
    # output_dir = os.path.join(BASE_DIR,'output')
    # # conversion directory. Where the files that convert the raw values to formatted values is stored
    # convert_dir = os.path.join(BASE_DIR,'conversion')
    # # reions shapefiles directory
    # regions_dir = os.path.join(BASE_DIR,'regions')
    # # backup directory
    # backup_dir = os.path.join(BASE_DIR,'backup')
    
    # # backup directory
    # archive_dir = os.path.join(BASE_DIR,'archive')
    # # db_dumps directory
    # db_dumps_dir = os.path.join(BASE_DIR,'db_dumps')
    # # data groups
    # data_groups = ['occurrence','tenement']
    # # update is True if there are no files in the output/core directory
    # isUpdate = True if file_exist(os.path.join(output_dir,'update','change.csv')) else False
    # # is this a 'local' or 'remote' database update
    # db_location = 'local'
    # # # set variables if testing
    # if 'pytest' in sys.modules:
    #     db_location = 'test'
    # I could set this when new data is downloaded, either update or complete fresh download, then if any other steps are run at a later date, the last download date will be used
    # last_download_date = get_json(os.path.join(configs_dir,'run_tracker.json'))['last_download_date']
    # pyDate = datetime.datetime.strptime(last_download_date, "%d-%m-%Y").date()
    # tDate = pyDate.strftime("%y%m%d")
        
        
    



class LoggerFunctions:

    @staticmethod
    def create_log_name(name):
        ''' create the name of the next log file without overwriting the existing files '''
        file_lst = [x for x in os.listdir(SetUp.logs_dir) if re.search(r"%s_%s_\d*\.log"%(name,SetUp.tDate),x)]
        high = 0
        for x in file_lst:
            val = int(re.search(r"\d*(?=\.log)",x).group(0))
            if val > high:
                high = val
        high += 1
        return f'{name}_{SetUp.tDate}_{high}.log'


class Logger:
    hashed = '#'*120
    dashed = '-'*30
    formatter = logging.Formatter('%(levelname)s: %(asctime)s: %(message)s')
    log_file = LoggerFunctions.create_log_name('main')
    file_handler = logging.FileHandler(os.path.join(SetUp.logs_dir,log_file))
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)



