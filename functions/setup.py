import datetime
import os
import sys
import pandas as pd
import re

from functions.common.directory_files import file_exist, get_json


class SetUp:    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    data_groups = ['occurrence','tenement']
    directories = os.path.join(BASE_DIR,'directories')
    scripts_dir = os.path.join(BASE_DIR,'scripts')
    logs_dir = os.path.join(scripts_dir,'logs')
    configs_dir = os.path.join(scripts_dir,'configs')
    test_data_dir = os.path.join(BASE_DIR,'test_data')
    
    run_tracker_configs = get_json(os.path.join(configs_dir,'run_tracker.json'))
    active_atlas_directory_name = run_tracker_configs["active_atlas_directory_name"]
    active_directory_configs = run_tracker_configs[active_atlas_directory_name]

    last_download_date = active_directory_configs["last_download_date"]
    task = active_directory_configs["task"]
    run_module = active_directory_configs["run_module"]
    
    # get the date from the config file so all dates are the same. Also useful for testing so the 
    #   date is the date of the last download. This will maintain equal output
    pyDate = datetime.datetime.strptime(last_download_date, "%d-%m-%Y").date()
    tDate = pyDate.strftime("%y%m%d")
    
    # the active directory selected from either local, remote or testing
    active_atlas_dir = os.path.join(BASE_DIR, "directories", active_atlas_directory_name)
    input_dir = os.path.join(active_atlas_dir,'input')
    output_dir = os.path.join(active_atlas_dir,'output')
    convert_dir = os.path.join(active_atlas_dir,'conversion')
    regions_dir = os.path.join(active_atlas_dir,'regions')
    backup_dir = os.path.join(active_atlas_dir,'backup')
    archive_dir = os.path.join(active_atlas_dir,'archive')
    db_dumps_dir = os.path.join(active_atlas_dir,'db_dumps')
    # update is True if there are no files in the output/core directory
    isUpdate = True if file_exist(os.path.join(output_dir,'update','change.csv')) else False
    # logs are recorded. not recoreded by default when running tests
    logs = False if 'pytest' in sys.modules.keys() else True
    

def update_setup():
    """ 
    Used to update the values in SetUp object from 'run_tracker.json' if the user has select different
    run preferences then the previous run. 
    """
    run_tracker_path = os.path.join(SetUp.configs_dir, 'run_tracker.json')
    run_tracker_config = get_json(run_tracker_path)
    # the active directory selected from either local, remote or testing
    SetUp.active_atlas_dir = os.path.join(SetUp.BASE_DIR, "directories", run_tracker_config["active_atlas_directory_name"])
    SetUp.input_dir = os.path.join(SetUp.active_atlas_dir,'input')
    SetUp.output_dir = os.path.join(SetUp.active_atlas_dir,'output')
    SetUp.convert_dir = os.path.join(SetUp.active_atlas_dir,'conversion')
    SetUp.regions_dir = os.path.join(SetUp.active_atlas_dir,'regions')
    SetUp.backup_dir = os.path.join(SetUp.active_atlas_dir,'backup')
    SetUp.archive_dir = os.path.join(SetUp.active_atlas_dir,'archive')
    SetUp.db_dumps_dir = os.path.join(SetUp.active_atlas_dir,'db_dumps')
    SetUp.active_atlas_directory_name = run_tracker_config["active_atlas_directory_name"]
    SetUp.active_directory_configs = run_tracker_config[SetUp.active_atlas_directory_name]
    SetUp.last_download_date = SetUp.active_directory_configs["last_download_date"]
    SetUp.task = SetUp.active_directory_configs["task"]
    SetUp.run_module = SetUp.active_directory_configs["run_module"]
    # get the date from the config file so all dates are the same. Also useful for testing so the 
    #   date is the date of the last download. This will maintain equal output
    SetUp.pyDate = datetime.datetime.strptime(SetUp.last_download_date, "%d-%m-%Y").date()
    SetUp.tDate = SetUp.pyDate.strftime("%y%m%d")    
    # update is True if there are no files in the output/core directory
    SetUp.isUpdate = True if file_exist(os.path.join(SetUp.output_dir,'update','change.csv')) else False
    