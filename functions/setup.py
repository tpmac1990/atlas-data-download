import datetime
import os
import logging
import pandas as pd
import re

from .directory_files import fileExist


class SetUp:
    # BASE_DIR in this instance is the RAW_DATASETS
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # set the date
    tDate = datetime.datetime.now().strftime("%y%m%d")
    pyDate = datetime.datetime.now().date() # format: yyyy-mm-dd
    # scripts directory
    scripts_dir = os.path.join(BASE_DIR,'scripts')
    # configs directory
    logs_dir = os.path.join(scripts_dir,'logs')
    # configs directory
    configs_dir = os.path.join(scripts_dir,'configs')
    # input directory. Where the raw data is downloaded to and formatted
    input_dir = os.path.join(BASE_DIR,'input')
    # output directory. Where combined data is stored
    output_dir = os.path.join(BASE_DIR,'output')
    # conversion directory. Where the files that convert the raw values to formatted values is stored
    convert_dir = os.path.join(BASE_DIR,'conversion')
    # reions shapefiles directory
    regions_dir = os.path.join(BASE_DIR,'regions')
    # backup directory
    backup_dir = os.path.join(BASE_DIR,'backup')
    # data groups
    data_groups = ['occurrence','tenement']
    # update is True if there are no files in the output/core directory
    isUpdate = True if fileExist(os.path.join(output_dir,'update','change.csv')) else False
    # is this a 'local' or 'remote' database update
    db_location = 'local'







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



