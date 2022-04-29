import datetime
import os
import sys
import logging
import re

from ..setup import SetUp


def _create_log_name(name):
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
    log_file = _create_log_name('main')
    file_handler = logging.FileHandler(os.path.join(SetUp.logs_dir,log_file))
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)