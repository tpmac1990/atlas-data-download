import os
import shutil

from .directory_files import get_json
from .timer import time_past, start_time

from .setup import SetUp, Logger




class Data:
    stage: str = ''
    backup_dir: str = ''
    folders_to_backup: list = []
    affects_db: bool = False



class DataBackup:

    def __init__(self,stage):
        Data.stage = stage
        files_configs = get_json(os.path.join(SetUp.configs_dir,'backup_files.json'))[stage]
        Data.folders_to_backup = files_configs['folders_to_backup']
        Data.affects_db = files_configs['affects_db']

        Data.backup_dir = os.path.join(SetUp.backup_dir,stage)

        func_start = start_time()

        Logger.logger.info('Time: %s' %(time_past(func_start)))


    def backup_data(self):
        self.move_data_between(restore=False)

    def restore_data(self):
        self.move_data_between()


    @staticmethod
    def move_data_between(restore=True):
        ''' This will loop through all the directories and copy all necessary files to the backup directory using the 
            backup_data.json config file.
            Lastly, update the backup_control.json file. This is used to identify which stage the data needs to be reverted back to in the case of an error
            ??? update_missing_data in the config file: This will only work if it is updated immediately after the ss update run. If not, then there is a chance that a user may have made some edits
                that are not in the core file. This means if there is an error then the reinstated core files will not be identical to the db files. There needs to be a way to collect the user edits 
                from the database first and apply these to the core files and then.
                Answer: 
                    1. backup core files and extract user edits from the database and update the core files
                    2. apply manual updates to the database
        '''
       
        for group in Data.folders_to_backup:
            store_folder = group['store_folder']
            src_extension = group['src_extension']
            copy_files = group['copy_files']

            src_dir = os.path.join(SetUp.BASE_DIR,src_extension)
            dest_dir = os.path.join(Data.backup_dir,store_folder)

            # switch the direction the files are moved when running th restore rather than an archive method
            if restore:
                src_dir, dest_dir = dest_dir, src_dir

            # get list of all the files in the src_dir and loop through them
            src_files = os.listdir(src_dir)
            for file_name in src_files:

                # skip the file if it is not in the list
                if copy_files != '__all__' and not file_name[:-4] in copy_files:
                    continue

                file_scr_path = os.path.join(src_dir,file_name)
                file_dest_path = os.path.join(dest_dir,file_name)

                # copy files between directories
                shutil.copy(file_scr_path, file_dest_path)
