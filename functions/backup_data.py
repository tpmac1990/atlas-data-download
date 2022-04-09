import os
import shutil

from .directory_files import get_json, write_json, copy_directory, clear_directory, delete_files_in_directory, create_directory_tree, clear_all_directory
from .timer import Timer

from .setup import SetUp, Logger
from .migrate_files_db import DatabaseManagement



class Data:
    stage: str = ''
    backup_dir: str = ''
    folders_to_backup: list = []
    folders_to_clear: list = []
    db_restore: bool = False
    backup_stage_path: str = ''
    backup_stage_configs: dict = {}
    


class DataBackup:

    def __init__(self,stage):
        Data.stage = stage
        files_configs = get_json(os.path.join(SetUp.configs_dir,'backup_files.json'))[stage]
        Data.backup_stage_path = os.path.join(SetUp.configs_dir,'backup_stage.json')
        Data.backup_stage_configs = get_json(Data.backup_stage_path)
        Data.folders_to_backup = files_configs['folders_to_backup']
        Data.folders_to_clear = files_configs['folders_to_clear']
        Data.db_restore = files_configs['db_restore']


    def backup_data(self):
        timer = Timer()
        Logger.logger.info(f"backing up data for stage: {Data.stage}")
        Data.backup_dir = os.path.join(SetUp.backup_dir,Data.stage)
        self.move_data_between(restore=False)
        self.update_backup_stage(Data.stage)
        Logger.logger.info('Data backup time duration: %s' %(timer.time_past()))

    def restore_data(self):
        timer = Timer()
        Logger.logger.info(f"An error occurred during stage: {Data.stage}. Data will be restored to last backup point")
        Data.backup_dir = os.path.join(SetUp.backup_dir,Data.stage)
        self.move_data_between()
        Logger.logger.info('Data restore time duration: %s' %(timer.time_past()))

    def backup_data_archive(self):
        timer = Timer()
        Logger.logger.info(f"archiving data for stage: {Data.stage}")
        Data.backup_dir = os.path.join(SetUp.archive_dir,SetUp.tDate,Data.stage)
        create_directory_tree(Data.backup_dir)
        self.move_data_between(restore=False)
        Logger.logger.info('Data archive backup time duration: %s' %(timer.time_past()))

    def restore_data_archive(self):
        ''' restores the local files to the last successful archive point and set the backup_stage back to 'data_download' '''
        timer = Timer()
        Logger.logger.info(f"An error occurred during stage: {Data.stage}. Data will be restored to last archive point")
        # get the name of the latest archive folder
        archive_folder = os.listdir(SetUp.archive_dir)[-1]
        Data.backup_dir = os.path.join(SetUp.archive_dir,archive_folder,Data.stage)
        self.move_data_between()
        # clear database and load tables in core directory if required
        if Data.db_restore:
            core_dir = os.path.join(SetUp.output_dir,'core')
            DatabaseManagement().clear_db_tables_and_remigrate(src_dir=core_dir)
            # set the stage back to 'data_download' so the restart will begin at that point
            self.update_backup_stage('data_download')
        Logger.logger.info('Data archive restore time duration: %s' %(timer.time_past()))



    @staticmethod
    def move_data_between(restore=True):
        ''' This will loop through all the directories and copy all necessary files to the backup directory using the 
            backup_data.json config file.
            Lastly, update the backup_control.json file. This is used to identify which stage the data needs to be reverted back to in the case of an error
        '''
       
        for group in Data.folders_to_backup:
            store_folder = group['store_folder']
            src_extension = group['src_extension']
            copy_files = group['copy_files']

            src_dir = os.path.join(SetUp.active_atlas_dir,src_extension)
            dest_dir = os.path.join(Data.backup_dir,store_folder)

            # slice to remove either the .csv or .jsom extension
            ext_slice = -5 if src_dir.endswith('configs') else -4

            # switch the direction the files are moved when running th restore rather than an archive method
            if restore:
                src_dir, dest_dir = dest_dir, src_dir
            else:
                # create src_dir if it doesn't exist
                create_directory_tree(dest_dir)

            # get list of all the files in the src_dir and loop through them
            src_files = os.listdir(src_dir)
            for file_name in src_files:

                # skip the file if it is not in the list
                if copy_files != '__all__' and not file_name[:ext_slice] in copy_files:
                    continue

                file_scr_path = os.path.join(src_dir,file_name)
                file_dest_path = os.path.join(dest_dir,file_name)

                # copy files between directories
                shutil.copy(file_scr_path, file_dest_path)

        # While it will not create any issues, clearing directories when not a restore is pointless
        if not restore:
            return

        # loop through folders to clear files from
        for group in Data.folders_to_clear:
            src_extension = group['src_extension']
            clear_files = group['clear_files']
            src_dir = os.path.join(SetUp.active_atlas_dir,src_extension)
            delete_files_in_directory(src_dir)


    @staticmethod
    def update_backup_stage(stage=None):
        Data.backup_stage_configs['stage'] = stage
        write_json(Data.backup_stage_path,Data.backup_stage_configs)


    @staticmethod
    def set_process(process='state_source_update'):
        ''' process is either 'manual_data_update' or 'state_source_update'. This is the last task that was undertaken. Set its success as initially False but this will be updated if successful '''
        Data.backup_stage_configs['last_process'] = {'process': process, 'successful': False}
        write_json(Data.backup_stage_path,Data.backup_stage_configs)


    @staticmethod
    def set_process_successful():
        ''' This method only runs if the process was successful, so set successful key value to True. Clear the backup folder as it is no longer required '''
        Data.backup_stage_configs['last_process']['successful'] = True
        write_json(Data.backup_stage_path,Data.backup_stage_configs)
        # clear backup folder
        clear_all_directory(SetUp.backup_dir)





''' Clear the output sub directories only leaving the core files. If it is an update then re-create the update.csv file. This is
        required in the case the macro is run in part in which case it will remain an update.
    change: records changes made to datasets. archived, holds changes before changes were attempted
    core: core data that mimics the database fields. archived, exact copy of the db before changes are applied. used when roll back required
    new: new data to be applied to the core and db. not archived, created and applied within same run
    ss: a maintined set of core data without the user edits applied. archived, even though the new data has already been downloaded, the changes have not been applied, so this will
        save a copy before changes are applied. 
    onew: a copy of the new files before user edits are applied which provides a set of data to compare to the post user edit new data. not archived, created and applied within same run
    edit: maintained tables of all the user edits. archived, exact copy before changes are made
''' 