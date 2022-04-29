import os
import shutil

from ..setup import SetUp
from functions.common.directory_files import delete_files_in_directory, copy_directory
from .database_commands import pg_dump, drop_and_restore_db


def complete_script__backup():
    if SetUp.task != "complete_script":
        return
    shutil.rmtree(SetUp.backup_dir)
    _copy_relevant_directories_to_backup()
    pg_dump(filename="atlas_dump.sql", directory=SetUp.backup_dir)
    
def complete_script__restore():
    if SetUp.task != "complete_script":
        return
    shutil.rmtree(SetUp.active_atlas_dir)
    _copy_relevant_directories_to_backup(reverse=True)
    drop_and_restore_db(filename="atlas_dump.sql", directory=SetUp.backup_dir)
    
    
def _copy_relevant_directories_to_backup(reverse=False):
    directories = ["conversion","input","output"]
    for directory_name in directories:
        src_directory = os.path.join(SetUp.active_atlas_dir, directory_name)
        dest_directory = os.path.join(SetUp.backup_dir, directory_name)
        if reverse:
            src_directory, dest_directory = dest_directory, src_directory
        shutil.copytree(src_directory, dest_directory)
    
