import os

from functions.common.backup import complete_script__backup, complete_script__restore, _copy_relevant_directories_to_backup
from functions.common.database_commands import drop_and_restore_db
from ..setup import SetUp
from functions.common.constants import *


# pytest -s functions/tests/backup_test.py

class BackupTests:
    
    def _copy_relevant_directories_to_backup_test(self, tmpdir):
        SetUp.active_atlas_dir = tmpdir.mkdir("main")
        SetUp.backup_dir = tmpdir.mkdir("backup")
        directories = [CONVERSION, INPUT, OUTPUT]
        for index, directory in enumerate(directories):
            f = SetUp.active_atlas_dir.mkdir(directory).join(f"file_{index}.txt")
            f.write("")
            
        _copy_relevant_directories_to_backup(reverse=False)
        
        assert os.listdir(SetUp.backup_dir) == directories
        for index, directory in enumerate(directories):
            assert os.path.exists(os.path.join(SetUp.backup_dir, directory, f"file_{index}.txt"))
            
        