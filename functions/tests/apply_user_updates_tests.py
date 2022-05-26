import pytest
from unittest import mock
import sqlalchemy
import os
import pandas as pd

from functions.common.constants import *
from functions.segment.apply_user_updates import ExtractUserEdits
from functions.common.database_commands import drop_and_restore_db, pg_dump, drop_db
from functions.setup import SetUp
from .apply_user_updates_data import *
from functions.common.db_functions import drop_all_db_tables
from .common import *


# pytest -s functions/tests/apply_user_updates_tests.py
# pytest -s functions/tests/apply_user_updates_tests.py::ExtractUserEditsTests::transfer_user_edits_to_core_test

# print(self.get_csv_file_as_value_list(directory=obj.edit_dir, table_name=TEST_TABLE))
# print(self.create_fixture_ready_data_from_file('Parent'))


class ExtractUserEditsTests(DataframeTest):

    @mock.patch('functions.segment.apply_user_updates.ExtractUserEdits.transfer_user_creations_to_core', return_value=None, autospec=False)
    def extract_user_edits__is_not_update__test(self, mock_1):
        """ 
        don't run any further methods in this module if this is the first run, 
        hence there is no data to update.
        """
        obj = ExtractUserEdits()
        SetUp.isUpdate = False
        obj.extract_user_edits()
        mock_1.assert_not_called()
        
        
    @pytest.mark.parametrize(
        "fixture", 
        [
            USER_CREATIONS_TO_EDIT_CORE_FILES__NO_EXISTING_EDITS__FIXTURE,
            USER_CREATIONS_TO_EDIT_CORE_FILES__NO_NEW_EDITS__FIXTURE
        ]
    ) 
    @mock.patch.object(ExtractUserEdits, "__init__", return_value=None)
    def _transfer_user_creations_to_core_table_test(self, mock_1, tmpdir, sqlalchemy_engine, fixture):
        obj = self.create_obj_with_sqlalchemy_engine(obj_name=ExtractUserEdits, sqlalchemy_engine=sqlalchemy_engine)
        self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
        obj._transfer_user_creations_to_core_table(TEST_TABLE)
        for assertion in self.assert_output_values(obj=obj, data=fixture):
            assert assertion[RESULT], assertion[MSG]
        
        
    @pytest.mark.parametrize(
        "fixture", 
        [
            USER_EDITS_TO_CORE_FILES__NO_EXISTING_EDIT_FILES__FIXTURE,
            USER_EDITS_TO_CORE_FILES__EXISTING_EDIT_FILES__FIXTURE
        ]
    ) 
    @mock.patch.object(ExtractUserEdits, "__init__", return_value=None)
    def transfer_user_edits_to_core_test(self, mock_1, tmpdir, sqlalchemy_engine, fixture):
        obj = self.create_obj_with_sqlalchemy_engine(obj_name=ExtractUserEdits, sqlalchemy_engine=sqlalchemy_engine)
        obj.update_configs = USER_EDITS_TO_CORE_FILES__CONFIGS_1
        self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
        obj.transfer_user_edits_to_core()
        for assertion in self.assert_output_values(obj=obj, data=fixture):
            assert assertion[RESULT], assertion[MSG]
            
              
    @pytest.mark.parametrize(
        "fixture", 
        [
            DB_CHANGES_TO_CORE_CHANGE_FILES__BASIC_ADD__FIXTURE,
            DB_CHANGES_TO_CORE_CHANGE_FILES__NO_CORE_EXISTS__FIXTURE,
            DB_CHANGES_TO_CORE_CHANGE_FILES__NO_NEW_EDITS__FIXTURE
        ]
    ) 
    @mock.patch.object(ExtractUserEdits, "__init__", return_value=None)
    def _transfer_changes_to_core_table_test(self, mock_1, tmpdir, sqlalchemy_engine, fixture):
        obj = self.create_obj_with_sqlalchemy_engine(obj_name=ExtractUserEdits, sqlalchemy_engine=sqlalchemy_engine)
        self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
        obj._transfer_changes_to_core_table(TEST_TABLE)
        for assertion in self.assert_output_values(obj=obj, data=fixture):
            assert assertion[RESULT], assertion[MSG]
            
        