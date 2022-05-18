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

# import ipdb; ipdb.set_trace()

# pytest -s functions/tests/apply_user_updates_tests.py
# pytest -s functions/tests/apply_user_updates_tests.py::ExtractUserEditsTests::transfer_user_edits_to_core_test

# print(self.get_csv_file_as_value_list(directory=obj.edit_dir, table_name=TEST_TABLE))
# print(self.make_3_row_df_from_file('Parent'))


class ExtractUserEditsTests(DataframeTest):
    
    @pytest.mark.parametrize(
        "fixture", 
        [
            FIXTURES_1,
            FIXTURES_2
        ]
    ) 
    @mock.patch.object(ExtractUserEdits, "__init__", return_value=None)
    def _transfer_user_creations_to_core_table_test(self, mock_1, tmpdir, sqlalchemy_engine, fixture):
        obj = self.create_obj_with_sqlalchemy_engine(obj_name=ExtractUserEdits, sqlalchemy_engine=sqlalchemy_engine)
        
        self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
        
        obj._transfer_user_creations_to_core_table(TEST_TABLE)
        
        for item in fixture.file_output:
            result, msg = self.assert_df_values(
                                directory=getattr(obj, item['directory']), 
                                table_name=item['table'], 
                                expected=item['result']
                            )
            assert result, msg
  
        
        
    @pytest.mark.parametrize(
        "fixture", 
        [
            FIXTURES_HOLDER_1,
            FIXTURES_HOLDER_2
        ]
    ) 
    @mock.patch.object(ExtractUserEdits, "__init__", return_value=None)
    def transfer_user_edits_to_core_test(self, mock_1, tmpdir, sqlalchemy_engine, fixture):
        obj = self.create_obj_with_sqlalchemy_engine(obj_name=ExtractUserEdits, sqlalchemy_engine=sqlalchemy_engine)
        obj.update_configs = UPDATE_CONFIGS_1
        self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
        
        obj.transfer_user_edits_to_core()
        
        for item in fixture.file_output:
            result, msg = self.assert_df_values(
                                directory=getattr(obj, item['directory']), 
                                table_name=item['table'], 
                                expected=item['result']
                            )
            assert result, msg
            
