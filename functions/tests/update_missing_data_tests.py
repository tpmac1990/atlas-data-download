import pytest
from unittest import mock
import sqlalchemy
import os
import pandas as pd

from functions.common.constants import *
from functions.segment.commit_new_values import UpdateMissingData
from functions.common.database_commands import drop_and_restore_db, pg_dump, drop_db
from functions.setup import SetUp
from .update_missing_data_data import *
from functions.common.db_functions import drop_all_db_tables
from .common import *


# pytest -s functions/tests/update_missing_data_tests.py
# pytest -s functions/tests/update_missing_data_tests.py::UpdateMissingDataTests::do_not_apply_missing_data_unless_task_is__manual_updates__test

# print(self.get_csv_file_as_value_list(directory=obj.edit_dir, table_name=TEST_TABLE))
# print(self.create_fixture_ready_data_from_file('Parent', sub_dir='update'))


class UpdateMissingDataTests(DataframeTest):      
    
    @mock.patch('functions.segment.commit_new_values.Timer', return_value=None, autospec=False)
    def do_not_apply_missing_data_unless_task_is__manual_updates__test(self, mock_1):
        """ 
        if task is not 'manual_updates' then skip this step. It has not beed requested
        """
        obj = UpdateMissingData()
        SetUp.task = 'segments'
        obj.apply_missing_data_updates()
        mock_1.assert_not_called()
        
        
    
    @mock.patch('functions.segment.commit_new_values.get_path', return_value=None, autospec=False)
    @mock.patch.object(UpdateMissingData, "__init__", return_value=None)
    def _transfer_user_creations_to_core_table___no_changes__test(self, mock_1, mock_2, tmpdir, sqlalchemy_engine, psycopg_conn):
        fixture = COMMIT_FIELDS_UPDATED_DATA__NO_NEW_VALUES__FIXTURE
        obj = self.create_obj_with_connections(obj_name=UpdateMissingData, sqlalchemy_engine=sqlalchemy_engine, psycopg_conn=psycopg_conn)
        self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
        obj.commit_fields_updated_data(field='HOLDER', configs=COMMIT_UPDATES_CONFIGS['HOLDER'])
        assertions = self.assert_output_values(obj=obj, data=fixture)
        for assertion in assertions:
            assert assertion[RESULT], assertion[MSG]
        mock_2.assert_not_called()
            
            
    # # this was failing when i decided to change the backend to suit wagtail
    # @pytest.mark.parametrize(
    #     "fixture", 
    #     [
    #         # SIMPLE__FIXTURE,
    #         COMMIT_FIELDS_UPDATED_DATA__UPDATE_VALUE_THAT_EXISTS_IN_CONVERSION_FILE__FIXTURE
    #     ]
    # ) 
    # @mock.patch.object(UpdateMissingData, "__init__", return_value=None)
    # def _transfer_user_creations_to_core_table__full__test(self, mock_1, tmpdir, sqlalchemy_engine, psycopg_conn, fixture):
    #     obj = self.create_obj_with_connections(obj_name=UpdateMissingData, sqlalchemy_engine=sqlalchemy_engine, psycopg_conn=psycopg_conn)
    #     self.populate_files_and_db_tables(data=fixture, obj=obj, temp_dir=tmpdir)
    #     obj.commit_fields_updated_data(field='HOLDER', configs=COMMIT_UPDATES_CONFIGS['HOLDER'])
    #     assertions = self.assert_output_values(obj=obj, data=fixture)
    #     for assertion in assertions:
    #         assert assertion[RESULT], assertion[MSG]
        
        
        # print(self.create_fixture_ready_data_from_file('Companies_R', directory=SetUp.convert_dir, sub_dir=None))
        # print(self.create_fixture_ready_data_from_file('TenHolder'))
        
        
        