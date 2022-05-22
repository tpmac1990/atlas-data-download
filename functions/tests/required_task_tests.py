import pytest
from unittest import mock

from functions.common.constants import *
from functions.segment.required_task import PromptRequiredTask
from .required_task_data import *


# pytest -s functions/tests/required_task_tests.py
# order: fixture, patch, monkeypatch, paramatrize, tmpdir
# mock path is where it is used, not where it is homed


def _init_class__set_obj__call_function(obj, method=None, returns=False):
    prt = PromptRequiredTask()
    prt.run_tracker_config = obj
    if method:
        func = getattr(prt, method)
        if returns:
            val = func()
            return prt, val
        func()
        return prt
    return prt
    
  
    

class RequiredTaskAssistTests:
    
    @pytest.mark.parametrize(
        "choice, output", 
        [
            ("y", True), 
            ("yEs", True),
            ("no", False),
            ("random", False)
        ]
    ) 
    def yes_no_boolean_test(self, choice, output):
        result = PromptRequiredTask.yes_no_boolean(answer=choice)
        assert result == output
        
        
    def dict_and_string_test(self):
        test_groups, options_string = PromptRequiredTask.dict_and_string(dic=SEGMENTS_TESTS_B())
        assert test_groups == {'1': '01_sub_group_1', '2': '02_sub_group_2'}
        assert "(1) - 01_sub_group_1" in options_string
        assert "(2) - 02_sub_group_2" in options_string
        

    def build_segments_list_test(self):
        segment_choice_ids = PromptRequiredTask.build_segments_list("3-5,7", {'3': '03_sub_group_3', '4': '04_sub_group_4', '5': '05_sub_group_5', '6': '06_sub_group_6', '7': '07_sub_group_7'})
        assert segment_choice_ids == ['3', '4', '5', '7']
        
        

class RequiredTaskCoreTests:
    
    @pytest.mark.parametrize(
        "obj, choice, answer", 
        [
            (LOCAL_SEGMENTS_SINGLE_SUB_GROUP(), "y", True), 
            (LOCAL_SEGMENTS_SINGLE_SUB_GROUP(), "n", False),
            (LOCAL_SEGMENTS_SINGLE_SUB_GROUP(), "blue", False)
        ]
    ) 
    def use_last_configs_test(self, monkeypatch, obj, choice, answer):
        monkeypatch.setattr('builtins.input', lambda _: choice)
        prt, result = _init_class__set_obj__call_function(obj=obj, method='use_last_configs', returns=True)
        assert result == answer
        
    
    
    @pytest.mark.parametrize(
        "obj, answer", 
        [
            (LOCAL_SEGMENTS_SINGLE_SUB_GROUP(), False), 
            (LOCAL_COMPLETE_SINGLE_SUB_GROUP(), True)
        ]
    ) 
    def run_complete_script_test(self, obj, answer):
        prt, result = _init_class__set_obj__call_function(obj=obj, method='run_complete_script', returns=True)
        assert result == answer
        
    
    
    def set_directory_to_use_test(self, monkeypatch):
        """ options: {"1": "production", "2": "local", "3": "testing"} """
        monkeypatch.setattr('builtins.input', lambda _: "2")
        prt, result = _init_class__set_obj__call_function(
            obj=LOCAL_SEGMENTS_SINGLE_SUB_GROUP(), method='set_directory_to_use', returns=True)
        assert prt.run_tracker_config['active_atlas_directory_name'] == "local"
        
        

    @mock.patch('functions.common.backup.complete_script__backup', return_value=None, autospec=False)
    @pytest.mark.parametrize(
        "obj, choice, run_module, task", 
        [
            (LOCAL_COMPLETE_SINGLE_SUB_GROUP(), "1", "complete_script", LOCAL_COMPLETE_SINGLE_SUB_GROUP()["local"]["task"]), 
            (LOCAL_COMPLETE_SINGLE_SUB_GROUP(), "2", "segment", "")
        ]
    ) 
    def set_modules_to_run_test(self, mock_value, monkeypatch, obj, choice, run_module, task):
        """ options: {"1": "Complete Script", "2": "Segment"} """
        monkeypatch.setattr('builtins.input', lambda _: choice)
        prt, result = _init_class__set_obj__call_function(
            obj=obj, method='set_modules_to_run', returns=True)
        run_tracker_result = prt.run_tracker_config[LOCAL]
        assert run_tracker_result[RUN_MODULE] == run_module
        assert run_tracker_result[TASK] == task
        


    @mock.patch('pandas.read_csv', return_value=MANUAL_UPDATE_DF(), autospec=False)
    @mock.patch('builtins.print', return_value=None, autospec=False)
    @mock.patch('functions.segment.required_task.PromptRequiredTask.save_run_tracker_configs', return_value=None, autospec=False)
    @pytest.mark.parametrize(
        "choice, task", 
        [
            ("1", "manual_updates"), 
            ("2", "state_source_updates")
        ]
    ) 
    def set_task_to_run_test(self, mock_value_1, mock_value_2, mock_value_3, monkeypatch, choice, task):
        monkeypatch.setattr('builtins.input', lambda _: choice)
        prt = _init_class__set_obj__call_function(obj=LOCAL_COMPLETE_SINGLE_SUB_GROUP(), method='set_task_to_run')
        assert prt.run_tracker_config[LOCAL][TASK] == task



    @mock.patch('builtins.print', return_value=None, autospec=False)
    @mock.patch('functions.segment.required_task.PromptRequiredTask.save_run_tracker_configs', return_value=None, autospec=False)
    @pytest.mark.parametrize(
        "df, choice, msg", 
        [
            (MANUAL_UPDATE_DF(), "99", r".* was not an option"), 
            (MANUAL_UPDATE_DF_EMPTY(), "1", r".* was not a viable option")
        ]
    ) 
    def set_task_to_run_errors_test(self, mock_value_1, mock_value_2, monkeypatch, df, choice, msg):
        with pytest.raises(KeyError, match=msg):
            monkeypatch.setattr('pandas.read_csv', lambda _: df)
            monkeypatch.setattr('builtins.input', lambda _: choice)
            prt = _init_class__set_obj__call_function(obj=LOCAL_COMPLETE_SINGLE_SUB_GROUP(), method='set_task_to_run')



    @mock.patch('functions.segment.required_task.PromptRequiredTask.save_run_tracker_configs', return_value=None, autospec=False)
    @mock.patch('functions.segment.required_task.get_json', return_value=E2E_GROUP_SUB_GROUP(), autospec=False)
    @pytest.mark.parametrize(
        "grp_val, grp_str, sub_val, sub_list", 
        [
            ("1", "01_group_1", "3", ["03_sub_group_3"]), 
            ("2", "02_group_2", "3-5,7", ["03_sub_group_3", "04_sub_group_4", "05_sub_group_5", "07_sub_group_7"]),
        ]
    ) 
    def set_segments_to_run_test(self, mock_value_1, mock_value_2, monkeypatch, grp_val, grp_str, sub_val, sub_list):
        inputs = iter([grp_val, sub_val])
        monkeypatch.setattr('builtins.input', lambda msg: next(inputs))
        prt = _init_class__set_obj__call_function(obj=LOCAL_SEGMENTS_SINGLE_SUB_GROUP(), method='set_segments_to_run')
        assert prt.run_tracker_config[LOCAL][SEGMENTS][GROUP] == grp_str
        assert prt.run_tracker_config[LOCAL][SEGMENTS][SUBGROUPS] == sub_list
    
    
    

        
        
