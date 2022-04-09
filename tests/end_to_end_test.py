import pytest
import sys, traceback
import os
import importlib
import shutil
import subprocess

from functions.directory_files import get_json, file_exist
from functions.setup import SetUp
from functions.database_commands import drop_and_restore_db

def test_end_to_end():
    test_configs = get_json(os.path.join(SetUp.configs_dir,'end_to_end_tests.json'))
    # replace the necessary folders so the correct data is in place before the test is run
    test_data_dir = SetUp.test_data_dir
    base_dir = SetUp.BASE_DIR

    # prompt the user on which test to use
    test_groups, options_string = dict_and_string(test_configs)
    msg = f"Select test group:\n{options_string}\n(0) - Exit\n"

    group_choice_id = input(msg)
    
    exit_under_user_request(group_choice_id)
   
    group_choice = test_groups[group_choice_id]
    
    subgrp_configs = test_configs[group_choice]['tests']
    subtest_groups, suboptions_string = dict_and_string(subgrp_configs)
    
    msg = f"Select sub-test group:\n{suboptions_string}\n(0) - Exit\n"
    subgroup_choice_id = input(msg)
    exit_under_user_request(subgroup_choice_id)
        
    subgroup_choice = subtest_groups[subgroup_choice_id]
    
    selected_test = subgrp_configs[subgroup_choice]
    
    test_data_path = os.path.join(test_data_dir, group_choice, subgroup_choice)
    
    # replace the folders and files required for the test
    dirs_to_replace = os.listdir(test_data_path)
    for folder in dirs_to_replace:
        del_folder_path = os.path.join(base_dir,folder)
        try:
            shutil.rmtree(del_folder_path)
        except FileNotFoundError:
            # folder didn't exist
            pass
        
        copy_dir = os.path.join(test_data_path,folder)
        shutil.copytree(copy_dir, del_folder_path)
        
    # reset database if .sql file exists
    dump_path = os.path.join(test_data_path,"atlas_dump.sql")
    if file_exist(dump_path):
        drop_and_restore_db(directory=test_data_path,filename="atlas_dump.sql")
    
    # dynamically finds and executes the method
    function_to_test = selected_test['function_to_test']
    select_module = function_to_test['module']
    select_class = function_to_test['class']
    select_method = function_to_test['method']
    
    mymodule = importlib.import_module(select_module)
    myclass = getattr(mymodule,select_class)
    myfunc = getattr(myclass(),select_method)
    myfunc()
    
   
    
    
def dict_and_string(dic):
    test_groups = {str(int(x[:2])): x for x in dic.keys()}
    options_string = ("\n").join(["(%s) - %s"%(x,test_groups[x]) for x in test_groups])
    return test_groups, options_string


def exit_under_user_request(choice_id):
    if choice_id == "0":
        print("You have chosen to exit the test")
        os._exit(1)
    
    
    
    
