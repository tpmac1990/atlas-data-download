import os
import sys
import shutil
import pandas as pd
import importlib
import datetime

from functions.common.directory_files import get_json, write_json, file_exist
from functions.common.database_commands import drop_and_restore_db
from ..setup import SetUp, update_setup
from functions.logging.logger import logger
from functions.common import pre_segment
from functions.common.backup import complete_script__backup


class PromptRequiredTask:
    """ 
    Here, the run configs are set. The user can choose whether to run the 'complete_script' or run a single or
    multiple 'segments'.
    complete_script: 
        1. download state source data
        2. update core data with user edits from the remote db
        3. load manual updates in to the db
    segments: run indivdual modules of code that make up the complete_script. This enables a single segment to 
    be run in unison. Each segment has a dataset and db_dump (if required) that is set before the segment is run.
    There are three different directories, each also has its own database:
        1. remote: used for production. Segments can not be used on this, as doing so would overwrite the production data.
        2. local: used to run as if remote without affecting the production data. allows complete_script and segments
        3. testing: used to test new features. allows complete_script and segments
    local and testing are the same in nature, but having two allows the user to test something without affecting the local
    dataset and having to re-run it.
    """
    run_tracker_path = os.path.join(SetUp.configs_dir, 'run_tracker.json')
    run_tracker_config = get_json(run_tracker_path)

    def all_steps(self):
        """ Go through all the required prompts to figure out what steps to take """
        logger(message="Setting run configurations", category=1)
        if self.use_last_configs():
            self.trigger_run_module()
            
        else:
            self.set_directory_to_use()
            self.set_modules_to_run()
            self.set_task_to_run()
            # this will be skipped if 'complete_script' has been selected
            self.set_segments_to_run()
            self.trigger_run_module()
            
            
    def trigger_run_module(self):
        """ 
        If the run_module equals 'full_script' then a back up will be made and the script will
        naturally exit this module and continue on with the full script. If 'segments' then
        the segments from the run_configs.json file will be run. On completion, the function will be exited
        before it has the chance to continue on running the complete script.
        """
        if self.run_complete_script():
            complete_script__backup()
        else:
            self.run_segments()
            os._exit(1)
    
    def use_last_configs(self):
        """
        Get the last used values from run_tracker.json and prompt the user if they want to use them rather than
        go though all options.
        """
        run_tracker_config = self.run_tracker_config
        active_atlas_directory_name = run_tracker_config["active_atlas_directory_name"]
        task = run_tracker_config[active_atlas_directory_name]["task"]
        run_module = run_tracker_config[active_atlas_directory_name]["run_module"]
        segments = run_tracker_config[active_atlas_directory_name]["segments"]
        if type(segments) == dict:
            group = segments["group"]
            subgroups = (', ').join(segments["subgroups"])
            
        msg = f"Last run configs:\n------------------\nDirectory: {active_atlas_directory_name}\nRun module: {run_module}\n"
        suf_msg = "Would you like to re-use these configs? (y/n)\n"
        
        if run_module == "complete_script":
            msg += f"Task: {task}\n{suf_msg}"
            return self.yes_no_boolean(input(msg))
            
        elif run_module == "segment":
            msg += f"Group: {group}\nSegments: {subgroups}\n{suf_msg}"
            return self.yes_no_boolean(input(msg))
        
        return False

    
    def set_directory_to_use(self):
        """ Set the directory for the dataset to use """
        msg = "Which data directory would you like to use?\n(1) - Production\n(2) - Local\n(3) - Test\n"
        choice = input(msg)
        dic = {"1": "production", "2": "local", "3": "testing"}
        directory = dic[choice]

        logger(message=f"User has selected to use directory: '{directory}'", category=4)
        # save the directory to run_tracker.json so SetUp object can read it
        self.run_tracker_config["active_atlas_directory_name"] = directory
        

    def set_modules_to_run(self):
        """
        Select between running the complete script from start to end or one or more segments. If the produciton 
        directory was selected then complete_script will be automatically be selected. This is because running segments
        replacing existing data which would remove all current production data.
        """
        directory = self.run_tracker_config["active_atlas_directory_name"]
        # run complete or single module
        if directory == "production":
            # can't run "segment" for production data as it will delete the existing data and replace it with test data
            run_module = "complete_script"
        else:
            msg = "Would you like to run the complete script or just a segment?\n(1) - Complete Script\n(2) - Segment\n"
            choice = input(msg)
            dic = {"1": "complete_script", "2": "segment"}
            run_module = dic[choice]
        
        logger(message=f"User has selected to run: '{run_module}'", category=4)
        
        self.run_tracker_config[directory]["run_module"] = run_module
        if run_module == "segment":
            self.run_tracker_config[directory]["task"] = ""
            
        # create back up of files and database before running complete script. They will be re-instated on error
        if run_module == "complete_script":
            #  1. copy folders from one place to another if they exist
            #  2. create database dump and save it to db_dumps
            #  3. backup folder: SetUp.backup_dir
            complete_script__backup()

            

    def set_task_to_run(self):
        ''' This allows the user to select the task they want to run. The task is saved in the 'required_task.json' file which is read at necessary points in this 
                application to determine if it is required for the task requested here.
            manual_updates: update the yet to be formatted values in the 'manual_update_required.csv' 
            state_source_updates: get the latest updates from the state sources
            ## only run when complete_script is requested ##
        '''
        directory = self.run_tracker_config["active_atlas_directory_name"]
        
        # if complete_script then see if manual_update_required needs to be rund
        if not self.run_complete_script():
            return
        
        manual_update_path = os.path.join(SetUp.output_dir, 'update', 'manual_update_required.csv')
        manual_update_df = pd.read_csv(manual_update_path)

        # options are: state sources or updating user updates
        last_task = self.run_tracker_config[directory]["task"]
        
        df = manual_update_df
        total_count = len(df.index)
        update_rows_count = len(df[~df['LIKELY_MATCH'].isnull()])

        logger(message=f"The 'manual_update_required' table contains a total of '{total_count}' rows with a provided update value for '{update_rows_count}' of these rows.", category=4)
        logger(message=f"Last task run: '{last_task}'", category=4)

        msg = (
            f"The 'manual_update_required' table contains a total of '{total_count}' rows with a"
            f"\nprovided update value for '{update_rows_count}' of these rows."
            f"\nLast task run: {last_task}"
            "\nWhich task would you like to complete (Enter the number of your choice)?"
            "\n(1) Apply manual updates to the database."
            "\n(2) Update datasets using state data sources."
            "\npress any other key to Exit\n"
        )

        choice = input(msg)

        if choice == '1':
            if update_rows_count == 0:
                logger(message=f"User has selected to run the 'manual_update' task, however there is no data in the 'manual_update_required' file to apply. This process will be terminated.", category=4)
                print("You have selected to run the 'manual_update' task, however there is no data in the 'manual_update_required' table to apply.\nExiting process.")
                raise KeyError(f"{choice} was not a viable option")
            new_task = 'manual_updates'
        elif choice == '2':
            new_task = 'state_source_updates'
        else:
            logger(message=f"The user exited the function", category=4)
            print('No task was selected, Exiting process')
            raise KeyError(f"{choice} was not an option")
        
        logger(message=f"User has selected to run task: '{new_task}'", category=4)
        print(f"Preparing to run task: '{new_task}'")

        self.run_tracker_config[directory]['task'] = new_task
        
        # save configs
        self.save_run_tracker_configs()

        
        
    def set_segments_to_run(self):
        '''  '''
        directory = self.run_tracker_config["active_atlas_directory_name"]
        run_module = self.run_tracker_config[directory]["run_module"]
        # No need to run if not segment as the complete_script will have already been run.
        if not run_module == "segment":
            return
        
        # replace the necessary folders so the correct data is in place before the test is run
        test_configs = get_json(os.path.join(SetUp.configs_dir,'end_to_end_tests.json'))
        
        test_data_dir = SetUp.test_data_dir
        directories = SetUp.directories

        # prompt user on which test group to use
        test_groups, options_string = self.dict_and_string(test_configs)
        msg = f"Select test group:\n{options_string}\n"
        group_choice_id = input(msg)
        group_choice = test_groups[group_choice_id]
        
        # prompt the user on the segments to run
        segments_configs = test_configs[group_choice]['tests']
        segments_dict, segments_string = self.dict_and_string(segments_configs)
        msg = f"Select sub-test group:\n{segments_string}\n"
        segment_choice_ids_raw = input(msg)
        segment_choice_ids = self.build_segments_list(segment_choice_ids_raw, segments_dict)
        
        # create a list of all the segments to run
        segment_choices = []
        for segment_id in segment_choice_ids:
            segment_choices.append(segments_dict[segment_id])
            
        # store the test choices
        self.run_tracker_config[directory]['segments']['group'] = group_choice
        self.run_tracker_config[directory]['segments']['subgroups'] = segment_choices
        
        # save configs
        self.save_run_tracker_configs()
        
    
    def run_segments(self):
        
        directory = self.run_tracker_config["active_atlas_directory_name"]
        group_choice = self.run_tracker_config[directory]['segments']['group']
        segment_choices = self.run_tracker_config[directory]['segments']['subgroups']
        test_configs = get_json(os.path.join(SetUp.configs_dir,'end_to_end_tests.json'))
        segments_configs = test_configs[group_choice]['tests']
        test_data_dir = SetUp.test_data_dir
        directories = SetUp.directories
        
        # check for any pre-run_prompts before starting e.g. new download data may need be manually downloaded before hand
        for segment_name in segment_choices:
            print(f"Starting: {segment_name}")
            if not 'pre-run_prompt' in segments_configs[segment_name].keys():
                continue;
            pre_run_prompt = segments_configs[segment_name]['pre-run_prompt']
            msg = f"Pre-run check: {segment_name}\n{pre_run_prompt} (y/n)\n"
            answer = input(msg)
            self.exit_if_no(answer)
        
        logger(message="User has selected test group '%s' & sub-group '%s'"%(group_choice,(', ').join(segment_choices)), category=4)
        # loop through each segment and run its associated module/method
        for segment_name in segment_choices:
            selected_test = segments_configs[segment_name]
            
            test_data_path = os.path.join(test_data_dir, group_choice, segment_name)
            
            # replace the folders and files required for the test
            # ???? need to make this more efficient for subsequent methods
            dirs_to_replace = os.listdir(test_data_path)
            for folder in dirs_to_replace:
                if folder.endswith(".sql") or folder.endswith(".csv"):
                    continue;
                del_folder_path = os.path.join(directories,directory,folder)
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
                
            # run any pre-functions
            if "pre-function" in selected_test:
                getattr(pre_segment,selected_test["pre-function"])(segment_dir=test_data_path)
                
            # dynamically finds and executes the method
            function_to_test = selected_test['function_to_test']
            select_module = function_to_test['module']
            select_class = function_to_test['class']
            select_method = function_to_test['method']
            
            mymodule = importlib.import_module(select_module)
            myclass = getattr(mymodule,select_class)
            myfunc = getattr(myclass(),select_method)
            myfunc()
            print(f"finished: {segment_name}")
        
        
    @staticmethod
    def dict_and_string(dic):
        test_groups = {str(int(x[:2])): x for x in dic.keys()}
        options_string = ("\n").join(["(%s) - %s"%(x,test_groups[x]) for x in test_groups])
        return test_groups, options_string
    
    @staticmethod
    def build_segments_list(ids,segments_dict):
        """ 
        return a list of values that have been requested by a user with short hand
        syntax.
        1-3 = 1,2,3
        """
        # common error message to display
        error_msg = f"Only use numbers, commas & dashes i.e. '1-3,5,6'. You used '{ids}'"
        # test only numbers, commas & dashes exist
        try:
            num = int(ids.replace('-','').replace(',',''))
        except ValueError:
            raise ValueError(error_msg)
        # create list of potential values
        potential_vals = list(segments_dict.keys())
        # lst of segments to run
        segment_lst = []
        # split on commas and loop through
        segment_grp = ids.split(',')
        for seg_val in segment_grp:
            if '-' in seg_val:
                dash_split = seg_val.split('-')
                # check 2 values exist separated by the dash
                if len(dash_split) > 2 or len(dash_split) == 1:
                    raise ValueError(error_msg)
                # check the values are integers and the 2nd value is higher than the 1st
                try:
                    spread_1 = int(dash_split[0])
                    spread_2 = int(dash_split[1])
                    if spread_1 >= spread_2:
                        raise ValueError
                except ValueError:
                    raise ValueError(error_msg)
                # check the two values are viable options and get their index from the potential values list
                try:
                    index_1 = potential_vals.index(str(spread_1))
                    index_2 = potential_vals.index(str(spread_2))
                except ValueError:
                    raise ValueError(f"'{spread_1}' or '{spread_2}' was not an option.")
                
                segment_lst += potential_vals[index_1:index_2 + 1]
                
            else:
                # check the value exists and append it to the list
                try:
                    index_1 = potential_vals.index(seg_val)
                    segment_lst.append(seg_val)
                except ValueError:
                    raise ValueError(f"'{seg_val}' was not an option.")
                
        # remove all duplicates and arrange in order
        ordered_segment_lst = [x for x in potential_vals if x in segment_lst]
                
        return ordered_segment_lst
            
    @staticmethod   
    def exit_if_no(answer):
        if answer.lower() in ["n","no"]:
            print("Solve the issue and try again.")
            os._exit(1)
            
    @staticmethod   
    def yes_no_boolean(answer):
        """ converts 'yes' to True and anyother answer to False """
        if answer.lower() in ["y","yes"]:
            return True 
        return False

    def save_run_tracker_configs(self):
        ''' save the run configs to the run_tracker.json file and reload SetUp with the new configs '''
        write_json(self.run_tracker_path,self.run_tracker_config)
        update_setup()
        
    def run_complete_script(self):
        """ return true if user has chosen to run the full script, false means the user has chosen to run a segment """
        directory = self.run_tracker_config["active_atlas_directory_name"]
        run_module = self.run_tracker_config[directory]["run_module"]
        # No need to run if not segment as the complete_script will have already been run.
        if run_module == "complete_script":
            return True
        return False
        