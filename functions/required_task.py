import os
import sys

import pandas as pd

from .directory_files import get_json, write_json
from .setup import SetUp, Logger


class PromptRequiredTask:
    run_tracker_path = os.path.join(SetUp.configs_dir, 'run_tracker.json')
    run_tracker_config = get_json(run_tracker_path)
    last_task = run_tracker_config['task']
    manual_update_path = os.path.join(SetUp.output_dir, 'update', 'manual_update_required.csv')
    manual_update_df = pd.read_csv(manual_update_path)


    def __init__(self):

        self.prompt_user_on_required_task()


    def prompt_user_on_required_task(self):
        ''' This allows the user to select the task they want to run. The task is saved in the 'required_task.json' file which is read at necessary points in this 
                application to determine if it is required for the task requested here.
            manual_updates: update the yet to be formatted values in the 'manual_update_required.csv' 
            state_source_updates: get the latest updates from the state sources
        '''

        Logger.logger.info(f"\n\n{Logger.hashed}\nPicking the task\n{Logger.hashed}")
        
        df = self.manual_update_df
        total_count = len(df.index)
        update_rows_count = len(df[~df['LIKELY_MATCH'].isnull()])

        Logger.logger.info(f"The 'manual_update_required' table contains a total of '{total_count}' rows with a provided update value for '{update_rows_count}' of these rows.")
        Logger.logger.info(f"Last task run: '{self.last_task}'")

        msg = (
            f"The 'manual_update_required' table contains a total of '{total_count}' rows with a"
            f"\nprovided update value for '{update_rows_count}' of these rows."
            f"\nLast task run: {self.last_task}"
            "\nWhich task would you like to complete (Enter the number of your choice)?"
            "\n(1) Apply manual updates to the database."
            "\n(2) Update datasets using state data sources."
            "\npress any other key to Exit\n"
        )

        choice = input(msg)

        if choice == '1':
            if update_rows_count == 0:
                Logger.logger.info(f"User has selected to run the 'manual_update' task, however there is no data in the 'manual_update_required' file to apply. This process will be terminated.")
                print("You have selected to run the 'manual_update' task, however there is no data in the 'manual_update_required' table to apply.\nExiting process.")
                sys.exit(1)
            new_task = 'manual_updates'
        elif choice == '2':
            new_task = 'state_source_updates'
        else:
            Logger.logger.info(f"The user exited the function")
            print('No task was selected, Exiting process')
            sys.exit(1)

        Logger.logger.info(f"User has selected to run task: '{new_task}'")
        print(f"Preparing to run task: '{new_task}'")

        # save the requested task to the 'required_task.json' file
        self.run_tracker_config['task'] = new_task
        write_json(self.run_tracker_path,self.run_tracker_config)

