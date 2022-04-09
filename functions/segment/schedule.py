import os
from functions.common.directory_files import get_json, write_json
from ..setup import SetUp


class Schedule:
    schedule_config_path = os.path.join(SetUp.configs_dir,'download_schedule.json')

    def open_schedule(self):
        return get_json(self.schedule_config_path)

    def update_schedule(self,configs):
        write_json(self.schedule_config_path, configs)

    def failed_file(self,file,data_group,configs):
        ''' there was some type of error so remove from now so no further steps will be attempted, 
            add to today so it will be attempted again tomorrow 
        '''
        # remove file from 'now' list
        configs[data_group]['now'].remove(file)
        # add file to 'today' list so it is attempted again on next update
        configs[data_group]['today'].append(file)
        return configs

    def unrequired_file(self,file,data_group,configs):
        ''' remove from now, but don't add to today as the file exists, there is just no data.
            only one of the datagroups segments is passed, there no need for the data_group param
        '''
        # remove file from 'now' list
        configs[data_group]['now'].remove(file)
        return configs





