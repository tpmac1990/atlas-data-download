import ctypes
from functions import *
from prevba_functions import *


class Functions():

    def __init__(self):
        csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
        root_directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
        self.output_directory = root_directory + 'output/'
        self.new_directory = self.output_directory + 'new/'
        self.change_directory = self.output_directory + 'change/'
        self.core_directory = self.output_directory + 'core/'
        self.update_directory = self.output_directory + 'update/'
        
        self.tenement_update_path = root_directory + 'tenement/update/update.csv'
        self.occurrence_update_path = root_directory + 'occurrence/update/update.csv'

        self.configs = getJSON(root_directory + 'scripts/db_update_configs.json')


    def recordChangesUpdateDb(self):
        createOutputChangeFiles(self)




Functions().recordChangesUpdateDb()



