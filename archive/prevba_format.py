import ctypes
import datetime
from functions import *
from prevba_functions import *


class Functions():

    def __init__(self,data_group):
        csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
        root_directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
        self.data_group = data_group
        self.data_group_directory = "%s%s/" %(root_directory,data_group)
        self.core_directory = self.data_group_directory + 'core/'
        self.plain_directory = self.data_group_directory + 'plain/'
        self.change_directory = self.data_group_directory + 'change/'
        self.new_directory = self.data_group_directory + 'new/'
        self.update_directory = self.data_group_directory + 'update/'
        self.archive_directory = self.data_group_directory + 'archive/'
        self.vba_directory = self.data_group_directory + 'vba/'
        self.tDate = datetime.datetime.now().strftime("%y%m%d")
        self.archive_directory = "%sarchive/%s/" %(self.data_group_directory,self.tDate)
        self.update_path = self.update_directory + 'update.csv'
        createMultipleDirectories(self.archive_directory,['change','core','update'])
        self.output_directory = root_directory + 'output/'
        self.output_archive_directory = "%sarchive/%s/" %(self.output_directory,self.tDate)
        createMultipleDirectories(self.output_archive_directory,['change','core','update'])
        self.configs = getJSON(root_directory + 'scripts/config.json')[data_group]['Primary_Format']

    def preformatTenementFiles(self):
        # archiveRemoveOldFiles(self)
        # archiveRemoveOutputFiles(self)
        # combineSameNameWellsAusOS(self)
        # combinePolygonsWithSameID_VIC(self)
        # deleteSecondofDuplicate_QLD_1(self)
        # removeDuplicateRowsByKeyAllFiles(self)
        # filterAllFilesForRelevantData(self)
        # createUniqueKeyFieldAllFiles(self)
        # combineFilesAllFiles(self)
        # mergeRowsAllFiles(self)
        # sortMultipleValuesString(self)
        # deletingInvalidWktRowsAllFiles(self)
        # addIdentifierField(self)
        createChangeFiles(self)


Functions('occurrence').preformatTenementFiles()
# # print("#"*100)
# # print('occurrence_complete')
# Functions('tenement').preformatTenementFiles()
