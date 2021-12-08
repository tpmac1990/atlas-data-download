import datetime
import os
import logging

from functions.data_download import DataDownload
from functions.apply_user_updates import ExtractUserEdits
from functions.preformat import PreformatData
from functions.build_format import CombineDatasets
from functions.spatial_relationships import SpatialRelations
from functions.db_update import ChangesAndUpdate
from functions.commit_new_values import UpdateMissingData
from functions.required_task import PromptRequiredTask

from functions.timer import time_past, start_time
from functions.setup import Logger



def main():
    func_start = start_time()
    # prompt user on required task to undertake; manual edit update or state source download and update
    PromptRequiredTask()
    # save the frontend user edits to the core file
    ExtractUserEdits()
    # add the missing data updates to the required tables and update the database
    UpdateMissingData()
    # download the data and convert it to WKT in csv
    DataDownload()
    # Format required files, compare with the core and create the change files and update file.
    PreformatData()
    # combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later
    CombineDatasets()
    # add spatially related data. add crs to each of the geometires in the WKT field and reduce points in Tenement polygons
    SpatialRelations()
    # Find the changes between the new and the core files and update them
    ChangesAndUpdate()
    # log the total time
    Logger.logger.info("\n\nTotal duration: %s"%(time_past(func_start)))



if __name__ == "__main__":
    main()
