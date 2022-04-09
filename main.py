import os
import logging

from functions.segment.data_download import DataDownload
from functions.segment.apply_user_updates import ExtractUserEdits
from functions.segment.preformat import PreformatData
from functions.segment.build_format import CombineDatasets
from functions.segment.spatial_relationships import SpatialRelations
from functions.segment.db_update import ChangesAndUpdate
from functions.segment.commit_new_values import UpdateMissingData
from functions.segment.required_task import PromptRequiredTask

from functions.common.timer import Timer
from functions.setup import Logger



def main():
    timer = Timer()

    # prompt user on required task to undertake; manual edit update or state source download and update
    PromptRequiredTask().all_steps()
    # # save the frontend user edits to the core file
    # ExtractUserEdits().extract_user_edits()
    # # add the missing data updates to the required tables and update the database
    # UpdateMissingData().apply_missing_data_updates()
    # # download the data and convert it to WKT in csv
    # DataDownload().all_data_download()
    # # Format required files, compare with the core and create the change files and update file.
    # PreformatData().all_preformat()
    # # combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later
    # CombineDatasets().combine_datasets() # missing data is collected here
    # # add spatially related data. add crs to each of the geometires in the WKT field and reduce points in Tenement polygons
    # SpatialRelations().build_spatial_relations()
    # # Find the changes between the new and the core files and update them
    # ChangesAndUpdate().find_changes_update_core_and_database()

    # log the total time
    Logger.logger.info("\n\nTotal duration: %s"%(timer.time_past()))



if __name__ == "__main__":
    main()
