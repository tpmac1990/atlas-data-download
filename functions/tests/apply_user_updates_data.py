import pandas as pd
from functions.common.constants import *

# ########################################################
# // _transfer_user_creations_to_core_table_test

# // New edit values should be added to core and edit files with no duplicate _id values

# NEW_FIXTURE_1 = {
#     'TestTable': {
#         'columns': ['name', '_id', 'user_name', 'valid_instance', 'date_created'],
#         'inputs': {
#             'core_cs': [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20']],
#             'edit_cs': [['alt_name_1', 3, 'user', True, '2021-10-20']],
#             'databas': [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],['alt_name_2', 4, 'user', False, '2021-10-24']]
#         },
#         'outputs': {
#             'core_cs': [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],['alt_name_2', 4, 'user', False, '2021-10-24']],
#             'edit_cs': [['alt_name_1', 3, 'user', True, '2021-10-20'], ['alt_name_2', 4, 'user', False, '2021-10-24']],
#         }
#     }
# }



# table existing in output file. 
def CORE_TABLE_1_DF():
    return pd.DataFrame(
        columns=['name', '_id', 'user_name', 'valid_instance', 'date_created'],
        data=[
            ['name_1', 1, 'ss', True, '2021-09-24'],
            ['alt_name_1', 3, 'user', True, '2021-10-20']
        ])

# how the table exists in the database
def DB_TABLE_1_DF():
    return pd.DataFrame(
        columns=['name', '_id', 'user_name', 'valid_instance', 'date_created'], 
        data=[
            ['name_1', 1, 'ss', True, '2021-09-24'],
            ['alt_name_1', 3, 'user', True, '2021-10-20'],
            ['alt_name_2', 4, 'user', False, '2021-10-24']
        ])

# table containing all past user edits
def EDIT_TABLE_1_DF():
    return pd.DataFrame(
        columns=['name', '_id', 'user_name', 'valid_instance', 'date_created'],
        data=[
            ['alt_name_1', 3, 'user', True, '2021-10-20'],
        ])

# data expected in the edit file
EDIT_TABLE_OUTPUT_1 = [
            ['alt_name_1', 3, 'user', True, '2021-10-20'], 
            ['alt_name_2', 4, 'user', False, '2021-10-24']
        ]
    
# data expected in the core file
CORE_TABLE_OUTPUT_1 = [
            ['name_1', 1, 'ss', True, '2021-09-24'],
            ['alt_name_1', 3, 'user', True, '2021-10-20'],
            ['alt_name_2', 4, 'user', False, '2021-10-24']
        ]

# collection of data passed to the test
class FIXTURES_1:
    to_csv = [
            [CORE_TABLE_1_DF, OUTPUT_CORE, TEST_TABLE, CORE_DIR],
            [EDIT_TABLE_1_DF, OUTPUT_EDIT, TEST_TABLE, EDIT_DIR],
        ]
    to_db = [
            [DB_TABLE_1_DF, TEST_TABLE]
        ]
    file_output = [
            {'directory': 'edit_dir', 'table': TEST_TABLE, 'result': EDIT_TABLE_OUTPUT_1},
            {'directory': 'core_dir', 'table': TEST_TABLE, 'result': CORE_TABLE_OUTPUT_1}
        ]
    

# // No new edit values to add to core. Core and edit files should remain the same

# how the table exists in the database
def DB_TABLE_2_DF():
    return pd.DataFrame(
        columns=['name', '_id', 'user_name', 'valid_instance', 'date_created'], 
        data=[
            ['name_1', 1, 'ss', True, '2021-09-24'],
            ['alt_name_1', 3, 'user', True, '2021-10-20']
        ])

# table containing all past user edits
def EDIT_TABLE_2_DF():
    return pd.DataFrame(
        columns=['name', '_id', 'user_name', 'valid_instance', 'date_created'],
        data=[
            ['alt_name_1', 3, 'user', True, '2021-10-20'],
        ])

# data expected in the edit file
EDIT_TABLE_OUTPUT_2 = [
            ['alt_name_1', 3, 'user', True, '2021-10-20'],
        ]
    
# data expected in the core file
CORE_TABLE_OUTPUT_2 = [
            ['name_1', 1, 'ss', True, '2021-09-24'],
            ['alt_name_1', 3, 'user', True, '2021-10-20'],
        ]

# collection of data passed to the test
class FIXTURES_2:
    to_csv = [
            [CORE_TABLE_1_DF, OUTPUT_CORE, TEST_TABLE, CORE_DIR],
            [EDIT_TABLE_2_DF, OUTPUT_EDIT, TEST_TABLE, EDIT_DIR],
        ]
    to_db = [
            [DB_TABLE_2_DF, TEST_TABLE]
        ]    
    file_output = [
            {'directory': 'edit_dir', 'table': TEST_TABLE, 'result': EDIT_TABLE_OUTPUT_2},
            {'directory': 'core_dir', 'table': TEST_TABLE, 'result': CORE_TABLE_OUTPUT_2}
        ]
    
    
# ########################################################
# // transfer_user_edits_to_core

# when user_edit=True then function will look through related table for the edit by comparing the db to core file

UPDATE_CONFIGS_1 = {
    "Holder": {
        "db_to_core_transfer": {
            "pk": "_id",
            "related_field": ["holder_id","name_id","child_id"],
            "is_geospatial": False,
            "table_lst": ["holder_listed", "Parent"]
        },
    }
}

def CORE_HOLDER_TABLE_1_DF():
    return pd.DataFrame(
        columns=['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
        data=[
            [1, 'name_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],
            [2, 'name_2', 'ss', True, True, False, '2021-04-19', '2021-04-19'],
            [3, 'name_3', 'ss', True, True, False, '2021-04-19', '2021-04-19']
        ])

def DB_HOLDER_TABLE_1_DF():
    return pd.DataFrame(
        columns=['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
        data=[
            [1, 'name_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],
            [2, 'name_2', 'ss', True, True, True, '2021-04-19', '2021-04-19'],
            [3, 'name_3', 'ss', True, True, True, '2021-05-19', '2021-04-19']
        ])
    

def CORE_HOLDER_LISTED_TABLE_1_DF():
    return pd.DataFrame(
        columns=['holder_id', 'listed_id'],
        data=[
                [2, 1],
                [3, 2],
                [3, 3],
        ]
)
    
def CORE_PARENT_TABLE_1_DF():  
    return pd.DataFrame(
        columns=['_id', 'name_id', 'child_id', 'percown'],
        data=[
                [1, 1, 2, 0],
                [2, 1, 3, 0],
        ]
    )
    
def DB_HOLDER_LISTED_TABLE_1_DF():
    return pd.DataFrame(
        columns=['holder_id', 'listed_id'],
        data=[
                [2, 1],
                [3, 2],
        ]
)
    
CORE_HOLDER_TABLE_OUTPUT_1 = [
        [1, 'name_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],
        [2, 'name_2', 'ss', True, True, True, '2021-04-19', '2021-04-19'],
        [3, 'name_3', 'ss', True, True, True, '2021-05-19', '2021-04-19'],
    ]

EDIT_HOLDER_TABLE_OUTPUT_1 = [
        [2, 'name_2', 'ss', True, True, True, '2021-04-19', '2021-04-19'],
        [3, 'name_3', 'ss', True, True, True, '2021-05-19', '2021-04-19'],
    ]
    


""" Info
No edit files exist prior
Holder: only user_edit=True should endup in the edit file
holder_listed: [3, 3], is missing in the db table and Holder with id 3 has user_edit=3, thus 
    represents a user has removed it so it should'nt be present in the core file.
    the edit output should be what the values s have been changed to.
Parent: edit results=None as there were no changes, therefore nothing to record in the edit file.
"""
class FIXTURES_HOLDER_1:
    to_csv = [
            [CORE_HOLDER_TABLE_1_DF, OUTPUT_CORE, 'Holder', CORE_DIR],
            [CORE_HOLDER_LISTED_TABLE_1_DF, OUTPUT_CORE, 'holder_listed', CORE_DIR],
            [CORE_PARENT_TABLE_1_DF, OUTPUT_CORE, 'Parent', CORE_DIR],
            [None, OUTPUT_EDIT, None, EDIT_DIR]
        ]
    to_db = [
            [DB_HOLDER_TABLE_1_DF, 'Holder'],
            [DB_HOLDER_LISTED_TABLE_1_DF, 'holder_listed'],
            [CORE_PARENT_TABLE_1_DF, 'Parent']
        ]
    file_output = [
        {'directory': 'core_dir', 'table': 'Holder', 'result': CORE_HOLDER_TABLE_OUTPUT_1},
        {'directory': 'edit_dir', 'table': 'Holder', 'result': EDIT_HOLDER_TABLE_OUTPUT_1},
        {'directory': 'core_dir', 'table': 'holder_listed', 'result': [[2, 1],[3, 2],]},
        {'directory': 'edit_dir', 'table': 'holder_listed', 'result': [[2, 1],[3, 2],]},
        {'directory': 'core_dir', 'table': 'Parent', 'result': [[1, 1, 2, 0], [2, 1, 3, 0],]},
        {'directory': 'edit_dir', 'table': 'Parent', 'result': None}
    ]
    
    
    
    
def CORE_HOLDER_TABLE_2_DF():
    return pd.DataFrame(
        columns=['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
        data=[
            [1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],
            [2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],
            [3, 'name_3', 'ss', True, True, False, '2021-04-19', '2021-04-19']
        ])

def DB_HOLDER_TABLE_2_DF():
    return pd.DataFrame(
        columns=['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
        data=[
            [1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],
            [2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],
            [3, 'name_3', 'ss', True, True, True, '2021-06-19', '2021-04-19']
        ])
    
def EDIT_HOLDER_TABLE_2_DF():
    return pd.DataFrame(
        columns=['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
        data=[
            [1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],
            [2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],
        ])   
    
def CORE_HOLDER_LISTED_TABLE_2_DF():
    return pd.DataFrame(
        columns=['holder_id', 'listed_id'],
        data=[
                [1, 1],
                [2, 2],
                [3, 3],
                [3, 4],
        ]
)
    
def EDIT_HOLDER_LISTED_TABLE_2_DF():
    return pd.DataFrame(
        columns=['holder_id', 'listed_id'],
        data=[
                [1, 1],
                [2, 2],
                [3, 3],
                [3, 4],
        ]
)
      
def DB_HOLDER_LISTED_TABLE_2_DF():
    return pd.DataFrame(
        columns=['holder_id', 'listed_id'],
        data=[
                [1, 1],
                [2, 2],
                [3, 3],
                [3, 4],
                [3, 5],
        ]
)
    
CORE_HOLDER_TABLE_OUTPUT_2 = [
                [1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'], 
                [2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'], 
                [3, 'name_3', 'ss', True, True, True, '2021-06-19', '2021-04-19']
            ]

EDIT_HOLDER_TABLE_OUTPUT_2 = [
                [1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'], 
                [2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'], 
                [3, 'name_3', 'ss', True, True, True, '2021-06-19', '2021-04-19']
            ]

    
    
""" Info
Edit files exist prior
Holder: only user_edit=True should endup in the edit file
holder_listed: [3, 3], is missing in the db table and Holder with id 3 has user_edit=3, thus 
    represents a user has removed it so it should'nt be present in the core file.
    the edit output should be what the values s have been changed to.
Parent: edit results=None as there were no changes, therefore nothing to record in the edit file.
"""
class FIXTURES_HOLDER_2:
    to_csv = [
            [CORE_HOLDER_TABLE_2_DF, OUTPUT_CORE, 'Holder', CORE_DIR],
            [EDIT_HOLDER_TABLE_2_DF, OUTPUT_EDIT, 'Holder', EDIT_DIR],
            [CORE_HOLDER_LISTED_TABLE_2_DF, OUTPUT_CORE, 'holder_listed', CORE_DIR],
            [EDIT_HOLDER_LISTED_TABLE_2_DF, OUTPUT_EDIT, 'holder_listed', EDIT_DIR],
            [CORE_PARENT_TABLE_1_DF, OUTPUT_CORE, 'Parent', CORE_DIR],
            [None, OUTPUT_EDIT, None, EDIT_DIR]
        ]
    to_db = [
            [DB_HOLDER_TABLE_2_DF, 'Holder'],
            [DB_HOLDER_LISTED_TABLE_2_DF, 'holder_listed'],
            [CORE_PARENT_TABLE_1_DF, 'Parent']
        ]
    file_output = [
        {'directory': 'core_dir', 'table': 'Holder', 'result': CORE_HOLDER_TABLE_OUTPUT_2},
        {'directory': 'edit_dir', 'table': 'Holder', 'result': EDIT_HOLDER_TABLE_OUTPUT_2},
        {'directory': 'core_dir', 'table': 'holder_listed', 'result': [[1, 1], [2, 2], [3, 3], [3, 4], [3, 5]]},
        {'directory': 'edit_dir', 'table': 'holder_listed', 'result': [[1, 1], [2, 2], [3, 3], [3, 4], [3, 5]]},
        {'directory': 'core_dir', 'table': 'Parent', 'result': [[1, 1, 2, 0], [2, 1, 3, 0],]},
    ]
    
    
