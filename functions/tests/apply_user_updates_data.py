import pandas as pd
from functions.common.constants import *

# NAME = {
#         DIRECTORIES: {
#                 CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
#                 EDIT_CSV: {'tree': 'output.edit', 'store': 'edit_dir'}
#         },
#         TABLES: {
#             TEST_TABLE: {
#                 COLUMNS: ,
#                 UNITS: {
#                     CORE_CSV: {
#                         IN_PUT: 
#                         OUTPUT: 
#                     },
#                     EDIT_CSV: {
#                         IN_PUT: 
#                         OUTPUT: 
#                     },
#                     DATABASE: {
#                         IN_PUT: 
#                         OUTPUT:
#                     }
#                 }
#             }
#         }
#     }

# ########################################################
# // _transfer_user_creations_to_core_table_test

# // New edit values should be added to core and edit files with no duplicate _id values
USER_CREATIONS_TO_EDIT_CORE_FILES__NO_EXISTING_EDITS__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                EDIT_CSV: {'tree': 'output.edit', 'store': 'edit_dir'}
        },
        TABLES: {
            TEST_TABLE: {
                COLUMNS: ['name', '_id', 'user_name', 'valid_instance', 'date_created'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20']],
                        OUTPUT: [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],['alt_name_2', 4, 'user', False, '2021-10-24']],
                    },
                    EDIT_CSV: {
                        IN_PUT: [['alt_name_1', 3, 'user', True, '2021-10-20']],
                        OUTPUT: [['alt_name_1', 3, 'user', True, '2021-10-20'], ['alt_name_2', 4, 'user', False, '2021-10-24']],
                    },
                    DATABASE: {
                        IN_PUT: [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],['alt_name_2', 4, 'user', False, '2021-10-24']],
                    }
                }
            }
        }
    }


# // No new edit values to add to core. Core and edit files should remain the same
USER_CREATIONS_TO_EDIT_CORE_FILES__NO_NEW_EDITS__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                EDIT_CSV: {'tree': 'output.edit', 'store': 'edit_dir'}
        },
        TABLES: {
            TEST_TABLE: {
                COLUMNS: ['name', '_id', 'user_name', 'valid_instance', 'date_created'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20']],
                        OUTPUT: [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],],
                    },
                    EDIT_CSV: {
                        IN_PUT: [['alt_name_1', 3, 'user', True, '2021-10-20'],],
                        OUTPUT: [['alt_name_1', 3, 'user', True, '2021-10-20']],
                    },
                    DATABASE: {
                        IN_PUT: [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20']],
                    }
                }
            }
        }
    }


    
# ########################################################
# // transfer_user_edits_to_core

USER_EDITS_TO_CORE_FILES__CONFIGS_1 = {
    HOLDER: {
        "db_to_core_transfer": {
            "pk": "_id",
            "related_field": ["holder_id","name_id","child_id"],
            "is_geospatial": False,
            "table_lst": ["holder_listed", "Parent"]
        },
    }
}

# when user_edit=True then function will look through related table for the edit by comparing the db to core file
""" Info
No edit files exist prior
Holder: only user_edit=True should endup in the edit file
holder_listed: [3, 3], is missing in the db table and Holder with id 3 has user_edit=3, thus 
    represents a user has removed it so it should'nt be present in the core file.
    the edit output should be what the values s have been changed to.
Parent: edit results=None as there were no changes, therefore nothing to record in the edit file.
"""
USER_EDITS_TO_CORE_FILES__NO_EXISTING_EDIT_FILES__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                EDIT_CSV: {'tree': 'output.edit', 'store': 'edit_dir'}
        },
        TABLES: {
            HOLDER: {
                COLUMNS: ['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 'name_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, False, '2021-04-19', '2021-04-19']],
                        OUTPUT: [[1, 'name_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, True, '2021-05-19', '2021-04-19'],],
                    },
                    EDIT_CSV: {
                        OUTPUT: [[2, 'name_2', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, True, '2021-05-19', '2021-04-19'],],
                    },
                    DATABASE: {
                        IN_PUT: [[1, 'name_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, True, '2021-05-19', '2021-04-19']],
                    }
                }
            },
            HOLDER_LISTED: {
                COLUMNS: ['holder_id', 'listed_id'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[2, 1],[3, 2],[3, 3],],
                        OUTPUT: [[2, 1],[3, 2],],
                    },
                    EDIT_CSV: {
                        OUTPUT: [[2, 1],[3, 2],],
                    },
                    DATABASE: {
                        IN_PUT: [[2, 1],[3, 2],],
                    }
                }
            },
            PARENT: {
                COLUMNS: ['_id', 'name_id', 'child_id', 'percown'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 1, 2, 0],[2, 1, 3, 0],],
                        OUTPUT: [[1, 1, 2, 0],[2, 1, 3, 0],],
                    },
                    EDIT_CSV: {
                        OUTPUT: None,
                    },
                    DATABASE: {
                        IN_PUT: [[1, 1, 2, 0],[2, 1, 3, 0],],
                    }
                }
            }
        }
    }



""" Info
Edit files exist prior
Holder: only user_edit=True should endup in the edit file
holder_listed: [3, 3], is missing in the db table and Holder with id 3 has user_edit=3, thus 
    represents a user has removed it so it should'nt be present in the core file.
    the edit output should be what the values s have been changed to.
Parent: edit results=None as there were no changes, therefore nothing to record in the edit file.
"""
USER_EDITS_TO_CORE_FILES__EXISTING_EDIT_FILES__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                EDIT_CSV: {'tree': 'output.edit', 'store': 'edit_dir'}
        },
        TABLES: {
            HOLDER: {
                COLUMNS: ['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, False, '2021-04-19', '2021-04-19']],
                        OUTPUT: [[1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, True, '2021-06-19', '2021-04-19']],
                    },
                    EDIT_CSV: {
                        IN_PUT: [[1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19']],
                        OUTPUT: [[1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, True, '2021-06-19', '2021-04-19']],
                    },
                    DATABASE: {
                        IN_PUT: [[1, 'name_1', 'ss', True, True, True, '2021-04-19', '2021-04-19'],[2, 'name_2', 'ss', True, True, True, '2021-05-19', '2021-04-19'],[3, 'name_3', 'ss', True, True, True, '2021-06-19', '2021-04-19']],
                    }
                }
            },
            HOLDER_LISTED: {
                COLUMNS: ['holder_id', 'listed_id'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 1],[2, 2],[3, 3],[3, 4]],
                        OUTPUT: [[1, 1], [2, 2], [3, 3], [3, 4], [3, 5]],
                    },
                    EDIT_CSV: {
                        IN_PUT: [[1, 1],[2, 2],[3, 3],[3, 4]],
                        OUTPUT: [[1, 1], [2, 2], [3, 3], [3, 4], [3, 5]]
                    },
                    DATABASE: {
                        IN_PUT: [[1, 1],[2, 2],[3, 3],[3, 4],[3, 5],],
                    }
                }
            },
            PARENT: {
                COLUMNS: ['_id', 'name_id', 'child_id', 'percown'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 1, 2, 0],[2, 1, 3, 0],],
                        OUTPUT: [[1, 1, 2, 0], [2, 1, 3, 0]],
                    },
                    DATABASE: {
                        IN_PUT: [[1, 1, 2, 0],[2, 1, 3, 0],],
                    }
                }
            }
        }
    }



# copy the change records from the database to the core files. Edit files are not required here.
# These types of files include the TenementChange, HolderChange
# required: 
# - The date_created for the new database rows needs to be greater or equal to the latest in the core file
# - user can't be ss for the new database entry

""" Info
check new row in the database is copied to the core file. 
"""
DB_CHANGES_TO_CORE_CHANGE_FILES__BASIC_ADD__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
        },
        TABLES: {
            TEST_TABLE: {
                COLUMNS: ['_id', 'ind_id', 'action', 'field', 'holderperc', 'date_created', 'user'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 1000001, 'REMOVE', 'holderperc', 40, '2021-10-10', 'ss'],],
                        OUTPUT: [[1, 1000001, 'REMOVE', 'holderperc', 40, '2021-10-10', 'ss'],[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                    },
                    DATABASE: {
                        IN_PUT: [[1, 1000001, 'REMOVE', 'holderperc', 40, '2021-10-10', 'ss'],[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                    }
                }
            },
        }
    }


""" Info
check new row in the database is copied to the empty core file. 
"""
DB_CHANGES_TO_CORE_CHANGE_FILES__NO_CORE_EXISTS__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
        },
        TABLES: {
            TEST_TABLE: {
                COLUMNS: ['_id', 'ind_id', 'action', 'field', 'holderperc', 'date_created', 'user'],
                UNITS: {
                    CORE_CSV: {
                        OUTPUT: [[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                    },
                    DATABASE: {
                        IN_PUT: [[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                    }
                }
            },
        }
    }


""" Info
check core file remains the same as no new edits exist in the database. 
"""
DB_CHANGES_TO_CORE_CHANGE_FILES__NO_NEW_EDITS__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
        },
        TABLES: {
            TEST_TABLE: {
                COLUMNS: ['_id', 'ind_id', 'action', 'field', 'holderperc', 'date_created', 'user'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                        OUTPUT: [[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                    },
                    DATABASE: {
                        IN_PUT: [[2, 1000002, 'REMOVE', 'holderperc', 50, '2021-10-11', 'user']],
                    }
                }
            },
        }
    }
