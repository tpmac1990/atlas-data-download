from functions.common.constants import *



# // 

COMMIT_UPDATES_CONFIGS = {
    "HOLDER": {
        "find_matches": {
            "type": "core_match",
            "file": "Holder_pre",
            "field": "name"
        },
        "update_core": {
            "raw_file": "Companies_R",
            "raw_update": {"style":"simple","fields":{"ORIGINAL":"DS_VALUE","LIKELY_MATCH":"GP_VALUE"}},
            "id_file": {"name":"Holder","left_on":"LIKELY_MATCH","right_on":"name","index":"_id"},
            "miss_merge": {"left_on":"VALUE","right_on":"ORIGINAL"},
            "ind_file": "TenHolder",
            "df_build": {"user_name": "ss", "valid_relations": True, "valid_instance": True, "user_edit": False, "date_modified": "date", "date_created": "date"},
            "remove_temp": True,
            "db_update": {"type":"holder","columns":["tenement_id","name_id"],"index":None,"proccess":["clear","append"]}
        }
    },
}


"""
MANUAL_UPDATE_REQUIRED and MISSING_ALL are empty so nothing to do here
"""
COMMIT_FIELDS_UPDATED_DATA__NO_NEW_VALUES__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                UPDA_CSV: {'tree': 'output.update', 'store': 'update_dir'},
                CONV_CSV: {'tree': 'convert.conversion', 'store': 'convert_dir'}
        },
        PATHS: {
            UPDA_CSV: [
                {FILE_NAME: 'manual_update_required.csv', STORE: 'manual_update_path'},
                {FILE_NAME: 'missing_all.csv', STORE: 'missing_all_path'}
            ]
        },
        TABLES: {
            MANUAL_UPDATE_REQUIRED: {
                COLUMNS: ['STATE', 'GROUP', 'FIELD', 'ORIGINAL', 'LIKELY_MATCH', 'MATCH_A'],
                UNITS: {
                    UPDA_CSV: {
                        IN_PUT: [],
                    },
                }
            },
            MISSING_ALL: {
                COLUMNS: ['STATE', 'GROUP', 'FIELD', 'IND', 'VALUE'],
                UNITS: {
                    UPDA_CSV: {
                        IN_PUT: [],
                    },
                }
            }
        }
    }



"""
update single value that exists already in conversion (raw) file
"""
COMMIT_FIELDS_UPDATED_DATA__UPDATE_VALUE_THAT_EXISTS_IN_CONVERSION_FILE__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                UPDA_CSV: {'tree': 'output.update', 'store': 'update_dir'},
                CONV_CSV: {'tree': 'convert.conversion', 'store': 'convert_dir'}
        },
        PATHS: {
            UPDA_CSV: [
                {FILE_NAME: 'manual_update_required.csv', STORE: 'manual_update_path'},
                {FILE_NAME: 'missing_all.csv', STORE: 'missing_all_path'}
            ]
        },
        TABLES: {
            MANUAL_UPDATE_REQUIRED: {
                COLUMNS: ['STATE', 'GROUP', 'FIELD', 'ORIGINAL', 'LIKELY_MATCH', 'MATCH_A'],
                UNITS: {
                    UPDA_CSV: {
                        IN_PUT: [['NSW', None, 'HOLDER', 'HOLDER_1', 'HOLDER', 'None 1']],
                    },
                }
            },
            MISSING_ALL: {
                COLUMNS: ['STATE', 'GROUP', 'FIELD', 'IND', 'VALUE'],
                UNITS: {
                    UPDA_CSV: {
                        IN_PUT: [['NSW', None, 'HOLDER', 1000001, 'HOLDER_1']],
                    },
                }
            },
            COMPANIES_R: {
                COLUMNS: ['DS_VALUE', 'GP_VALUE'],
                UNITS: {
                    CONV_CSV: {
                        IN_PUT: [['HOLDER_1', 'HOLDER']],
                        OUTPUT: [['HOLDER_1', 'HOLDER']]
                    },
                }
            },
            HOLDER: {
                COLUMNS: ['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 'HOLDER', 'ss', True, True, False, '2022-02-16', '2022-02-16'],[2, 'HOLDER_1', 'ss', True, True, False, '2021-04-19', '2021-04-19']],
                        OUTPUT: [[1, 'HOLDER', 'ss', True, True, False, '2022-02-16', '2022-02-16']]
                    },
                    DATABASE: {
                        IN_PUT: [[1, 'HOLDER', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[2, 'HOLDER_1', 'ss', True, True, False, '2021-04-19', '2021-04-19']],
                        OUTPUT: [[1, 'HOLDER', 'ss', True, True, False, '2022-02-16', '2022-02-16']]
                    }
                }
            },
            TENHOLDER: {
                COLUMNS: ['tenement_id', 'percown', 'name_id', '_id'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 2, 2]],
                        OUTPUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 1, 2]]
                    },
                    DATABASE: {
                        IN_PUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 2, 2]],
                        OUTPUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 1, 2]]
                    }
                }
            }
        }
    }









SIMPLE__FIXTURE = {
        DIRECTORIES: {
                CORE_CSV: {'tree': 'output.core', 'store': 'core_dir'},
                UPDA_CSV: {'tree': 'output.update', 'store': 'update_dir'},
                CONV_CSV: {'tree': 'convert.conversion', 'store': 'convert_dir'}
        },
        PATHS: {
            UPDA_CSV: [
                {FILE_NAME: 'manual_update_required.csv', STORE: 'manual_update_path'},
                {FILE_NAME: 'missing_all.csv', STORE: 'missing_all_path'}
            ]
        },
        TABLES: {
            MANUAL_UPDATE_REQUIRED: {
                COLUMNS: ['STATE', 'GROUP', 'FIELD', 'ORIGINAL', 'LIKELY_MATCH', 'MATCH_A'],
                UNITS: {
                    UPDA_CSV: {
                        IN_PUT: [['NSW', None, 'HOLDER', 'HOLDER_1', 'HOLDER', 'None 1'],['NSW', None, 'HOLDER', 'HOLDER_2', 'HOLDER', 'None 2'],],
                    },
                }
            },
            MISSING_ALL: {
                COLUMNS: ['STATE', 'GROUP', 'FIELD', 'IND', 'VALUE'],
                UNITS: {
                    UPDA_CSV: {
                        IN_PUT: [['NSW', None, 'HOLDER', 1000001, 'HOLDER_1'],['NSW', None, 'HOLDER', 1000002, 'HOLDER_2'],],
                    },
                }
            },
            COMPANIES_R: {
                COLUMNS: ['DS_VALUE', 'GP_VALUE'],
                UNITS: {
                    CONV_CSV: {
                        IN_PUT: [['HOLDER_A', 'HOLDER_B'],],
                        OUTPUT: [['HOLDER_A', 'HOLDER_B'],['HOLDER_1', 'HOLDER'],['HOLDER_2', 'HOLDER']]
                    },
                }
            },
            HOLDER: {
                COLUMNS: ['_id', 'name', 'user_name', 'valid_relations', 'valid_instance', 'user_edit', 'date_modified', 'date_created'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1, 'HOLDER_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[2, 'HOLDER_1', 'ss', True, True, False, '2021-04-19', '2021-04-19']],
                        OUTPUT: [[1, 'HOLDER', 'ss', True, True, False, '2022-02-16', '2022-02-16']]
                    },
                    DATABASE: {
                        IN_PUT: [[1, 'HOLDER_1', 'ss', True, True, False, '2021-04-19', '2021-04-19'],[2, 'HOLDER_1', 'ss', True, True, False, '2021-04-19', '2021-04-19']],
                        OUTPUT: [[1, 'HOLDER', 'ss', True, True, False, '2022-02-16', '2022-02-16']]
                    }
                }
            },
            TENHOLDER: {
                COLUMNS: ['tenement_id', 'percown', 'name_id', '_id'],
                UNITS: {
                    CORE_CSV: {
                        IN_PUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 2, 2]],
                        OUTPUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 1, 2]]
                    },
                    DATABASE: {
                        IN_PUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 2, 2]],
                        OUTPUT: [[1000001, 100.0, 1, 1],[1000002, 100.0, 1, 2]]
                    }
                }
            }
        }
    }

