import pandas as pd



def LOCAL_SEGMENTS_SINGLE_SUB_GROUP():
    return {
        "active_atlas_directory_name": "local",
        "production": {},
        "local": {
            "task": "",
            "run_module": "segment",
            "last_download_date": "16-02-2022",
            "segments": {
                "group": "01_initial",
                "subgroups": [
                    "04_build_spatial_data"
                ]
            }
        },
        "testing": {}
    }



def LOCAL_COMPLETE_SINGLE_SUB_GROUP():
    return {
        "active_atlas_directory_name": "local",
        "production": {},
        "local": {
            "task": "state_source_updates",
            "run_module": "complete_script",
            "last_download_date": "16-02-2022",
            "segments": None
        },
        "testing": {}
    }



def MANUAL_UPDATE_DF():
    return pd.DataFrame(data={'LIKELY_MATCH': [1, 2]})


def MANUAL_UPDATE_DF_EMPTY():
    return pd.DataFrame(data={'LIKELY_MATCH': []})


def SEGMENTS_TESTS_A():
    return {
            "03_sub_group_3": {},
            "04_sub_group_4": {},
            "05_sub_group_5": {},
            "06_sub_group_6": {},
            "07_sub_group_7": {},
        }
    

def SEGMENTS_TESTS_B():
    return {
            "01_sub_group_1": {},
            "02_sub_group_2": {},
        }
    
    
def E2E_GROUP_SUB_GROUP():
    return {
        "01_group_1": {
            "tests": {
                "03_sub_group_3": {}
            }
        },
        "02_group_2": {
            "tests": SEGMENTS_TESTS_A()
        },
    }
