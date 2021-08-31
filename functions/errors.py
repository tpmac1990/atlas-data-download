import os
import pandas as pd 
import numpy as np
import ctypes
import csv

from .directory_files import fileExist


def check_csvs_for_errors(self):
    ''' checks for errors in the data before it it loaded to the database. This inludes blanks and primary keys that 
        don't exist in the related file.
    '''
    csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

    self.new_dir = os.path.join(self.output_dir,'new')
    self.core_dir = os.path.join(self.output_dir,'core')

    miss_id_dic = find_missing_ids_related_files(self)
    blanks_dic = find_blanks_in_tables(self)

    errors = True if miss_id_dic != {} or blanks_dic != {} else False

    if errors:
        try:
            raise Exception('Errors where found in the data. They need to be fixed before progressing.',{'missing_ids': miss_id_dic, 'blanks': blanks_dic})
        except Exception as e:
            print(e)

    # return {'errors': errors, 'missing_ids': miss_id_dic, 'blanks': blanks_dic}



def find_blanks_in_tables(self):
    ''' checks for blanks in tables and returns the table and field they exist in '''

    blank_lst = ["TenHolder","Tenement","Occurrence_pre"]

    blank_dic = {}

    for fname in blank_lst:
        path = os.path.join(self.new_dir,"%s.csv"%(fname))
        if fileExist(path):
            df = pd.read_csv(path,engine='python')
            na_fields = df.columns[df.isna().any()].tolist()
            if len(na_fields) > 0:
                blank_dic[fname] = na_fields

    return(blank_dic)


def find_missing_ids_related_files(self):
    ''' compares related files and checks that there are no primary keys in a file that don't exist in its related file.
        This will return a dictionary of the missing ids for the file and field.
    '''

    id_dic = {}

    lst = [
        {
            'check': {'name': 'TenHolder', 'column': 'name_id'},
            'constant': {'name': 'Holder', 'column': '_id'}
        }
    ]

    for dic in lst:

        check_name = dic['check']['name']
        const_name = dic['constant']['name']
        check_col = dic['check']['column']
        const_col = dic['constant']['column']

        check_new_df = pd.read_csv(os.path.join(self.new_dir,"%s.csv"%(check_name)),engine='python')
        check_core_df = pd.read_csv(os.path.join(self.core_dir,"%s.csv"%(check_name)),engine='python')

        const_new_df = pd.read_csv(os.path.join(self.new_dir,"%s.csv"%(const_name)),engine='python')
        const_core_df = pd.read_csv(os.path.join(self.core_dir,"%s.csv"%(const_name)),engine='python')

        check_df = pd.concat((check_core_df,check_new_df))[check_col]
        const_df = pd.concat((const_core_df,const_new_df))[const_col]

        merg_df = pd.merge(check_df, const_df, left_on=check_col, right_on=const_col, how='outer', indicator=True)
        missing_lst = merg_df.query('_merge == "left_only"').drop(columns=[check_col,'_merge'], axis=1).drop_duplicates()[const_col].values.tolist()

        if len(missing_lst) > 0:
            id_dic[check_name] = {check_col: missing_lst}

    return(id_dic)