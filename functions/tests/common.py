import os
import pandas as pd
import numpy as np
from functions.setup import SetUp
from functions.common.constants import *

from functions.segment.db_update import sqlalchemy_engine, connect_psycopg2

DB_DUMPS_DIR = os.path.join(SetUp.scripts_dir, "functions", "tests", "db_dumps")

DB_CONFIGS = {
    "dbname": TEST_DB,
    "user": POSTGRES,
    "password": PASSWORD,
    "host": LOCALHOST,
}


class DataframeTest:
    @staticmethod
    def create_obj_with_sqlalchemy_engine(obj_name, sqlalchemy_engine):
        obj = obj_name()
        obj.sqlalchemy_con = sqlalchemy_engine
        return obj
    
    @staticmethod
    def create_obj_with_connections(obj_name, sqlalchemy_engine, psycopg_conn):
        obj = obj_name()
        obj.con = sqlalchemy_engine
        obj.sqlalchemy_con = sqlalchemy_engine
        obj.conn = psycopg_conn
        return obj


    def populate_files_and_db_tables(self, data, obj, temp_dir=None):
        """ 
        create and add data to csv files and postgres database tables. Also assign 
        _dir attributes to obj.
        passed dictionary:
            directories: dictionary of directories to assign as attribute to the obj. key name matches inputs key name
                tree: directory tree to be created
                store: attribute name to store the path in on the obj
            paths: dictionary used to assign a path as an attribute of a class object
                key: key is the key to the directory in directories which is the directory of the file
                value:
                    file_name: the file_name and its extension to be stored
                    store: the attribute name the path will be stored in the object class as
            tables: dictionry of tables that have data required to be loaded to file and or database for the required test
                columns: field names of the table
                inputs: 
                    key: used to find the path in the directories dictionary to store the values as csv. if database, then
                        store in database
                    values: the values to store in csv or db table given above.
                ouputs: 
                    key: matches with the field given in directories dictionary above. The list of values are the table the 
                        test expects for a pass
        example:              
        {
            'directories': {
                'core_csv': {'tree': 'output.core', 'store': 'core_dir'},
            },
            'paths': {
                'core_csv': [{'file_name': 'file_name.extension', 'store': 'attribute_name'}]
            },
            'tables': {
                'TestTable': {
                    'columns': ['name', '_id', 'user_name', 'valid_instance', 'date_created'],
                    'inputs': {
                        'core_csv': [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20']],
                        'database': [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],['alt_name_2', 4, 'user', False, '2021-10-24']]
                    },
                    'outputs': {
                        'core_csv': [['name_1', 1, 'ss', True, '2021-09-24'],['alt_name_1', 3, 'user', True, '2021-10-20'],['alt_name_2', 4, 'user', False, '2021-10-24']],
                    },
                }
            }
        }
        """
        directories = data[DIRECTORIES]
        self.create_directories(directories, temp_dir, obj)
        if PATHS in data:
            self.create_paths(data['paths'], directories, obj)
        tables = data[TABLES]
        for table_name in tables:
            table_data = tables[table_name]
            units_data = table_data[UNITS]
            columns = table_data[COLUMNS]
            for unit in units_data:
                unit_data = units_data[unit]
                if IN_PUT in unit_data:
                    input_data = unit_data[IN_PUT]
                    if unit == DATABASE:
                        self.add_test_data_to_db(
                            table_name=table_name, 
                            columns=columns,
                            data=input_data,
                            obj=obj
                            )
                    
                    else:
                        self.add_test_data_to_file(
                            directory=directories[unit],
                            obj=obj,
                            table_name=table_name,
                            data=input_data,
                            columns=columns
                        )
                    
    @staticmethod
    def create_directories(directories, temp_dir, obj):
        """ create the directories in the tmpdir and add them as attributes to the obj """
        for directory in directories:
            tree = directories[directory][TREE]
            split_tree = tree.split('.')
            main_dir_name = split_tree[0]
            sub_dir_name = split_tree[1]

            main_dir = os.path.join(temp_dir, main_dir_name)
            if not os.path.isdir(main_dir):
                temp_dir.mkdir(main_dir_name)

            sub_dir = os.path.join(main_dir, sub_dir_name)
            if not os.path.isdir(sub_dir):
                os.mkdir(sub_dir)
                
            store = directories[directory][STORE]
            if not hasattr(obj, store):
                setattr(obj, store, sub_dir)
        obj
                
    @staticmethod
    def create_paths(paths, directories, obj):
        for key in paths:
            dir_attr_name = directories[key][STORE]
            directory = getattr(obj, dir_attr_name)
            for group in paths[key]:
                file_name = group[FILE_NAME]
                store = group[STORE]
                path = os.path.join(directory, file_name)
                setattr(obj, store, path)
        return obj
                            
                        
    @staticmethod
    def add_test_data_to_db(table_name, columns, data, obj):
        """ add test data to the test database """
        db_table_name = "gp_{0}".format(table_name.lower())
        df = pd.DataFrame(columns=columns, data=data)
        if '_' in table_name:
            df[ID] = np.arange(1, len(df) + 1)
        con = obj.sqlalchemy_con if hasattr(obj, 'sqlalchemy_con') else obj.con
        df.to_sql(db_table_name, con=con, index=False)    
       
    @staticmethod
    def add_test_data_to_file(directory, obj, table_name, data, columns):
        """ add test data to file and add _dir attributes to obj """
        sub_dir = getattr(obj, directory['store'])
                
        file_name = "{0}.csv".format(table_name)
        path = os.path.join(sub_dir, file_name)
        df = pd.DataFrame(columns=columns, data=data)
        df.to_csv(path, index=False)
    
    
    @staticmethod
    def get_csv_file_as_df(directory, table_name):
        """return csv as a df. This allows for easier manipulation on assert"""
        if not directory and not table_name:
            raise TypeError('At least a directory and table_name need to be supplied')
        file_name = "{0}.csv".format(table_name)
        path = os.path.join(directory, file_name)
        return pd.read_csv(path, engine="python")
    
    
    def get_csv_file_as_value_list(self, directory, table_name, fields='__all__'):
        """
        used to help with testing. returns a csv file as list ready to be used as the expected 
        value in 'assert_df_values'
        """
        df = self.get_csv_file_as_df(directory=directory, table_name=table_name)
        if fields == '__all__':
            table = df.values.tolist()
        else:
            table = df[fields].values.tolist()
        string = '\n#### {0} csv output ####\n[\n'.format(table_name)
        for row in table:
            string += '\t{0},\n'.format(str(row))
        string += ']'
        return string
            
    
    @staticmethod
    def return_assert_data(result, expected, table_name, key):
        if result == expected:
            outcome = True
            msg = ''
        else:
            outcome = False
            msg = '\n{0}\nTable: {1}, Key: {2}\n{0}\n{3} \n!= \n{4}'.format('-'*50, table_name, key, result, expected)
        return {RESULT: outcome, MSG: msg}
            
        

    def assert_output_values(self, obj, data):
        """ 
        return True if the list of values of given fields are equal to the expected values. Incase of an error, the two dfs
            are returned so the user can see where the difference lies on failure
        directory: directory holding the csv file. not required if df is supplied.
        table_name: name of the csv file without the .csv extension. not required if df is supplied.
        return_df: if True then return the df created from the csv file. Useful so the df doesn't need to be re-created 
            for subsequent tests.
        df: dataframe. default is a empty list for length can be tested to determine if if exists or not.
        fields: allows single or multiple fields
            single field: 'field_1'
            multiple fields: ['field_1', 'field_2']
        expected: list of values expected to be returned e.g. 
            single field: ['value 1', 'value 2'] 
            multiple fields: [['row1_col1_val', row1_col2_val], ['row2_col1_val', row2_col2_val]]
            
        a file which should not be created can be tested by passing in expected=None
        """
        assertions = []
        directories = data[DIRECTORIES]
        tables = data[TABLES]
        for table in tables:
            units_data = tables[table][UNITS]
            columns = tables[table][COLUMNS]
            for unit in units_data:
                unit_data = units_data[unit]
                if OUTPUT in unit_data:
                    output_data = unit_data[OUTPUT]
                    if unit == DATABASE:
                        outcome = self.compare_test_data_from_db(
                            table_name=table, 
                            columns=columns,
                            expected=output_data,
                            sqlalchemy_con=obj.sqlalchemy_con,
                            key=unit,
                        )
                    
                    else:
                        outcome = self.compare_test_data_from_file(
                            directory=directories[unit],
                            table_name=table,
                            expected=output_data,
                            obj=obj,
                            key=unit,
                        )
                    assertions.append(outcome)
        return assertions
        
        
    
    def compare_test_data_from_db(self, table_name, columns, expected, sqlalchemy_con, key):
        """ compare the ouput database table data with the expected results """
        table_name = "gp_{0}".format(table_name.lower())
        command = f"SELECT * FROM {table_name}"
        df = pd.read_sql(command, sqlalchemy_con)
        # drop id column as it is auto-managed elsewhere
        if ID in df.columns:
            df.drop(columns=[ID],inplace=True)
        result = df.values.tolist()
        return self.return_assert_data(result, expected, table_name, key)
            
    def compare_test_data_from_file(self, directory, table_name, expected, obj, key):
        """ compare the output csv table data with the expected results """
        path = os.path.join(getattr(obj, directory['store']), f"{table_name}.csv")
        try:
            df = pd.read_csv(path, engine='python')
        except FileNotFoundError:
            if expected == None:
                return {RESULT: True, MSG: ''}
            raise
        result = df.values.tolist()
        return self.return_assert_data(result, expected, table_name, key)
               
    
    def make_3_mmrow_df_from_file(self, file_name, directory=SetUp.output_dir, sub_dir='core', rows=3, fields=[]):
        """ 
        returns columns and data lists ready to be inserted into a fixture in the _data file. Fields can
        the be edited. e.g.
            columns=['field_1', 'field_2', 'field_3'],
            data=[['field_1_row_1', 'field_2_row_1', 'field_3_row_1'],['field_1_row_2', 'field_2_row_2', 'field_3_row_2']]
        """
        path = os.path.join(directory, sub_dir, '{}.csv'.format(file_name))
        df = pd.read_csv(path, engine='python')
        if len(fields) > 0:
            df = df[fields]
        df = df.iloc[0:rows]
        values = df.values.tolist()
        string = "\n{0}\nreturn pd.DataFrame(\n\tcolumns=".format('-'*100)
        string += str(list(df.columns))
        string += ",\n\tdata=["
        for val in values:
            string += "\n\t\t{0},".format(str(val))
        if len(values) > 0:
            string += "\n\t]"
        else:
            string += "]"
        string += "\n)\n{0}".format('-'*100)
        
        return string
    
        
    def create_fixture_ready_data_from_file(self, file_name, directory=SetUp.output_dir, sub_dir='core', rows=3, fields=[]):
        """ 
        returns pd.DataFrame() string that is ready to be pasted in to the _data file and 
        columns & data lists ready to be pasted into the fixtures object. Fields can
        the be edited. e.g.
        return pd.DataFrame(
                columns=['field_1', 'field_2', 'field_3'],
                data=[
                    ['field_1_row_1', 'field_2_row_1', 'field_3_row_1'],
                    ['field_1_row_2', 'field_2_row_2', 'field_3_row_2'],
                ]
        )
        columns=['field_1', 'field_2', 'field_3']
           data=[['field_1_row_1', 'field_2_row_1', 'field_3_row_1'],['field_1_row_2', 'field_2_row_2', 'field_3_row_2']]
        """
        if sub_dir:
            path = os.path.join(directory, sub_dir, '{}.csv'.format(file_name))
        else:
            path = os.path.join(directory, '{}.csv'.format(file_name))
        df = pd.read_csv(path, engine='python')
        if len(fields) > 0:
            df = df[fields]
        df = df.iloc[0:rows]
        values = df.values.tolist()
        # create the 'ready df string'
        df_string = "\n{0} Dataframe data {0}\nreturn pd.DataFrame(\n\tcolumns=".format('-'*40)
        df_string += str(list(df.columns))
        df_string += ",\n\tdata=["
        for val in values:
            df_string += "\n\t\t{0},".format(str(val))
        if len(values) > 0:
            df_string += "\n\t]"
        else:
            df_string += "]"
        df_string += "\n)\n"
        
        # create the 'fixture ready string'
        fix_string = "{0} Fixture data {0}\ncolumns=".format('-'*40)
        fix_string += str(list(df.columns))
        fix_string += "\n   data=["
        for val in values:
            fix_string += "{0},".format(str(val))
        fix_string += "]\n{0}".format('-'*100)
        
        return df_string + fix_string