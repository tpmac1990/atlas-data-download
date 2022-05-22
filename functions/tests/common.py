import os
import pandas as pd
import numpy as np
from functions.setup import SetUp
from functions.common.constants import *


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


    # @staticmethod
    # def add_test_dfs_to_db(tables_data_list, con):
    #     """inserts list of dataframes in to the database. makes sure dependent tables are inserted last"""
    #     for table_group in tables_data_list:
    #         TABLE_DF = table_group[0]
    #         db_table_name = "gp_{0}".format(table_group[1].lower())
            
    #         df = TABLE_DF()
    #         if '_' in table_group[1]:
    #             df['id'] = np.arange(1, len(df) + 1)

    #         df.to_sql(db_table_name, con=con, index=False)


    # @staticmethod
    # def add_test_dfs_as_csv_to_folders(tables_data_list, obj, temp_dir):
    #     """
    #     This converts a list of dataframes to csv and inserts it into a temp directory

    #     tables_data_list: [(CORE_TABLE_1_DF, 'output.core', 'my_cool_table.csv', 'core_dir'),]
    #         [0]: dataframe to be converted to csv
    #         [1]: {main_dir}.{sub_dir} to be created and which will home the csv file
    #         [2]: table name without .csv extension
    #         [3]: attribute name which holds the path to the directory which holds the csv file
            
    #     to add a directory without a file, leave the df and file_name as None:
    #         [None, OUTPUT_EDIT, None, EDIT_DIR]

    #     IMPORTANT: order dataframes are created here does not matter as they are not being inserted
    #     into the database
    #     """

    #     for table_group in tables_data_list:
    #         TABLE_DF = table_group[0]
    #         split_dir = table_group[1].split(".")
    #         main_dir_name = split_dir[0]
    #         sub_dir_name = split_dir[1]
    #         attr_name = table_group[3]
    #         file_name = "{0}.csv".format(table_group[2].lower()) if table_group[2] else None
        
    #         main_dir = os.path.join(temp_dir, main_dir_name)
    #         if not os.path.isdir(main_dir):
    #             temp_dir.mkdir(main_dir_name)

    #         if not hasattr(obj, attr_name):
    #             sub_dir = os.path.join(main_dir, sub_dir_name)
    #             if not os.path.isdir(sub_dir):
    #                 os.mkdir(sub_dir)
    #             setattr(obj, attr_name, sub_dir)

    #         if file_name:
    #             path = os.path.join(main_dir, sub_dir_name, file_name)

    #             TABLE_DF().to_csv(path, index=False)

    #     return obj

    
    def populate_files_and_db_tables(self, data, obj, temp_dir=None):
        """ 
        create and add data to csv files and postgres database tables. Also assign 
        _dir attributes to obj.
        passed dictionary:
            directories: dictionary of directories to assign as attribute to the obj. key name matches inputs key name
                tree: directory tree to be created
                store: attribute name to store the path in on the obj
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
        directories = data['directories']
        self.create_directories(directories, temp_dir, obj)
        tables = data['tables']
        for table_name in tables:
            table_data = tables[table_name]
            input_data = table_data['inputs']
            columns = table_data['columns']
            for input_table in input_data:
                if input_table == 'database':
                    self.add_test_data_to_db(
                        table_name=table_name, 
                        columns=columns,
                        data=input_data[input_table],
                        obj=obj
                        )
                    
                else:
                    self.add_test_data_to_file(
                        directory=directories[input_table],
                        # key=input_table,
                        # temp_dir=temp_dir,
                        obj=obj,
                        table_name=table_name,
                        data=input_data[input_table],
                        columns=columns
                    )
                    
    @staticmethod
    def create_directories(directories, temp_dir, obj):
        """ create the directories in the tmpdir and add them as attributes to the obj """
        for directory in directories:
            tree = directories[directory]['tree']
            split_tree = tree.split('.')
            main_dir_name = split_tree[0]
            sub_dir_name = split_tree[1]

            main_dir = os.path.join(temp_dir, main_dir_name)
            if not os.path.isdir(main_dir):
                temp_dir.mkdir(main_dir_name)

            sub_dir = os.path.join(main_dir, sub_dir_name)
            if not os.path.isdir(sub_dir):
                os.mkdir(sub_dir)
                
            store = directories[directory]['store']
            if not hasattr(obj, store):
                setattr(obj, store, sub_dir)
                        
                        
    @staticmethod
    def add_test_data_to_db(table_name, columns, data, obj):
        """ add test data to the test database """
        db_table_name = "gp_{0}".format(table_name.lower())
        df = pd.DataFrame(columns=columns, data=data)
        if '_' in table_name:
            df['id'] = np.arange(1, len(df) + 1)
        df.to_sql(db_table_name, con=obj.sqlalchemy_con, index=False)    
       
    @staticmethod
    def add_test_data_to_file(directory, obj, table_name, data, columns):
        """ add test data to file and add _dir attributes to obj """
        # # tree = directory['tree']
        # # split_tree = tree.split('.')
        # # main_dir_name = split_tree[0]
        # # sub_dir_name = split_tree[1]

        # # main_dir = os.path.join(temp_dir, main_dir_name)
        # # if not os.path.isdir(main_dir):
        # #     temp_dir.mkdir(main_dir_name)

        # # sub_dir = os.path.join(main_dir, sub_dir_name)
        # # if not os.path.isdir(sub_dir):
        # #     os.mkdir(sub_dir)
        
        # store = directory['store']
        # # if not hasattr(obj, store):
        # #     setattr(obj, store, sub_dir)
        sub_dir = getattr(obj, directory['store'])
                
        file_name = "{0}.csv".format(table_name)
        path = os.path.join(sub_dir, file_name)
        df = pd.DataFrame(columns=columns, data=data)
        df.to_csv(path, index=False)
            
            
        
        # if len(data.to_csv) > 0:
        #     if temp_dir:
        #         self.add_test_dfs_as_csv_to_folders(tables_data_list=data.to_csv, obj=obj, temp_dir=temp_dir)
        #     else:
        #         raise ValueError('temp_dir was not provided to create the csv files')
        
        # if len(data.to_db) > 0:
        #     if obj.sqlalchemy_con:
        #         self.add_test_dfs_to_db(tables_data_list=data.to_db, con=obj.sqlalchemy_con)
        #     else:
        #         raise ValueError('sqlalchemy connection required to insert the dataframes in to the database')
    
    
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
        return {'result': outcome, 'msg': msg}
        

    # def assert_df_values(self, directory=None, table_name=None, return_df=False, df=[], fields='__all__', expected=[]):
    #     """ 
    #     return True if the list of values of given fields are equal to the expected values. Incase of an error, the two dfs
    #         are returned so the user can see where the difference lies on failure
    #     directory: directory holding the csv file. not required if df is supplied.
    #     table_name: name of the csv file without the .csv extension. not required if df is supplied.
    #     return_df: if True then return the df created from the csv file. Useful so the df doesn't need to be re-created 
    #         for subsequent tests.
    #     df: dataframe. default is a empty list for length can be tested to determine if if exists or not.
    #     fields: allows single or multiple fields
    #         single field: 'field_1'
    #         multiple fields: ['field_1', 'field_2']
    #     expected: list of values expected to be returned e.g. 
    #         single field: ['value 1', 'value 2'] 
    #         multiple fields: [['row1_col1_val', row1_col2_val], ['row2_col1_val', row2_col2_val]]
            
    #     a file which should not be created can be tested by passing in expected=None
    #     """
    #     if len(df) == 0:
    #         try:
    #             df = self.get_csv_file_as_df(directory=directory, table_name=table_name)
    #         except FileNotFoundError:
    #             if expected == None:
    #                 return True, ''
    #             raise
    #     if fields == '__all__':
    #         result = df.values.tolist()
    #     else:
    #         result = df[fields].values.tolist()
    #     return self.return_assert_data(result, expected, df, return_df)
    
        

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
        directories = data['directories']
        tables = data['tables']
        for table in tables:
            output_data = tables[table]['outputs']
            columns = tables[table]['columns']
            for output_table in output_data:
                if output_table == 'database':
                    outcome = self.compare_test_data_from_db(
                        table_name=table, 
                        columns=columns,
                        expected=output_data[output_table],
                        obj=obj.sqlalchemy_con,
                        key=output_table,
                        )
                    
                else:
                    outcome = self.compare_test_data_from_file(
                        directory=directories[output_table],
                        table_name=table,
                        expected=output_data[output_table],
                        obj=obj,
                        key=output_table,
                    )
                assertions.append(outcome)
                    
        return assertions
    
    
    def compare_test_data_from_db(self, table_name, columns, expected, sqlalchemy_con, key):
        """ compare the ouput database table data with the expected results """
        table_name = "gp_{0}".format(table_name.lower())
        command = f"SELECT * FROM {table_name}"
        df = pd.read_sql(command, sqlalchemy_con)
        # drop id column as it is auto-managed elsewhere
        if 'id' in df.columns:
            df.drop(columns=['id'],inplace=True)
        result = df.values.tolist()
        return self.return_assert_data(result, expected, table_name, key)
            
    def compare_test_data_from_file(self, directory, table_name, expected, obj, key):
        """ compare the output csv table data with the expected results """
        path = os.path.join(getattr(obj, directory['store']), f"{table_name}.csv")
        try:
            df = pd.read_csv(path, engine='python')
        except FileNotFoundError:
            if expected == None:
                return True, ''
            raise
        result = df.values.tolist()
        return self.return_assert_data(result, expected, table_name, key)
        
    
    
        
        # if len(df) == 0:
        #     try:
        #         df = self.get_csv_file_as_df(directory=directory, table_name=table_name)
        #     except FileNotFoundError:
        #         if expected == None:
        #             return True, ''
        #         raise
        # if fields == '__all__':
        #     result = df.values.tolist()
        # else:
        #     result = df[fields].values.tolist()
        # return self.return_assert_data(result, expected, df, return_df)
        
        
        
    def make_3_row_df_from_file(self, file_name, directory=SetUp.output_dir, sub_dir='core', rows=3, fields=[]):
        """ 
        returns pd.DataFrame() string that is ready to be pasted in to the _data file. Fields can
        the be edited. e.g.
        return pd.DataFrame(
                columns=['field_1', 'field_2', 'field_3'],
                data=[
                    ['field_1_row_1', 'field_2_row_1', 'field_3_row_1'],
                    ['field_1_row_2', 'field_2_row_2', 'field_3_row_2'],
                    ['field_1_row_3', 'field_2_row_3', 'field_3_row_3'],
                ]
        )
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