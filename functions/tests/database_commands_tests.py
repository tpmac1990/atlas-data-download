import os
import pandas as pd
import sqlalchemy
import pytest

from functions.common.database_commands import drop_and_restore_db, pg_dump, drop_db, create_db, restore_db_sql, close_db_connections
from ..setup import SetUp
from functions.common.constants import *

# pytest -s functions/tests/database_commands_tests.py

class DatabaseCommandTests:
    db_dumps_dir = os.path.join(SetUp.scripts_dir, 'functions', 'tests', 'db_dumps')
    
    def create_restore_dump_database_commands_test(self, tmpdir):
        """runs drop_db, create_db, restore_db_sql, close_db_connections"""
        sub_dir = tmpdir.mkdir("sub")
        dump_name = 'basic_dump.sql'
        drop_and_restore_db(
            filename=dump_name,
            directory=self.db_dumps_dir,
            dbname=TEST_DB, 
        )
        configs = f'postgresql://{POSTGRES}:{PASSWORD}@{LOCALHOST}/{TEST_DB}'
        engine_2 =  sqlalchemy.create_engine(configs)
        sql = "SELECT * FROM table_1"
        df = pd.read_sql(sql, engine_2)
        
        assert list(df.columns) == ['column_1', 'column_2']
        assert df.values.tolist() == [['a', 'b'], ['y', 'z']]
        
        pg_dump(dump_name,
            directory=sub_dir,
            dbname=TEST_DB
            )
        
        assert os.path.exists(os.path.join(sub_dir, dump_name)) == True
        
        drop_db(dbname=TEST_DB)
        
        engine_2 = sqlalchemy.create_engine(configs)
        with pytest.raises(sqlalchemy.exc.OperationalError):
            engine_2.connect()
        

 