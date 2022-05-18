import pytest
import sqlalchemy
from functions.common.constants import *
from functions.common.database_commands import drop_and_restore_db, pg_dump, drop_db
from .common import DB_DUMPS_DIR, DB_CONFIGS
from functions.common.db_functions import drop_all_db_tables


@pytest.fixture(autouse=True, scope=MODULE)
def create_db():
    """ creates a postgis database with only the essential gis tables """
    drop_and_restore_db(
        filename='blank_dump.sql',
        directory=DB_DUMPS_DIR,
        dbname=TEST_DB
    )
    

@pytest.fixture(autouse=True, scope=FUNCTION)
def sqlalchemy_engine():
    return sqlalchemy.create_engine(f'postgresql://{POSTGRES}:{PASSWORD}@{LOCALHOST}/{TEST_DB}').connect()


@pytest.fixture(autouse=True, scope=FUNCTION)
def clean_db(sqlalchemy_engine):
    """ drop all created tables at the end of the test """
    yield
    sqlalchemy_engine.close()
    drop_all_db_tables(DB_CONFIGS, _exclude=[GEOGRAPHY_COLUMNS, GEOMETRY_COLUMNS, SPATIAL_REF_SYS])