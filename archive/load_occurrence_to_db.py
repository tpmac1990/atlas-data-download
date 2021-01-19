import os
import urllib.request
import zipfile
import shutil
import processing
import json
import time

class Functions():

    def __init__(self):
        root_directory = 'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
        self.shp_directory = root_directory + 'shapefiles/shapefiles/wkts/data/occurrence/'

        self.db_configs = {
            "dbname": "shapefiles",
            "user": "postgres",
            "password": "tpm22sra2156!",
            "host": "localhost"
        }


    def pushToDb(self):
        occurrence_shp = self.shp_directory + 'occurrence_final.shp'

        PARAMETERS = {
                    'DATABASE':'shapefiles',
                    'INPUT': occurrence_shp,
                    'SHAPE_ENCODING':'utf-8',
                    'GTYPE':3,
                    'A_SRS':QgsCoordinateReferenceSystem('EPSG:4202'),
                    'T_SRS':None,
                    'S_SRS':None,
                    'SCHEMA':'public',
                    'TABLE':'wkts_occurrence_test',
                    'PK':'_id',
                    'PRIMARY_KEY':'OCCID',
                    'GEOCOLUMN':'pnt',
                    'DIM':0,
                    'SIMPLIFY':'',
                    'SEGMENTIZE':'',
                    'SPAT':None,
                    'CLIP':False,
                    'WHERE':'',
                    'GT':'',
                    'OVERWRITE':True,
                    'APPEND':False,
                    'ADDFIELDS':False,
                    'LAUNDER':False,
                    'INDEX':True,
                    'SKIPFAILURES':False,
                    'PROMOTETOMULTI':True,
                    'PRECISION':True,
                    'OPTIONS':''
                    }

                    
        processing.run("gdal:importvectorintopostgisdatabaseavailableconnections", PARAMETERS)


F = Functions()
F.pushToDb()

