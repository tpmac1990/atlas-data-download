import os
import sys
import csv
import ctypes
import pandas as pd
from datetime import datetime, date

from functions import *


class Functions():

    def __init__(self):
        csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))
        root_directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/'
        self.output_directory = root_directory + 'output/'
        self.change_directory = root_directory + 'tenement/change/'


    def createChangeDict(self):
        print('Creating Dictionary with gplore if and WKT.')
        change_dic = {}
        for file in os.listdir(self.change_directory):
            print('Working on: %s' %(file))
            if file.endswith("_WKT.csv"):
                with open(self.change_directory + file, 'r') as t1:
                    change_reader = csv.reader(t1)
                    next(change_reader)
                    for line in change_reader:
                        change_dic[line[-1]] = line[0]    
            print('Complete.') 
        return change_dic


    def insertWkt(self,change_dic):
        print('Starting to insert WKT.')
        tenement_path = self.output_directory + "new/Tenement.csv"

        all = []
        polyEmpty = 0
        notFound = 0
        added = 0
        with open(tenement_path, 'r') as t1:
            reader = csv.reader(t1)
            row = next(reader)
            row.insert(0,"WKT")
            all.append(row)

            for line in reader:
                try:
                    wkt = change_dic[line[0]]
                    if wkt == "MULTIPOLYGON EMPTY":
                        polyEmpty += 1
                    line.insert(0,wkt)
                    for i in [6,7,8]:
                        line[i] = datetime.strptime(line[i], '%d/%m/%Y').date()
                    all.append(line)
                    added += 1
                except:
                    notFound += 1

        print('Added: %s, Not Found: %s, Empty Multipolygon: %s' %(added,notFound,polyEmpty))
        return all



    # Loop through the WKT tenement files and add the WKT to each tenement by its creates ID
    def addWKTToTenementFile(self):

        wkt_tenement_path = self.output_directory + "new/Tenement_wkt.csv"

        change_dic = self.createChangeDict()

        tenement_lst_wkt = self.insertWkt(change_dic)
        
        writeToFile(wkt_tenement_path, tenement_lst_wkt)

        print('Complete.')


Functions().addWKTToTenementFile()

