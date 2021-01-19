import os
import csv
import sys
# import ctypes
# import pandas as pd
from datetime import datetime, date

def createChangeDict(self):
    print('Creating Dictionary with gplore if and WKT.')
    change_dic = {}
    for file in os.listdir(self.change_dir):
        print('Working on: %s' %(file))
        # if file == 'TAS_1_WKT.csv':
        if file.endswith("_WKT.csv"):
            with open(os.path.join(self.change_dir,file), 'r') as t1:
                change_reader = csv.reader(t1)
                next(change_reader)
                for line in change_reader:
                    # print(len(line[-1]))
                    # print("%s - "%(line[-1]))
                    # if line[0].find("MULTI") == -1:
                    #     print("%s - %s" %(file,line[0]))
                    change_dic[line[-1]] = line[0]    
        print('Complete.')
    return change_dic


def insertWkt(self,change_dic):
    print('Starting to insert WKT.')
    tenement_path = os.path.join(self.output_dir,'new','Tenement_nowkt.csv')

    lst = []
    polyEmpty = 0
    notFound = 0
    added = 0
    with open(tenement_path, 'r') as t1:
        reader = csv.reader(t1)
        row = next(reader)
        row.insert(0,"WKT")
        lst.append(row)

        for line in reader:
            try:
                wkt = change_dic[line[0]]
                if wkt == "MULTIPOLYGON EMPTY":
                    polyEmpty += 1
                line.insert(0,wkt)
                for i in [6,7,8]:
                    line[i] = datetime.strptime(line[i], '%d/%m/%Y').date()
                lst.append(line)
                added += 1
            except:
                # print to get GPLORE id
                # print(line[1])
                notFound += 1

    print('Added: %s, Not Found: %s, Empty Multipolygon: %s' %(added,notFound,polyEmpty))
    return lst