# import os
# import csv
# import sys
# # import ctypes
# # import pandas as pd
# from datetime import datetime, date

# def createChangeDict(self):
#     print('Creating Dictionary with gplore if and WKT.')
#     change_dic = {}
#     for file in os.listdir(self.change_dir):
#         print('Working on: %s' %(file))
#         # if file == 'NSW_1_WKT.csv':
#         if file.endswith("_WKT.csv"):
#             with open(os.path.join(self.change_dir,file), 'r') as t1:
#                 change_reader = csv.reader(t1)
#                 header = next(change_reader)
#                 if 'geometry' in header:
#                     geom_index = header.index('geometry')
#                     for line in change_reader:
#                         # print(line[-1])
#                         change_dic[line[-1]] = line[geom_index]    
#     return change_dic


# def insert_wkt(self,change_dic):
#     ''' This method loops through all of the Tenement rows and re-assigns the polygon spatial data. This was dropped earlier so the data was more manageable in excel '''
#     print('Starting to insert WKT.')
#     tenement_path = os.path.join(self.output_dir,'new','Tenement_nowkt.csv')

#     lst = []
#     polyEmpty = 0
#     notFound = 0
#     added = 0
#     with open(tenement_path, 'r') as t1:
#         reader = csv.reader(t1)
#         row = next(reader)
#         row.insert(0,"geom")
#         lst.append(row)

#         for line in reader:
#             try:
#                 wkt = change_dic[str(int(float(line[0])))]
#                 if wkt == "MULTIPOLYGON EMPTY":
#                     polyEmpty += 1
#                 line.insert(0,wkt)
#                 lst.append(line)
#                 added += 1
#             except:
#                 notFound += 1

#     print('Added: %s, Not Found: %s, Empty Multipolygon: %s' %(added,notFound,polyEmpty))
#     return lst