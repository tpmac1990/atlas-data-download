import csv
import pandas as pd
from .directory_files import writeToFile


def convertCsvToDic(reader,col):
    next(reader)
    row = next(reader)
    identifier = row[row.rfind(',', 0, row.rfind(',', 0, len(row)))+1:row.rfind(',', 0, len(row))]


def  dropLastColumnReadlines(file): 
    all=[]
    for r in file:
        all.append(r.replace(",%s" %(r.split(',')[-1]),'')+'\n')
    return all

    
def  dropLastColumn(reader): 
    all=[]
    for r in reader:
        all.append(r[:-1])
    return all


def convertToDic(reader_lst,key_position):
    next(reader_lst)
    dic = {}
    for line in reader_lst:
        if key_position == 'last':
            line = line[:-1]
            key = line[-1]
        elif isinstance(key_position, int):
            key = line[key_position]
        else:
            print('Incorrect key position!')
        dic[key] = line

    return dic


def time_past(start,end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)



def createUniqueKeyField(file_path,unique_dic):
    print('Creating Unique index field.')
    with open(file_path, 'r') as t1:
        reader = csv.reader(t1)
        lst = []
        unique_keys = []
        headers = next(reader)
        headers.append('UNIQUE_FIELD')
        lst.append(headers)
        for line in reader:
            key = line[col_num]
            if key not in unique_keys:
                unique_keys.append(key)
                lst.append(line)
    writeToFile(file_path, lst)
    print('Complete.')


def dropMultipleColumnsdf(df_lst,drop_index_lst):
    if drop_index_lst != "":
        df_lst = [df.drop(drop_index_lst, axis=1) for df in df_lst]

    return df_lst