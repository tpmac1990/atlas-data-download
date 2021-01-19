import os
import shutil
import csv
import json
import urllib.request
import zipfile
import pandas as pd

# copy files from one directory to another
def copyDirectory(src_directory,dest_directory):
    for file in os.listdir(src_directory):
        shutil.copy(src_directory + file, dest_directory)


def adjustDirectoryFilenames(**kwargs):
    from_val = kwargs['from_val']
    to_val = kwargs['to_val']
    src = kwargs['src']
    for file in os.listdir(src):  
        old_file = src + file
        new_file = src + file.replace(from_val,to_val)
        os.rename(old_file, new_file)


def clearDirectory(directory,**kwargs):
    if 'extension' in kwargs:
        extension = kwargs['extension']
    else:
        extension = ""
    for file in os.listdir(directory):
        if file.endswith(extension):
            os.remove(directory + file)


def createDirectory(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)


def createMultipleDirectories(directory,lst):
    createDirectory(directory)
    for name in lst:
        createDirectory("%s%s/"%(directory,name))


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


def writeToFile(path, lst):
    with open(path,'w') as t1:
        writer = csv.writer(t1, lineterminator='\n')
        writer.writerows(lst)


def fileExist(path):
    return os.path.isfile(file_path)


# fName = os.fsdecode(file)
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


def getJSON(path):
    with open(path) as json_file:
        return json.load(json_file)


def unzipFile(zip_file_path,output_directory):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
      zip_ref.extractall(output_directory)


def delete_files_in_directory(directory):
    for file in os.listdir(directory):
        os.remove(directory + file)


def time_past(start,end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)


def download_unzip_and_remove(link,zip_file_path,unzipped_directory):
    download_and_unzip(link,zip_file_path,unzipped_directory)
    os.remove(zip_file_path)


def getTempLink(data_import_group,temp_links):
    link = data_import_group['link']
    if link in list(temp_links.keys()):
        link = temp_links[link]
    return link


def download_file(link,path_output):
    urllib.request.urlretrieve(link, path_output)


def download_and_unzip(link,file_path,output_directory):
    download_file(link,file_path)
    unzipFile(file_path,output_directory)


def download_unzip_link_manual(data_import_group,temp_links,zip_file_path,manual_directory,unzipped_directory):
    if data_import_group['data_source'] != 'WFS':
        print('Downloading and unzipping ' + data_import_group['name'])
        if data_import_group['import_style'] == 'link':
            link = getTempLink(data_import_group,temp_links)
            download_unzip_and_remove(link,zip_file_path,unzipped_directory)
        else:
            unzipFile(manual_directory + data_import_group['link'] + '.zip',unzipped_directory)
        print('Complete.')


def removeDuplicateRowsByKey(file_path,col_num):
    print('Removing duplicates by key.')
    with open(file_path, 'r') as t1:
        reader = csv.reader(t1)
        lst = []
        unique_keys = []
        headers = next(reader)
        lst.append(headers)
        for line in reader:
            key = line[col_num]
            if key not in unique_keys:
                unique_keys.append(key)
                lst.append(line)
    writeToFile(file_path, lst)
    print('Complete.')


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


def readMultipledf(file_lst):
    return [pd.read_csv(file,engine='python') for file in file_lst]
    # lst = []
    # for file in file_lst:
    #     df = pd.read_csv(file)
    #     lst.append(df)
    # return lst

def dropMultipleColumnsdf(df_lst,drop_index_lst):
    if drop_index_lst != "":
        df_lst = [df.drop(drop_index_lst, axis=1) for df in df_lst]
        # lst = []
        # for df in df_lst:
        #     df1 = df.drop(drop_index_lst, axis=1)
        #     lst.append(df1)
        # df_lst = lst
    return df_lst

