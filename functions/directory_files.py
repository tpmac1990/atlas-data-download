import os
import shutil
import csv
import json
import pandas as pd


def copy_directory(src_directory,dest_directory):
    ''' copy all files from one directory to another '''
    for file in os.listdir(src_directory):
        shutil.copy(os.path.join(src_directory,file), dest_directory)


def copy_directory_in_list(file_lst,src_directory,dest_directory):
    ''' copy all files from one directory to another if it exists in the file_lst '''
    for file in os.listdir(src_directory):
        if file in file_lst:
            shutil.copy(os.path.join(src_directory,file), dest_directory)
        

def adjustDirectoryFilenames(**kwargs):
    from_val = kwargs['from_val']
    to_val = kwargs['to_val']
    src = kwargs['src']
    for file in os.listdir(src):  
        old_file = os.path.join(src,file)
        new_file = os.path.join(src,file).replace(from_val,to_val)
        os.rename(old_file, new_file)


def clearDirectory(directory,**kwargs):
    if 'extension' in kwargs:
        extension = kwargs['extension']
    else:
        extension = ""
    for file in os.listdir(directory):
        if file.endswith(extension):
            os.remove(os.path.join(directory,file))


def createDirectory(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)


def createMultipleDirectories(directory,lst):
    createDirectory(directory)
    for name in lst:
        createDirectory(os.path.join(directory,name))


def writeToFile(path, lst):
    with open(path,'w') as t1:
        writer = csv.writer(t1, lineterminator='\n')
        writer.writerows(lst)


def fileExist(path):
    return os.path.isfile(path)


def delete_files_in_directory(directory):
    for file in os.listdir(directory):
        os.remove(os.path.join(directory,file))


def getJSON(path):
    with open(path) as json_file:
        return json.load(json_file)

def writeJSON(path,obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


def readMultipledf(file_lst):
    return [pd.read_csv(file,engine='python') for file in file_lst]


def createDirectory(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)


def archive_and_clear_directories(archive_lst:str, delete_lst:list, p_src_dir:str, p_dest_dir:str) -> (None):
    ''' copy and compress folders from the archive_lst to the archive folder and then delete all the folders provided in the delete_lst '''
    # create a complete list of all the folders to complete an action on
    action_lst = archive_lst + [x for x in delete_lst if not x in archive_lst]
    # loop throuh all folders
    for name in action_lst:
        src_dir = os.path.join(p_src_dir,name)
        dest_dir = os.path.join(p_dest_dir,name)
        # archive folder if in the archive_lst
        if name in archive_lst:
            copy_directory(src_dir,dest_dir)
        # delete folder if in the delete_lst
        if name in delete_lst:
            clearDirectory(src_dir,extension=".csv")

    



