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
        


def clear_directory(directory,**kwargs):
    if 'extension' in kwargs:
        extension = kwargs['extension']
    else:
        extension = ""
    for file in os.listdir(directory):
        if file.endswith(extension):
            os.remove(os.path.join(directory,file))


def clear_all_directory(directory):
    ''' clears all files and sub-directories within the provided directory '''
    shutil.rmtree(directory)
    create_directory(directory)


def create_directory(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)


def create_directory_tree(directory):
    ''' creates non-existent directories for a provided directory tree '''
    directory = directory.replace('\\','/')
    split = directory.split('/')
    url = split[0]
    for x in split[1:]:
        url += '/' + x
        create_directory(url)


def create_multiple_directories(directory,lst):
    create_directory(directory)
    for name in lst:
        create_directory(os.path.join(directory,name))


def write_to_file(path, lst):
    with open(path,'w') as t1:
        writer = csv.writer(t1, lineterminator='\n')
        writer.writerows(lst)


def file_exist(path):
    return os.path.isfile(path)


def delete_file(path):
    if file_exist(path):
        os.remove(path)


def delete_files_in_directory(directory):
    for file in os.listdir(directory):
        os.remove(os.path.join(directory,file))


def get_json(path):
    with open(path) as json_file:
        return json.load(json_file)


def write_json(path,obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


# def archive_and_clear_directories(archive_lst:str, delete_lst:list, p_src_dir:str, p_dest_dir:str) -> (None):
#     ''' copy and compress folders from the archive_lst to the archive folder and then delete all the folders provided in the delete_lst '''
#     # create a complete list of all the folders to complete an action on
#     action_lst = archive_lst + [x for x in delete_lst if not x in archive_lst]
#     # loop throuh all folders
#     for name in action_lst:
#         src_dir = os.path.join(p_src_dir,name)
#         dest_dir = os.path.join(p_dest_dir,name)
#         # archive folder if in the archive_lst
#         if name in archive_lst:
#             # create directory if it doesn't exist
#             create_directory_tree(dest_dir)
#             # copy files from source directory to destination directory
#             copy_directory(src_dir,dest_dir)
#         # delete folder if in the delete_lst
#         if name in delete_lst:
#             clear_directory(src_dir,extension=".csv")

    