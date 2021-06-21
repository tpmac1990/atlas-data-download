import os
import shutil
import csv
import json
import urllib.request
import zipfile
import pandas as pd

# copy files from one directory to another
def copy_directory(src_directory,dest_directory):
    ''' copy all files from one directory to another '''
    # print(src_directory)
    # print(dest_directory)
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


def unzipFile(zip_file_path,output_directory):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_directory)

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


def download_unzip_link_manual(self,data_import_group):
    success = True
    if data_import_group['data_source'] != 'WFS':
        print('Downloading and unzipping ' + data_import_group['name'])
        try:
            if data_import_group['import_style'] == 'link':
                link = getTempLink(data_import_group,self.temp_links)
                download_unzip_and_remove(link,self.zip_file_path,os.path.join(self.unzipped_dir,data_import_group['created_extension']))
            else:
                unzipFile(os.path.join(self.manual_dir,data_import_group['link'] + '.zip'),self.unzipped_dir)
        except:
            # If the link fails then add the name, data_source and link to the download_fail.csv. This will be used later to determine whether to format the data or not.
            success = False
            df = pd.DataFrame({'NAME': [data_import_group['name']],'DATA_SOURCE': [data_import_group['data_source']],'LINK': [data_import_group['link']]})
            if fileExist(self.download_fail_path):
                existing_df = pd.read_csv(self.download_fail_path)
                df.concat((existing_df,df))
            df.to_csv(self.download_fail_path,index=False)
            print("Download was unsuccessful. Check the link.")
    return success


def getJSON(path):
    with open(path) as json_file:
        return json.load(json_file)


def readMultipledf(file_lst):
    return [pd.read_csv(file,engine='python') for file in file_lst]
