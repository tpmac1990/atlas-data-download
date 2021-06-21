



import os
import sys
import csv
import ctypes
import pandas as pd
import shutil
import datetime


def moveOldRenameNewFiles():
    directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/Occurrences/'

    tDate = datetime.datetime.now()
    tDate = tDate.strftime("%y%m%d")
    newDir = "archive/%s/" %(tDate)
    if not os.path.exists(newDir):
        os.mkdir(newDir)

    # Move old files to archive 'Be aware that they are stored in the current weeks archive'
    for file in os.listdir(directory):
        fName = os.fsdecode(file)
        if fName.endswith("OLD.csv"): 
            destination = "%s%s" %(newDir,fName)
            new_path = shutil.move(fName, destination)

    # Change the name of the previous weeks files from NEW to OLD. 
    for file in os.listdir(directory):
        fName = os.fsdecode(file)
        if fName.endswith("NEW.csv"): 
            if "WKT" in fName:
                newfName = fName.replace("NEW","OLD")
                os.rename(fName,newfName)
            else:
                os.remove(fName)

    # Rename the newly created WKT files with a NEW suffix
    for file in os.listdir(directory):
        fName = os.fsdecode(file)
        if fName.endswith("WKT.csv"): 
            newfName = fName.split(".")[0]
            newfName = "%s_NEW.csv" %(newfName)
            os.rename(fName,newfName)   

    print("Necessary files have been renamed and/or moved!")


# Add a unique ID for each tenement
def addIdentifierField():
    csv.field_size_limit(int(ctypes.c_ulong(-1).value//2))

    directory = r'C:/Django_Projects/03_geodjango/Atlas/datasets/Raw_datasets/Occurrences/'

    tDate = datetime.datetime.now()
    tDate = tDate.strftime("%y%m%d")
    newDir = "archive/%s/" %(tDate)
    if not os.path.exists(newDir):
        os.mkdir(newDir)

    i = 1
    id_template = "000000"

    for file in os.listdir(directory):
        fName = os.fsdecode(file)
        if fName.endswith("_WKT_NEW.csv"):
            print("Adding NEW_ID fields for %s" %(fName)) 

            fName =fName.split(".")[0]
            cSTATE = fName.split("_")[0]
            if cSTATE == "AUS":
                cSTATE = "OS"
            source = '%s.csv' %(fName)
            destination = "%s%s_BeforeID_%s.csv" %(newDir,fName,tDate)
            new_path = shutil.move(source, destination)

            with open(destination,'r', encoding="utf-8") as fInput, open("%s.csv" %(fName), 'w', encoding="utf-8") as fOutput:
                writer = csv.writer(fOutput, lineterminator='\n')
                reader = csv.reader(fInput)

                all = []
                row = next(reader)
                row.append("NEW_ID")
                all.append(row)
                for row in reader:
                    newID = "%s%s%s" %(cSTATE,id_template[:len(id_template)-len(str(i))],str(i))
                    row.append(newID)
                    all.append(row)
                    i +=1

                writer.writerows(all)
                print("%s completed successfully" %(fName)) 

    print("NEW_ID fields have been added successfully for all files!")



moveOldRenameNewFiles()
addIdentifierField()
