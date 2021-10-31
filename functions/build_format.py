import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, date, timedelta
import time
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from .directory_files import fileExist, getJSON
from .timer import time_past



def create_combined_datasets(self):
    ''' combine all the separate file into single dataset and record the missing values in the missing_all and missing_reduced files used to update values later '''
    # this is where the missing data will be stored along with its 'ind' value so it can be updated later
    self.missing_df = pd.DataFrame([],columns=['STATE','GROUP','FIELD','IND','VALUE'])

    # combine all title data in to one dataset
    combine_title_data(self)
    # combine all site data in to one dataset
    combine_site_data(self)
    # save the missing data to files
    finalize_missing_data(self)



def combine_title_data(self):
    ''' compile all the separate site files into one dataset '''
    func_start = time.time()
    print('Combine all title data into complete files')

    # convert_dir = os.path.join(self.output_dir,'convert')
    convert_dir = self.convert_dir
    core_dir = os.path.join(self.output_dir,'core')
    new_dir = os.path.join(self.output_dir,'new')
    self.update_dir = os.path.join(self.output_dir,'update')

    related_ids_core_path = os.path.join(core_dir,"TenOriginalID.csv")

    companies_orig_df = pd.read_csv(os.path.join(convert_dir,"Companies_R.csv"),engine='python')
    status_orig_df = pd.read_csv(os.path.join(convert_dir,"TenStatus_R.csv"),engine='python')
    type_orig_df = pd.read_csv(os.path.join(convert_dir,"TenType_R.csv"),engine='python')

    TenType_df = pd.read_csv(os.path.join(core_dir,"TenType.csv"),engine='python')
    TenStatus_df = pd.read_csv(os.path.join(core_dir,"TenStatus.csv"),engine='python')
    self.Holder_df = pd.read_csv(os.path.join(core_dir,"Holder.csv"),engine='python')
    Shore_df = pd.read_csv(os.path.join(core_dir,"Shore.csv"),engine='python')

    self.status_df = status_orig_df.merge(TenStatus_df,left_on='GP_VALUE',right_on='original',how='left').drop(columns=['original','simple_id'],axis=1)
    self.type_df = type_orig_df.merge(TenType_df,left_on='GP_VALUE',right_on='original',how='left').drop(columns=['original','act_id','simple_id','ACT'],axis=1)
    self.companies_df = companies_orig_df.merge(self.Holder_df,left_on='GP_VALUE',right_on='name',how='left').drop(columns=['name','user_name','valid_relations','valid_instance','user_edit','date_modified','date_created'],axis=1)
    
    self.status_uk = TenStatus_df.query('original == "Unknown"').iloc[0]['_id']
    self.type_uk = TenType_df.query('original == "Unknown"').iloc[0]['_id']
    self.holder_uk = self.Holder_df.query('name == "Unknown"').iloc[0]['_id']
    self.shore_uk = Shore_df.query('name == "Unknown"').iloc[0]['_id']

    self.Tenement_nowkt_df = pd.DataFrame()
    self.tenement_oid_df = pd.DataFrame()
    self.TenHolder_df = pd.DataFrame()

    # save self.Holder_df with suffix '_pre' so it can be referred to later when finding near matches for missing holders. The missing holders will be temporarily added to self.Holder_df, so it can not be used.
    self.Holder_df.to_csv(os.path.join(core_dir,"Holder_pre.csv"),index=False)

    # for data_group in self.data_groups:
    self.configs = getJSON(os.path.join(self.configs_dir,'formatting_config.json'))['tenement']

    for file in self.configs:
        # configs to format and combine the data 
        configs = self.configs[file]['build']
        # directory with the new files
        self.change_dir = os.path.join(self.input_dir,'tenement','change')
        self.plain_dir = os.path.join(self.input_dir,'tenement','plain')
        self.state = file[:-2]

        # read the csv file to pandas df
        if configs:
            print('tenement: ' + file)
            format_file_to_combine(self,file,configs)

    self.tenement_oid_df.drop_duplicates(inplace=True)
    self.Tenement_nowkt_df.drop_duplicates(inplace=True)
    self.TenHolder_df['_id'] = np.arange(1, len(self.TenHolder_df) + 1)

    add_new_related_ids_to_core(self,self.tenement_oid_df,related_ids_core_path)

    # self.Tenement_nowkt_df.to_csv(os.path.join(new_dir,'Tenement_nowkt.csv'),index=False)
    self.Tenement_nowkt_df.to_csv(os.path.join(new_dir,'Tenement.csv'),index=False)
    self.tenement_oid_df.to_csv(os.path.join(new_dir,'tenement_oid.csv'),index=False)
    self.TenHolder_df.to_csv(os.path.join(new_dir,'TenHolder.csv'),index=False)
    # Holder_df will have the missing holder names added to it, so it needs to be saved
    self.Holder_df.to_csv(os.path.join(core_dir,"Holder.csv"),index=False) 

    print('Title data compilation: %s' %(time_past(func_start,time.time())))




def combine_site_data(self):
    ''' compile all the separate title files into one dataset '''
    func_start = time.time()
    print('Combine all site data into complete files')

    convert_dir = self.convert_dir
    core_dir = os.path.join(self.output_dir,'core')
    new_dir = os.path.join(self.output_dir,'new')

    related_ids_core_path = os.path.join(core_dir,"OccOriginalID.csv")

    # directory with the new files
    self.change_dir = os.path.join(self.input_dir,'occurrence','change')
    self.plain_dir = os.path.join(self.input_dir,'occurrence','plain')
    self.update_dir = os.path.join(self.output_dir,'update')

    status_orig_df = pd.read_csv(os.path.join(convert_dir,"OccStatus_R.csv"),engine='python')
    type_orig_df = pd.read_csv(os.path.join(convert_dir,"OccType_R.csv"),engine='python')
    size_orig_df = pd.read_csv(os.path.join(convert_dir,"OccSize_R.csv"),engine='python')
    material_orig_df = pd.read_csv(os.path.join(convert_dir,"Materials_R.csv"),engine='python')

    OccStatus_df = pd.read_csv(os.path.join(core_dir,"OccStatus.csv"),engine='python')
    OccType_df = pd.read_csv(os.path.join(core_dir,"OccType.csv"),engine='python')
    OccSize_df = pd.read_csv(os.path.join(core_dir,"OccSize.csv"),engine='python')
    Material_df = pd.read_csv(os.path.join(core_dir,"Material.csv"),engine='python')

    self.status_df = status_orig_df.merge(OccStatus_df,left_on='GP_VALUE',right_on='original',how='left').drop(columns=['GP_VALUE','original','simple_id'],axis=1)
    self.type_df = type_orig_df.merge(OccType_df,left_on='GP_VALUE',right_on='original',how='left').drop(columns=['GP_VALUE','original','simple_id'],axis=1)
    self.size_df = size_orig_df.merge(OccSize_df,left_on='GP_VALUE',right_on='name',how='left').drop(columns=['GP_VALUE','name'],axis=1)
    self.material_df = material_orig_df.merge(Material_df,left_on='GP_VALUE',right_on='name',how='left')     
    self.related_mat_df = pd.read_csv(os.path.join(convert_dir,"Materials_Related_R.csv"),engine='python')
    self.name_lookup_df = pd.read_csv(os.path.join(convert_dir,"OccName_R.csv"),engine='python')
    self.name_id_df = pd.read_csv(os.path.join(core_dir,"OccName.csv"),engine='python')

    self.status_uk = OccStatus_df.query('original == "Unknown"').iloc[0]['_id']
    self.size_uk = OccSize_df.query('name == "Unspecified"').iloc[0]['_id']
    self.type_uk = OccType_df.query('original == "Unspecified"').iloc[0]['_id']
    self.material_uk = Material_df.query('name == "Unknown"').iloc[0]['_id']

    self.Occurrence_df = pd.DataFrame()
    self.occurrence_oid_df = pd.DataFrame()
    self.occurrence_name_df = pd.DataFrame()
    self.occurrence_typ_df = pd.DataFrame()
    self.occurrence_majmat_df = pd.DataFrame()
    self.occurrence_minmat_df = pd.DataFrame()

    self.configs = getJSON(os.path.join(self.configs_dir,'formatting_config.json'))['occurrence']

    for file in self.configs:
        # configs to format and combine the data 
        configs = self.configs[file]['build']
        self.state = file[:-2]

        # read the csv file to pandas df
        if configs:
            print('Occurrence: ' + file)
            format_site_file_to_combine(self,file,configs)
    
    self.occurrence_oid_df.drop_duplicates(inplace=True)
    self.occurrence_name_df.drop_duplicates(inplace=True)
    self.occurrence_majmat_df.drop_duplicates(inplace=True)
    self.occurrence_minmat_df.drop_duplicates(inplace=True)
    self.occurrence_typ_df.drop_duplicates(inplace=True)

    add_new_related_ids_to_core(self,self.occurrence_oid_df,related_ids_core_path)

    self.Occurrence_df.to_csv(os.path.join(new_dir,'Occurrence_pre.csv'),index=False)
    self.occurrence_oid_df.to_csv(os.path.join(new_dir,'occurrence_oid.csv'),index=False)
    self.occurrence_name_df.to_csv(os.path.join(new_dir,'occurrence_name.csv'),index=False)
    self.occurrence_typ_df.to_csv(os.path.join(new_dir,'occurrence_typ.csv'),index=False)
    self.occurrence_majmat_df.to_csv(os.path.join(new_dir,'occurrence_majmat.csv'),index=False)
    self.occurrence_minmat_df.to_csv(os.path.join(new_dir,'occurrence_minmat.csv'),index=False)

    print('Site data compilation: %s' %(time_past(func_start,time.time())))


# def output_missing_data(self):
#     ''' save missing data in files stored in the output/update directory.
#         missing_all: each missing item with its 'ind' value so it can be updated later
#         missing_reduced: unique missing values. if a new company name appears for multiple titles, it will only be shown in this list once.
#         use fuzzywuzzy to find similar names for missing company names
#     '''
#     print('Output missing data files.')
#     finalize_missing_data(self)




# ###########################################################
# pre-formatting functions
def nsw_1_date_format(x,source):
    if x[source] == "Renewal Sought": 
        return "2999-01-01"
    else:
        return datetime.strptime(x[source], '%d %b %Y').strftime('%Y-%m-%d')


def nsw_1_date_status(x,source):
    if x[source[0]] != "Renewal Sought":
        return "Active"
    elif datetime.strptime(x[source[1]],'%Y-%m-%d').date() < date.today():
        return "Expired - Renewal Sought"
    else:
        return "Active - Renewal Sought"


def nsw_2_date_status(x,source):
    if datetime.strptime(x[source],'%Y-%m-%d').date() < (date.today() - timedelta(days=365)):
        return "Application - Pending over 1 year"
    else:
        return "Application - Pending under 1 year"

def QLD_1_status(x,source):
    if x[source[0]] == 'Granted':
        return "%s%s - %s"%(time_since_expiry_desc(x,source[3]),x[source[0]],x[source[1]])
    else:
        return "%s%s - %s"%(time_since_lodge_desc(x,source[2]),x[source[0]],x[source[1]])

def WA_1_status(x,source):
    targ_date = datetime.strptime(x[source],'%Y-%m-%d').date()
    if targ_date == date(2999,12,31):
        return "Application - Pending Unknown"
    elif targ_date < (date.today() - timedelta(days=365)):
        return "Application - Pending over 1 year"
    elif targ_date < date.today():
        return "Application - Pending under 1 year"
    else:
        return "Active"


def WA_1_date(x,source):
    targ_date = datetime.strptime(x[source],'%Y-%m-%d').date()
    if targ_date == date(2999,12,31) or targ_date < date.today():
        return "2999-01-01" 
    else:
        return x[source]


def WA_5_status(x,source):
    if type(x[source]) == float:
        return "Active"
    targ_date = datetime.strptime(x[source],'%Y-%m-%d').date()
    if targ_date < (date.today() - timedelta(days=365)):
        return "Expired over 1 year - "
    elif targ_date < date.today():
        return "Expired under 1 year - "
    else:
        return "Active - "


def WA_8_status(x,source):
    if "AO" in x[source[1]]:
        return "%sSpecial Prospecting Authority with Acreage Option"%(x[source[0]])
    elif "SPA" in x[source[1]]:
        return "%sSpecial Prospecting Authority Application"%(x[source[0]])
    else:
        return "no match"

def WA_6_type(x,source):
    return re.search(r"[A-Z]*-[A-Z]*(?=-)", x[source]).group(0)

def SA_2_holder(x,source):
    ''' SA_2 has missing percentages for some holders. If only one holder is missing a percentage then it will
        find the remainder and add it, but if more it will apply 0 percent as their ownership 
    '''
    split = x[source].split(";")
    count = 0
    perc = 0
    for i in split:
        if not "%" in i:
            count += 1
        else:
            perc += float(re.search(r"(?<=\()[0-9].*(?=%\))", i).group(0))
    if count == 0:
        return x[source]
    else:
        lst = []
        for i in split:
            if not "%" in i:
                if count == 1:
                    lst.append(("%s (%s%%)"%(i.strip(),100 - perc)))
                else:
                    lst.append(("%s (0%)")%(i.strip()))
            else:
                lst.append(i)

        return "; ".join(lst)



def time_since_expiry_desc(x,source):
    if type(x[source]) == float:
        return ""
    elif datetime.strptime(x[source],'%Y-%m-%d').date() < (date.today() - timedelta(days=365)):
        return "Expired over 1 year - "
    elif datetime.strptime(x[source],'%Y-%m-%d').date() < date.today():
        return "Expired under 1 year - "
    else:
        return "Active - "

def time_since_expiry_desc_noexpiry(x,source):
    status = time_since_expiry_desc(x,source)
    if status == "":
        return "No Expiry - "
    else:
        return status


def time_since_lodge_desc(x,source):
    if type(x[source]) == float:
        return ""
    elif datetime.strptime(x[source],'%Y-%m-%d').date() < (date.today() - timedelta(days=365)):
        return "lodged over 1 year - "
    elif datetime.strptime(x[source],'%Y-%m-%d').date() < date.today():
        return "lodged under 1 year - "
    else:
        return "Active - "


def date_time_zero_to_null(x,source):
    if type(x[source]) != float:
        return "%s-%s-%s"%(x[source][:4],x[source][5:7],x[source][8:10])
    else:
        return "2999-01-01"


def combined_long(x,source):
    date = str(x[source])
    return "%s-%s-%s"%(date[:4],date[4:6],date[6:8])


def join_multi_by_semi(x,source):
    return ";".join([x[i] for i in source if not type(x[i]) == float])


def join(x,source):
    return "".join([x[i] for i in source if type(x[i]) != float])


def join_dash(x,source):
    return " - ".join([x[i] for i in source])


def join_with_text(x,source):
    return "%s%s"%(x[source[0]],source[1])


def join_with_dash_dif(x,source):
    return "%s%s - %s"%(x[source[0]],x[source[1]],x[source[2]])


def join_dif_2(x,source):
    return "%s%s%s"%(x[source[0]],x[source[1]],source[2])


def add_text(x,source):
    return source


def NT_2_3_merge_cleanup(x):
    return "%s - ALRA"%(x['STATUS']) if x['_merge'] == 'both' else x['STATUS']


def merge_files(self,df,merg):
    # path_2 = os.path.join(self.plain_dir,('%s.csv')%(merg['file']))
    path_2 = os.path.join(self.change_dir,('%s_WKT.csv')%(merg['file']))

    if merg['clean_up'] == 'NT_merge_1':
        df_2 = pd.read_csv(path_2)[merg['other_file']]
        df = df.merge(df_2,left_on=merg['this_file'],right_on=merg['other_file'],indicator=True,how='outer')
        df = df.query('_merge != "right_only"').copy()
        df['STATUS'] = df.apply(lambda x: NT_2_3_merge_cleanup(x), axis=1)
        df = df.drop('_merge', 1).drop_duplicates()

    elif merg['clean_up'] == 'WA_merge_1':
        df_2 = pd.read_csv(path_2,low_memory=False)[[merg['other_file'],'ALL_HOLDER']]
        df = df.merge(df_2,left_on=merg['this_file'],right_on=merg['other_file'],indicator=True,how='outer')
        df = df.query('_merge != "right_only"').copy()
        df = df.drop('_merge', 1).drop_duplicates()

    return df


# def str_title_format(x,source):
#     ''' converts 'hello there' to 'Hello There' '''
#     return x[source].title()

def commars_to_semicolon(x,source):
    ''' convert commars to semi-colons '''
    return x[source].replace(",",";")

def all_unknown(x,source):
    ''' returns 'Unknown' for every row '''
    return 'Unknown'


# def VIC_1_occ_status(x,source):
#     ''' convert commars to semi-colons '''
#     MININGMETH = x[source[0]].strip()
#     PLOTTYPE = x[source[1]].strip()
#     MININGMETH = MININGMETH[:len(MININGMETH) - 1].strip() if len(MININGMETH) > 0 else MININGMETH
#     if len(MININGMETH) > 0:
#         return MININGMETH
#     else:
#         return PLOTTYPE

def backup_field(x,source):
    ''' if the primary cell is blank then return the backup cell ''' 
    return x[source[1]] if x[source[0]] == "" else x[source[0]]


def value_before_first_commar(x,source):
    ''' the major material is the first material of all materials split by a commar when there is more than one '''
    return x[source].split(',')[0].strip()

def value_before_first_semi_colon(x,source):
    ''' the major material is the first material of all materials split by a cemi-colon when there is more than one '''
    return x[source].split(';')[0].strip()

def all_after_first_delimiter(value,delimiter):
    ''' the minor materials are all the materials except for the first in the list split by a delimiter '''
    if delimiter in value:
        return value[value.find(delimiter) + 1:].strip()
    else:
        return value

def all_after_first_commar(x,source):
    ''' the minor materials are all the materials except for the first in the list split by a commar '''
    return all_after_first_delimiter(x[source],',')

def all_after_first_semi_colon(x,source):
    ''' the minor materials are all the materials except for the first in the list split by a semi-colon '''
    return all_after_first_delimiter(x[source],';')


def dash_to_commar(x,source):
    ''' convert all dashes in a field to commars '''
    return x[source].replace(' - ',',') if '-' in x[source] else x[source]

def drop_brackets(x,source):
    ''' remove brackets and everything inside '''
    return re.sub(r"\((\w|\d)*\)","",x[source])

def remove_dashes(x,source):
    ''' remove all dashes from string. dashes at the start of strings are interpreted as formulas in excel '''
    return x[source].replace('--- ','')

def value_before_space(x,source):
    ''' get the value before the first space in a string '''
    return x[source].split(' ')[0]

def format_switch():
    return {
        "NSW_1-date": nsw_1_date_format,
        "NSW_1-status": nsw_1_date_status,
        "NSW_2-status": nsw_2_date_status,
        "time_since_expiry_desc": time_since_expiry_desc,
        "time_since_expiry_desc_noexpiry": time_since_expiry_desc_noexpiry,
        "time_since_lodge_desc": time_since_lodge_desc,
        "date-time-zero-to-null": date_time_zero_to_null,
        "join": join,
        "join-dash": join_dash,
        "join-with-dash-dif": join_with_dash_dif,
        "join-diff-2": join_dif_2,
        "QLD_1-status": QLD_1_status,
        "WA_8_status": WA_8_status,
        "join-with-text": join_with_text,
        "WA_1-status": WA_1_status,
        "WA_1-date": WA_1_date,
        "add-text": add_text,
        "combined_long": combined_long,
        "WA_5-status": WA_5_status,
        "join_multi_by_semi": join_multi_by_semi,
        "WA_6_type": WA_6_type,
        "SA_2_holder": SA_2_holder,
        "commars_to_semicolon": commars_to_semicolon,
        "all_unknown": all_unknown,
        "value_before_first_commar": value_before_first_commar,
        "value_before_first_semi_colon": value_before_first_semi_colon,
        "all_after_first_commar": all_after_first_commar,
        "all_after_first_semi_colon": all_after_first_semi_colon,
        "backup_field": backup_field,
        "dash_to_commar": dash_to_commar,
        "drop_brackets": drop_brackets,
        "remove_dashes": remove_dashes,
        "value_before_space": value_before_space
    }


# ###########################################################
# combining functions

def type_instr_fn(x,lst):
    ''' Some of the types need to be searched for in a string. if a ';' exists then all values need to be present. 
        'lst' is made from all the values from the 'OccType_R' file for the given groups and state. if the sub-string is found then its
        id is returned. if not found then NaN is returned and the 'Unknown' id is added in the next step 
    '''
    for row in lst:
        if ';' in row[0]:
            item = ';'.split()
            if item[0] in x and item[1] in x:
                return row[1]
        elif row[0] in x or row[0] == 'nan':
            return row[1]
    return np.nan


def format_type(self,df_main,configs):
    config = configs['type']
    key = config['format_type']
    field = config['field']
    type_df = self.type_df.query('STATE == "%s" and GROUP == "%s"'%(self.state, config['group'])).copy()

    if key == 'type_by_code':
        df = df_main.merge(type_df,left_on=field,right_on='DS_VALUE',how='left').drop(columns=['DS_VALUE','STATE','GROUP','GP_VALUE'],axis=1)
        # record missing values
        add_to_missing_df(self,df,'_id',field,config['group'],'TITLE_TYPE')
        # replace the nan's with the 'unknown' id
        df['_id'] = df['_id'].replace(np.nan,self.type_uk)
        df.rename(columns={'_id':'typ_id','STATE_B':'state_id','SHORE':'shore_id'},inplace=True)

    elif key in ['type_by_instr_code_a','type_by_instr_code_b']:
        ''' places values in and loops through them to find the type code that exists in the string '''
        df = type_df[['DS_VALUE','_id']].copy()
        s = df['DS_VALUE'].str.len().sort_values(ascending=False).index
        new_df = df.reindex(s)
        lst = new_df.values.tolist()
        # need to move 'R' to the end of the list to prevent those with a trailiing R being assigned 'Retention' type
        if key == 'type_by_instr_code_a':
            for i in lst:
                if i[0] == 'R':
                    r = i
            lst.remove(r)
            lst.insert(-1,r)

        df_main['typ_id'] = df_main[field].apply(lambda x: type_instr_fn(x,lst))
        # record missing values
        add_to_missing_df(self,df_main,'typ_id',field,config['group'],'TITLE_TYPE')
        # replace the nan's with the 'unknown' id
        df_main['typ_id'] = df_main['typ_id'].replace(np.nan,self.type_uk)
        df = df_main.merge(type_df[['_id','SHORE','STATE']],left_on='typ_id',right_on='_id',how='left').drop('_id',1)
        df.rename(columns={'STATE':'state_id','SHORE':'shore_id'},inplace=True)
    return df


#  and Group == "%s" status_configs['group']
def format_status(self,df_main,configs):
    config = configs['status']
    field = config['field']
    status_df = self.status_df.query('STATE == "%s"'%(self.state)).copy()
    df = df_main.merge(status_df,left_on=field,right_on='DS_VALUE',how='left').drop(columns=['GROUP','DS_VALUE','GP_VALUE'],axis=1)
    # record missing values
    add_to_missing_df(self,df,'_id',field,None,'TITLE_STATUS')
    # replace the nan's with the 'unknown' id
    df['_id'] = df['_id'].replace(np.nan,self.status_uk)
    df.rename(columns={'_id':'status_id'},inplace=True)
    return df


def format_dates(self,df_main,configs):
    date_configs = configs['dates']
    df_main.rename(columns=date_configs['rename'],inplace=True)
    for field in date_configs['create']:
        df_main[field] = '2999-01-01'
    return df_main


def tenement_format(self,df):
    df.rename(columns={'NEW_ID':'ind','geometry':'geom'},inplace=True)
    new_df = df[['ind','typ_id','status_id','state_id','shore_id','lodgedate','startdate','enddate','geom']].copy()
    new_df['valid_relations'] = True
    new_df['user_edit'] = False
    new_df['date_modified'] = date.today()
    new_df[['lodgedate','startdate','enddate']] = new_df[['lodgedate','startdate','enddate']].replace(np.nan,'2999-01-01')
    new_df['shore_id'] = new_df['shore_id'].replace(np.nan,self.shore_uk)
    return new_df


def format_related_ids(self,df_main,configs):
    ''' concatenate all the related ids '''
    ids_configs = configs['alternate_ids']
    df = pd.DataFrame([], columns=['tenement_id','tenoriginalid_id'])
    for column in ids_configs:
        temp_df = df_main[['NEW_ID',column]].set_axis(['tenement_id','tenoriginalid_id'],axis=1).copy()
        if len(df.index) > 1:
            df = pd.concat((df,temp_df))
        else:
            df = temp_df
    # remove any spaces in the ids
    df['tenoriginalid_id'] = df['tenoriginalid_id'].astype(str).str.replace(r'\s*','')
    df = df[~df['tenoriginalid_id'].isnull()]
    return df


def split_holder_perc(x):
    perc_raw = re.search(r"\([0-9].*\%\)", x).group(0)
    holder = x.replace(perc_raw,"").strip()
    perc = float(re.search(r"(?<=\().*(?=%)", perc_raw).group(0))
    return {'NEW_HOLDER_B': holder, 'percown': perc}


def format_holders(self,df_main,configs):
    holder_configs = configs['holders']

    if not holder_configs:
        return pd.DataFrame()

    field = holder_configs['field']
    tformat = holder_configs['format_type']
    holder_match = 'NEW_HOLDER_A'

    delimiter = ';' if tformat == 'semi-colon-split' else ','

    # set percown for single holders at 100 and those with more at 0. The 0's will be changed later if the data exists
    if not holder_configs['perc_own']:
        df_main['percown'] = df_main.apply(lambda x: 0 if type(x[field]) == float or delimiter in x[field] else 100, axis=1)

    s = df_main[field].str.split(delimiter).apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = holder_match
    df = df_main.join(s)

    if holder_configs['perc_own']:
        applied_df = df.apply(lambda x: split_holder_perc(x[holder_match]), axis='columns', result_type='expand')
        df = pd.concat([df, applied_df], axis='columns')
        holder_match = 'NEW_HOLDER_B'
    
    df[holder_match] = df[holder_match].apply(lambda x: x if type(x) == float else x.strip())
    df = df[['NEW_ID',holder_match,'percown']].copy()
    # filter out all row where holder is empty
    df = df[(~df[holder_match].isnull()) & (df[holder_match] != "")]
    companies_df = self.companies_df
    # .query('STATE == "%s"'%(self.state))
    new_df = df.merge(companies_df,left_on=holder_match,right_on='DS_VALUE',how='left')
    # record missing values
    add_to_missing_df(self,new_df,'_id',holder_match,None,'HOLDER')

    # make a df of only the missing holders and concatenate them to the Holder_df. These names will be used temporarily until they have been reviewed and updated
    missing_holders_df = new_df[new_df['_id'].isnull()][[holder_match]].drop_duplicates()
    if missing_holders_df.empty:
        final_df = new_df[['NEW_ID','percown','_id']].rename(columns={'NEW_ID':'tenement_id','_id':'name_id'}).drop_duplicates()
    else:
        missing_holders_df.columns = ['name']
        next_id = self.Holder_df['_id'].max() + 1
        missing_holders_df['_id'] = np.arange(next_id, len(missing_holders_df) + next_id)
        dic = {'user_name': 'ss', 'valid_relations': True, 'valid_instance': True, 'user_edit': False, 'date_modified': date.today(), 'date_created': date.today()}
        for i in dic:
            missing_holders_df[i] = dic[i]
        self.Holder_df = pd.concat((self.Holder_df,missing_holders_df)) # This will be saved to file once files have been added
        # create df of just the missing holders
        df_1 = new_df[new_df['_id'].isnull()][['NEW_ID',holder_match,'percown']]
        # merge with missing holders to get the ind values
        merge_df = df_1.merge(missing_holders_df,left_on=holder_match,right_on='name',how='left')[['NEW_ID','percown','_id']]
        # get the rest of the tenholder rows excluding the missing holder rows
        new_df = new_df[~new_df['_id'].isnull()][['NEW_ID','percown','_id']]
        # concat together to make the complete TenHolder df for the file. 
        final_df = pd.concat((new_df,merge_df)).rename(columns={'NEW_ID':'tenement_id','_id':'name_id'}).drop_duplicates()

    return final_df


def add_new_related_ids_to_core(self,new_related_ids_df,path):
    ''' look for related id's that don't exist in the core file and add them '''
    if fileExist(path):
        # delete the ind field and rename the other to 'code' to merge with the core file
        tenement_oid_df = new_related_ids_df.drop(new_related_ids_df.columns[0],axis=1)
        tenement_oid_df.columns = ['_id']
        tenement_oid_df['user_name'] = 'ss'
        tenement_oid_df['valid_instance'] = True
        tenement_oid_df['date_created'] = date.today()
        tenement_oid_df['_id'] = tenement_oid_df['_id'].astype(str)
        # open the current related_ids core file
        ten_id_df = pd.read_csv(path,dtype=str)
        # concat both and delete if the value is a duplicate. Only the original and new ids will remain
        final_df = pd.concat((ten_id_df,tenement_oid_df)).drop_duplicates(subset='_id', keep="first")
        # remove any null values
        final_df = final_df[final_df['_id'] != 'nan']
        final_df.to_csv(path,index=False)



def format_file_to_combine(self,file,configs):
    ''' formates and assigns the necessary fields from the raw data file to output files '''

    try:
        df = pd.read_csv(os.path.join(self.change_dir,('%s_WKT.csv')%(file)), low_memory=self.configs[file]['preformat']['low_memory']) # ,engine='python' 
    except pd.errors.EmptyDataError:
        print(f'## Empty file: {file}')
        return 
    except FileNotFoundError:
        print(f'## No file: {file}')
        return


    if df.empty:
        return None;
    
    # dictionary of the functions to format each file
    func_dic = format_switch()

    # merges data between 2 tables
    if configs['merge']:
        df = merge_files(self,df,configs['merge'])

    # formats the tables such as dates and creates new fields from multiple other fields such as for holders and status
    for row in configs['format']:
        func = func_dic[row['format_type']]
        # print(row['format_type'])
        df[row['new_field']] = df.apply(lambda x: func(x,row['source']), axis=1)

    # create 
    Tenement_nowkt_df = format_type(self,df,configs)
    Tenement_nowkt_df = format_status(self,Tenement_nowkt_df,configs)
    Tenement_nowkt_df = format_dates(self,Tenement_nowkt_df,configs)
    Tenement_nowkt_df = tenement_format(self,Tenement_nowkt_df)
    tenement_oid_df = format_related_ids(self,df,configs)
    TenHolder_df = format_holders(self,df,configs)

    self.Tenement_nowkt_df = pd.concat((self.Tenement_nowkt_df,Tenement_nowkt_df))
    self.tenement_oid_df = pd.concat((self.tenement_oid_df,tenement_oid_df))
    self.TenHolder_df = pd.concat((self.TenHolder_df,TenHolder_df))

    


# ######################################################
# format site data

def add_to_missing_df(self,df,null_field,raw_field,group,out_field):
    ''' raw_field: field from the lookup df to index the value that is missing
        out_field: the field where the value belongs in the output df
        group: either Mineral or Petroleum
        null_field: the field to filter for the Nan's. If this is nan than the value is new and needs to be added
    '''
    # get all the rows that have no match
    null_df = df[df[null_field].isnull()]
    # add all rows with no match to the missing_df so they can be updated later
    if not null_df.empty:
        temp_df = null_df[['NEW_ID',raw_field]].rename(columns={'NEW_ID': 'IND',raw_field:'VALUE'})
        temp_df['STATE'] = self.state
        temp_df['GROUP'] = group
        temp_df['FIELD'] = out_field
        self.missing_df = pd.concat((self.missing_df,temp_df))


#  and Group == "%s" status_configs['group']
def format_site_status(self,df_main,configs):
    config = configs['status']
    field = config['field']
    status_df = self.status_df.query('STATE == "%s" and GROUP == "%s"'%(self.state,config['group'])).copy()
    # replace blanks
    df_main[field] = df_main[field].replace('','Unknown')
    df = df_main.merge(status_df,left_on=field,right_on='DS_VALUE',how='left')
    # add all rows with no match to the missing_df so they can be updated later
    add_to_missing_df(self,df,'_id',field,config['group'],'SITE_STATUS')
    # replace the nan's with the 'unknown' id
    df['_id'] = df['_id'].replace(np.nan,self.status_uk)
    df = df.drop(columns=['GROUP','DS_VALUE'],axis=1)
    df.rename(columns={'_id':'status_id','geometry':'geom'},inplace=True)
    df['state_id'] = self.state
    return df


def format_site_size(self,df_main,configs):
    config = configs['size']
    field = config['field']
    size_df = self.size_df.query('STATE == "%s" and GROUP == "%s"'%(self.state,config['group'])).copy()
    df_main[field] = df_main[field].replace('','Unspecified')
    df = df_main.merge(size_df,left_on=field,right_on='DS_VALUE',how='left').drop(columns=['GROUP','DS_VALUE'],axis=1)
    # record missing values
    add_to_missing_df(self,df,'_id',field,config['group'],'SIZE')
    # replace missing values with the 'Unknown' id
    df['_id'] = df['_id'].replace(np.nan,self.size_uk)
    df.rename(columns={'_id':'size_id','NEW_ID':'ind'},inplace=True)
    return df


def occurrence_format(df):
    new_df = df[['geom','ind','status_id','size_id','state_id']].copy()
    new_df['user_name'] = 'ss'
    new_df['valid_relations'] = True
    new_df['valid_instance'] = True
    new_df['user_edit'] = False
    new_df['date_modified'] = date.today()
    new_df['date_created'] = date.today()
    return new_df

def format_names(self,df_main,configs):
    ''' find the formatted version of the site name and add those with no match to the missing_df.
        missing names will be added once the new names have been manually checked and formatted.
    '''
    # match with the formatted name and remove all rows where the name is blank
    df = df_main.merge(self.name_lookup_df,left_on=configs['name'],right_on='DS_VALUE',how='left').query('%s != ""'%(configs['name']))
    add_to_missing_df(self,df,'GP_VALUE',configs['name'],None,'NAME')
    # filter out all the rows with no match. these will be added later and will left out for now
    df = df[~df['GP_VALUE'].isnull()][['NEW_ID','GP_VALUE']]
    # if the df is empty then there are no matches so return an empty df
    if df.empty:
        df_final = pd.DataFrame([],columns=['occurrence_id','occname_id'])
    else:
        s = df['GP_VALUE'].str.split(';').apply(pd.Series, 1).stack()
        s.index = s.index.droplevel(-1)
        s.name = 'temp'
        df = df.join(s)
        df_final = df.merge(self.name_id_df,left_on='temp',right_on='name',how='left')[['NEW_ID','_id']].rename(columns={'NEW_ID':'occurrence_id','_id':'occname_id'})
    return df_final

def format_site_type(self,df_main,configs):
    ''' need to record a new type when found and rcord with the NEW_ID so it can be replaced later.
        missing values will be added later after manual inspection and formatting
    '''
    config = configs['type']
    field = config['field']
    type_df = self.type_df.query('STATE == "%s" and GROUP == "%s"'%(self.state,config['group'])).copy()
    df = df_main[['NEW_ID',field]].copy()
    # replace all Nan 
    df[field] = df[field].replace('','Unspecified')
    # separate the multiple types into their own rows
    s = df[field].str.split(';').apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = 'temp'
    s = s.str.strip()
    df = df.join(s)
    # remove all the blanks that occur when the 'type' collection contains a tailing semi-colon
    df = df[df['temp'] != '']
    # merge with the type df to get the types id's
    df_final = df.merge(type_df,left_on='temp',right_on='DS_VALUE',how='left')
    # record the missing values
    add_to_missing_df(self,df_final,'_id','temp',config['group'],'SITE_TYPE')
    # # replace Nan's with the 'Unknown' id
    # df_final['_id'] = df_final['_id'].replace(np.nan,self.type_uk)
    df_final = df_final[~df_final['_id'].isnull()][['NEW_ID','_id']]
    df_final.rename(columns={'NEW_ID':'occurrence_id','_id':'occtype_id'},inplace=True)
    # filter out all the nan's as no type exists
    df_final = df_final[~df_final['occtype_id'].isnull()]
    return df_final


def format_site_related_ids(self,df_main,configs):
    ''' concatenate all the related ids '''
    ids_configs = configs['alternate_ids']
    df = pd.DataFrame([], columns=['occurrence_id','occoriginalid_id'])
    for column in ids_configs:
        temp_df = df_main[['NEW_ID',column]].set_axis(['occurrence_id','occoriginalid_id'],axis=1).copy()
        if len(df.index) > 1:
            df = pd.concat((df,temp_df))
        else:
            df = temp_df
    df = df[df['occoriginalid_id'] != 0]
    return df


def split_to_separate_rows(df,field,delimiter):
    s = df[field].str.split(delimiter).apply(pd.Series, 1).stack()
    s.index = s.index.droplevel(-1)
    s.name = 'SPLIT_COLUMN'
    df = df.join(s)
    df['SPLIT_COLUMN'] = df['SPLIT_COLUMN'].apply(lambda x: x.strip())
    return df


def filter_out_mat_blanks(df,field):
    ''' filter out all rows with no material '''
    return df[df[field] != ""][['NEW_ID',field]].copy()

def match_material_code(self,df,mat_df,related_df,mat_field,group):
    ''' match the material with the correct code and add related materials where necessary, then drop duplicates.
        missing materials are added to the missing_df and will be added later after manual inspection and formatting
    '''
    df_2 = df.merge(mat_df,left_on=mat_field,right_on='DS_VALUE',how='left').query('%s != ""'%(mat_field))
    group_name = group.upper() + "_MATERIAL" # used to help identify it in the missing_df
    add_to_missing_df(self,df_2,'DS_VALUE',mat_field,None,group_name)
    df_2 = df_2[['NEW_ID','_id']]
    df_3 = related_df.merge(df_2,left_on='material',right_on='_id',how='inner')[['NEW_ID','related']].copy()
    df_2.rename(columns={'NEW_ID':'occurrence_id','_id':'material_id'},inplace=True)
    df_3.rename(columns={'NEW_ID':'occurrence_id','related':'material_id'},inplace=True)
    final_df = pd.concat((df_2,df_3)).drop_duplicates()
    return final_df[~final_df['material_id'].isnull()]

def finalize_min_mat_df(config,dic,maj_df):
    ''' if no minor materials field exists then return a blank df, if it does then clear all duplicates that exist in the major materials df'''
    if not config['minor']:
        return pd.DataFrame()
    else:
        return dic[1]['df'].merge(maj_df,how='outer',indicator=True).query('_merge == "left_only"').drop('_merge',1)


def format_material(self,df_main,configs):
    ''' collects and returns the major and minor materials '''
    config = configs['material']
    if not config:
        return pd.DataFrame(), pd.DataFrame()
    format_type = config['format_type']
    related_df = self.related_mat_df

    mat_df = self.material_df.query('STATE == "%s"'%(self.state)).copy()

    if format_type == 'commar_split' or format_type == 'semi_colon_split':
        delimiter = ',' if format_type == 'commar_split' else ';'
        dic = [{'name':'major'},{'name':'minor'}]
        for group in dic:
            field = config[group['name']]
            if field:
                group_df = filter_out_mat_blanks(df_main,field)
                new_df = split_to_separate_rows(group_df,field,delimiter)
                group['df'] = match_material_code(self,new_df,mat_df,related_df,'SPLIT_COLUMN',group['name'])

        maj_df = dic[0]['df']
        # remove minor materials that exist as a major material for each feature
        min_df = finalize_min_mat_df(config,dic,maj_df)
        return maj_df, min_df

    elif format_type == 'simple':
        dic = [{'name':'major'},{'name':'minor'}]
        for group in dic:
            field = config[group['name']]
            if field:
                group_df = filter_out_mat_blanks(df_main,field)
                group['df'] = match_material_code(self,group_df,mat_df,related_df,field,group['name'])

        maj_df = dic[0]['df']
        min_df = finalize_min_mat_df(config,dic,maj_df)
        return maj_df, min_df



def format_site_file_to_combine(self,file,configs):
    ''' formates and assigns the necessary fields from the raw data file to output files '''
    try:
        df = pd.read_csv(os.path.join(self.change_dir,('%s_WKT.csv')%(file)), low_memory=self.configs[file]['preformat']['low_memory']).fillna('')
    except pd.errors.EmptyDataError:
        print(f'## Empty file: {file}')
        return
    except FileNotFoundError:
        print(f'## No file: {file}')
        return


    if df.empty:
        return None;
    
    # dictionary of the functions to format each file
    func_dic = format_switch()

    # merges data between 2 tables
    if configs['merge']:
        df = merge_files(self,df,configs['merge'])

    # formats the tables such as dates and creates new fields from multiple other fields such as for holders and status
    for row in configs['format']:
        func = func_dic[row['format_type']]
        # print(row['format_type'])
        df[row['new_field']] = df.apply(lambda x: func(x,row['source']), axis=1)

    site_df = format_site_status(self,df,configs)
    site_df = format_site_size(self,site_df,configs)
    site_df = occurrence_format(site_df)
    site_name_df = format_names(self,df,configs)
    site_type_df = format_site_type(self,df,configs)
    site_related_ids_df = format_site_related_ids(self,df,configs)
    site_maj_mat_df, site_min_mat_df = format_material(self,df,configs)

    self.Occurrence_df = pd.concat((self.Occurrence_df,site_df))
    self.occurrence_oid_df = pd.concat((self.occurrence_oid_df,site_related_ids_df))
    self.occurrence_name_df = pd.concat((self.occurrence_name_df,site_name_df))
    self.occurrence_typ_df = pd.concat((self.occurrence_typ_df,site_type_df))
    self.occurrence_majmat_df = pd.concat((self.occurrence_majmat_df,site_maj_mat_df))
    self.occurrence_minmat_df = pd.concat((self.occurrence_minmat_df,site_min_mat_df))

# def tempp(i,x):
#     print(i,x)
#     return fuzz.ratio(i.lower(),x.lower())


def find_similar_values(missing_df,lookup_df,lookup_field,lst):
    '''  '''
    if missing_df.empty:
        final_df = pd.DataFrame([],columns=['ORIGINAL','LIKELY_MATCH','MATCH_A','MATCH_B','MATCH_C','MATCH_D','MATCH_E'])
    else:
        final_df = pd.DataFrame()
        for i in lst:
            # print(lookup_field)
            # print(lookup_df.head())
            # get ratio for each value in the holder df and then return the top 5
            lookup_df['ratio'] = lookup_df[lookup_field].apply(lambda x: fuzz.ratio(i.lower(),x.lower()))
            # lookup_df['ratio'] = lookup_df[lookup_field].apply(lambda x: tempp(i,x))
            lookup_df.sort_values(by='ratio', ascending=False, inplace=True)
            top_df = lookup_df.head()
            # get max ratio, it is used to determine if the match is close enough, above 85, to record as a likely match
            max_ratio = top_df['ratio'].max()
            top_df = top_df[[lookup_field]].T
            top_df.columns = ['MATCH_A','MATCH_B','MATCH_C','MATCH_D','MATCH_E']
            top_df['MAX_RATIO'] = [max_ratio]

            final_df = pd.concat((final_df,top_df))

        final_df['ORIGINAL'] = missing_df['VALUE'].to_list()
        final_df['LIKELY_MATCH'] = final_df.apply(lambda x: x['MATCH_A'] if x['MAX_RATIO'] >= 85 else np.nan,axis=1)
        final_df = final_df[['ORIGINAL','LIKELY_MATCH','MATCH_A','MATCH_B','MATCH_C','MATCH_D','MATCH_E']]
    return final_df


def find_similar_mat_values(missing_df,lookup_df,lookup_field,lst,mat_raw_format_df):
    if missing_df.empty:
        final_df = pd.DataFrame([],columns=['ORIGINAL','LIKELY_MATCH','MATCH_A','MATCH_B','MATCH_C','MATCH_D','MATCH_E'])
    else:
        final_df = pd.DataFrame()
        for i in lst:
            # get ratio for each value in the holder df and then return the top 5
            temp_lookup_df = lookup_df[~lookup_df[lookup_field].isnull()].copy()
            temp_lookup_df['ratio'] = temp_lookup_df[lookup_field].apply(lambda x: fuzz.ratio(i.lower(),x.lower()))
            temp_lookup_df.sort_values(by='ratio', ascending=False, inplace=True)
            # temp_lookup_df = temp_lookup_df.merge(mat_raw_format_df,left_on='code',right_on='',how='left',suffixes=('','_y'))
            temp_lookup_df.drop_duplicates(subset='GP_VALUE',inplace=True)
            top_df = temp_lookup_df.head(8)
            # get max ratio, it is used to determine if the match is close enough, above 85, to record as a likely match
            max_ratio = top_df['ratio'].max()
            top_df = top_df[['GP_VALUE']].T
            top_df.columns = ['MATCH_A','MATCH_B','MATCH_C','MATCH_D','MATCH_E','MATCH_F','MATCH_G','MATCH_H']
            top_df['MAX_RATIO'] = [max_ratio]

            final_df = pd.concat((final_df,top_df))

        final_df['ORIGINAL'] = missing_df['VALUE'].to_list()
        final_df['LIKELY_MATCH'] = final_df.apply(lambda x: x['MATCH_A'] if x['MAX_RATIO'] >= 85 else np.nan,axis=1)
        final_df = final_df[['ORIGINAL','LIKELY_MATCH','MATCH_A','MATCH_B','MATCH_C','MATCH_D','MATCH_E','MATCH_F','MATCH_G','MATCH_H']]
    return final_df


def all_core_options_as_matches(self,miss_df,file_name,miss_field,return_field,columns_lst):
    ''' get all the options that are available from the related core file and provide them as possible matches. The user will be able to 
        easily pick the best fit rather than having to open the core file first.
    '''
    df = pd.read_csv(os.path.join(self.output_dir,'core',file_name+'.csv'),engine='python').sort_values(by=return_field)[[return_field]].T
    df.columns = columns_lst[:len(df.columns)]
    df['FIELD'] = miss_field
    miss_field_df = miss_df[miss_df['FIELD'] == miss_field]
    final_df = miss_field_df.merge(df,on='FIELD',how='left').rename(columns={'VALUE':'ORIGINAL'})
    return final_df



# miss_df = pd.read_csv(os.path.join(self.update_dir,'missing_reduced.csv'))
def finalize_missing_data(self):
    ''' create and output missing data files, along with a file that contains similar names for missing companies 
        materials: this will look for similar raw values and return the nearest matches formatted name
        holder: the likely match is the value that will be displayed in the database
        name: no matching required, only use 'title' method to format
        status: returns all available options that currectly exist

        save missing data in files stored in the output/update directory.
        missing_all: each missing item with its 'ind' value so it can be updated later
        missing_reduced: unique missing values. if a new company name appears for multiple titles, it will only be shown in this list once.
        use fuzzywuzzy to find similar names for missing company names
    '''

    configs = getJSON(os.path.join(self.configs_dir,'commit_updates.json'))
    self.update_dir = os.path.join(self.output_dir,'update')

    # drop the material duplicates that exist in the major and minor group
    missing_df = self.missing_df
    materials_df = missing_df[missing_df['FIELD'].isin(['MAJOR_MATERIAL','MINOR_MATERIAL'])].drop_duplicates(subset=['STATE', 'VALUE'])
    non_materials_df = missing_df[~missing_df['FIELD'].isin(['MAJOR_MATERIAL','MINOR_MATERIAL'])]
    missing_df = pd.concat((non_materials_df,materials_df))
    # reduce the missing data to unique values without the ind values. save missing files
    miss_df = missing_df.drop('IND',1).drop_duplicates()
    # save the missing and reduced_missing df's to file
    missing_df.to_csv(os.path.join(self.update_dir,'missing_all.csv'),index=False)
    miss_df.to_csv(os.path.join(self.update_dir,'missing_reduced.csv'),index=False)    

    # # used for testing only
    # missing_df = pd.read_csv(os.path.join(self.update_dir,'missing_all.csv'))
    # miss_df = pd.read_csv(os.path.join(self.update_dir,'missing_reduced.csv'))   

    complete_df = pd.DataFrame()
    columns_lst = ['MATCH_%s'%(x) for x in list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')]

    for FIELD in configs:
        print('Working on: %s'%(FIELD))
        # if FIELD == 'MAJOR_MATERIAL':
        field_miss_df = miss_df.query('FIELD == "%s"'%(FIELD))
        if not field_miss_df.empty:
            config = configs[FIELD]['find_matches']
            match_type = config['type']
            file = config['file']
            return_field = config['field']

            if match_type == 'core_match':
                # find the most similar values from the appropriate field in the core file
                core_df = pd.read_csv(os.path.join(self.output_dir,'core',file+'.csv'),engine='python')
                core_df = core_df[[return_field]]
                value_miss_df = field_miss_df[['VALUE']]
                values_miss_lst = value_miss_df['VALUE'].to_list()
                matched_df = find_similar_values(value_miss_df,core_df,return_field,values_miss_lst)
                final_df = field_miss_df.merge(matched_df,left_on='VALUE',right_on='ORIGINAL',how='left').drop('VALUE',1)

            elif match_type == 'all_core':
                # get all the core options and insert them as matches so the correct one can easily be selected and inserted as the LIKELY option
                final_df = all_core_options_as_matches(self,miss_df,file,FIELD,return_field,columns_lst)

            elif match_type == 'format_only':
                # convert all to title format
                final_df = field_miss_df.rename(columns={'VALUE':'ORIGINAL'})
                final_df['LIKELY_MATCH'] = final_df['ORIGINAL'].apply(lambda x: x.title())

            elif match_type == 'raw_match_for_core':
                # match the missing fields wth the most similar values from the raw file and then return their match in the core file
                core_file = file['core']
                raw_file = file['raw']
                core_field = return_field['core']
                raw_field = return_field['raw']
                core_df = pd.read_csv(os.path.join(self.output_dir,'core',core_file+'.csv'),engine='python')
                raw_df = pd.read_csv(os.path.join(self.convert_dir,raw_file+'.csv'),engine='python')
                raw_df = raw_df[~raw_df[raw_field].isnull()]
                # match the raw material codes with their formatted name
                mat_raw_format_df = raw_df.merge(core_df,left_on='GP_VALUE',right_on='name',how='left')
                mat_raw_format_df = mat_raw_format_df[~mat_raw_format_df['DS_VALUE'].isnull()]
                value_miss_df = field_miss_df[['VALUE']].drop_duplicates()
                values_miss_lst = value_miss_df['VALUE'].to_list()
                matched_df = find_similar_mat_values(value_miss_df,raw_df,'DS_VALUE',values_miss_lst,mat_raw_format_df)
                final_df = field_miss_df.merge(matched_df,left_on='VALUE',right_on='ORIGINAL',how='left').drop('VALUE',1)

            # concatenate the df's
            complete_df = pd.concat((complete_df,final_df))

    # create a file for user to make final changes before they are commited
    complete_df.to_csv(os.path.join(self.update_dir,'manual_update_required.csv'),index=False)



