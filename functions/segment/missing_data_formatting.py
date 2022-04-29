import os
import re
import pandas as pd
import numpy as np
from functions.common.directory_files import get_json
from ..setup import SetUp
from functions.common.timer import Timer
from functions.logging.logger import logger



class MissingDataFormatting:
    """ functions to format the values inserted in the manual_data_required.csv """
    missing_data_format = get_json(os.path.join(SetUp.configs_dir,'missing_data_format.json'))
    # # used for testing
    # data_df = pd.read_csv("C:/Users/terrymac/Desktop/manual_update_required_220222.csv",dtype=str)
    # data_df = pd.read_csv(os.path.join(setup.output_dir,'update','manual_update_required.csv'),dtype=str)
    
    holder_conversions = [
        [" pty. ", " Pty "],
        [" ltd.", " Ltd"],
        [" limited.", " Ltd"],
        [" limited", " Ltd"],
        [" proprietary.", " Pty"],
        [" proprietary", " Pty"],
        [" no liability", " NL"],
        [" n.l.", " NL"],
        ["'s ", "'s "]
    ]
    
    unnamed_lst = [
        "unnamed",
        "operation not named",
        "unknown",
        "unexpected"
    ]
    
    
    def format_original_values(self,df):
        """ Apply formatting to ORIGINAL field in manual_update_required file """
        # format each of the ORIGINAL values accordingly and output in new field
        timer = Timer()
        logger(message="Format ORIGINAL field values where necessary", category=4)
        df['ORIGINAL_F'] = df.apply(lambda x: self.format_missing_row(x['STATE'],x['FIELD'],x['ORIGINAL']), axis=1)
        logger(message="Total ORIGINAL_F field creation run time: %s" %(timer.time_past()), category=4)
        return df
        
    
    def format_missing_row(self,state,field,value):
        """ find the correct method and format the value if necessary """
        if not field in self.missing_data_format[state]:
            return None
        for formatter in self.missing_data_format[state][field]:
            value = getattr(self, formatter)(value)
        return value
            
            
    @staticmethod
    def case_insensitive_replace(repl,subs,string):
        compiled = re.compile(re.escape(subs), re.IGNORECASE)
        return compiled.sub(repl, string)
            
    
    def pty_ltd_conversions(self,value):
        """ format common values to standard values given in 'holder_conversions' """
        for conversion in self.holder_conversions:
            if conversion[0] in value.lower():
                value = self.case_insensitive_replace(conversion[1],conversion[0],value)
        return value
       
        
    @staticmethod
    def proper_text(value):
        """ format string to proper format, first letter capital and the rest are lowercase """
        return value.title()
    
    
    def delete_unnamed(self,value):
        """ remove any unnamed value and insert 'DROP' so the value will be deleted from the database with no replacement"""
        for unnamed in self.unnamed_lst:
            if unnamed in value.lower():
                return 'DROP'
        return value
