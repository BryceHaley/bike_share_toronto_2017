#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  2 16:39:01 2020

@author: brycehaley
"""

import pandas as pd
import os
import re

def import_raw_csv() -> pd.DataFrame:
    """Returns raw df from raw csv
    """
    bike_path = os.path.join('..', '..', 'data', 'raw', '2017_Toronto_Bikeshare.csv')
    return pd.read_csv(bike_path) 

def drop_bottom_row(df: pd.DataFrame) -> pd.DataFrame:
    """drops last row of df
    """
    last_row = len(df) - 1
    print(last_row)
    return df.drop(df.index[last_row])

def dash_matcher(date: str) -> bool:
    """Checks if given string date is in 'dashed' format ending in 17 or 18
    Examples:
        dash_matcher(2018-01-17) -> True
        dash_matcher(12-12-18) -> True
        dash_matcher(1/2/17) -> False
        
    Args:
        date: a string to be testsed for date formating
    """
    dash_date = r'(\d{2}|\d{4})-\d?\d-1(7|8)'
    z = re.search(dash_date, date)
    if z:
        return True
    return False

def slash_matcher(date: str) -> bool:
    """Checks if given string date is in 'slashed' format ending in 17 or 18
    
    Examples:
        slash_matcher(2018-01-17) -> False
        slash_matcher(12-12-18) -> False
        slash_matcher(1/2/17) -> True
        slash_matcher(12/12/2018) - True
4
        s
    Args:
        date: a string to be testsed for date formating
    """
    slash_date = r'\d?\d/\d?\d/(\d{1,2})?1(7|8)'
    z = re.search(slash_date, date)
    if z:       
        return True
    return False

def create_date_flags(col_names: list, df: pd.DataFrame) -> pd.DataFrame:
    """Adds flag columns to indicate format of date of date columns
    """
    for col_name in col_names:
        df[col_name + '_slash_flag'] = df[col_name].apply(slash_matcher)
        df[col_name + '_dash_flag'] = df[col_name].apply(dash_matcher)
        df[col_name + 'is_good_date'] = df[col_name + '_slash_flag'] | df[col_name + '_dash_flag'] 
        df = df[df[col_name + 'is_good_date']] # drop records where date is not readily parseable
    return df

def main():
    df = import_raw_csv()
    df = drop_bottom_row(df)
    df = create_date_flags(['trip_start_time', 'trip_stop_time'], df)
    save_path = os.path.join('..', '..', 'data', 'interim', '1_tbs_with_date_flags.csv')
    df.to_csv(save_path)
    

if __name__ == '__main__':
    main()
