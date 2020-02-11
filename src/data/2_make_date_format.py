#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  2 21:02:56 2020

@author: brycehaley
"""

import pandas as pd
import os
from datetime import datetime

def import_csv_w_date_flags()-> pd.DataFrame:
    """Grab csv generated at step1
    """
    path =  os.path.join('..', '..', 'data', 'interim', '1_tbs_with_date_flags.csv')
    return pd.read_csv(path, index_col = 0) 

def format_slash_date(date_str: str, day_first: bool = True) -> datetime:
    """Format m/d/y or d/m/y date times into datetime.datetime
    """
    date, time = date_str.split()
    date_arr = date.split('/')
    if len(date_arr) == 3:
        if day_first:
            day, month, year = int(date_arr[0]), int(date_arr[1]), int(date_arr[2])
        else:
            month, day, year = int(date_arr[0]), int(date_arr[1]), int(date_arr[2])
        
        time_arr = time.split(':')
        if len(time_arr) == 3:
            hour, minute, second = int(time_arr[0]), int(time_arr[1]), int(time_arr[2])
        elif len(time_arr) == 2:
            hour, minute, second = int(time_arr[0]), int(time_arr[1]), 0
        elif len(time_arr) == 1:
            hour, minute, second = time_arr[0], 0 , 0
        else:
            hour, minute, second = 0, 0, 0
            
        if year == 17:
            year = 2017
        elif year == 18:
            year = 2018
    else:
        year, month, day, hour, minute, second = 2000, 1, 1, 0, 0, 0
        print('error on :', date_str)
    
    
    return datetime(year, month, day, hour, minute, second)

def dash_to_slash(datetime_str: str) -> str:
    """Convert d-m-y to y-m-d where original data recorded day in year format
    """
    date, time = datetime_str.split()
    date_arr = date.split('-')
    if len(date_arr[0]) > 2:
        date_arr[0] = date_arr[0][-2:]
    date_str = '/'.join(date_arr)
    ret_string = date_str + ' ' + time
    return ret_string



def main():
    first_loc_mdy = 465476 # magic number which is first loc of format switching from dmy to mdy
    
    df = import_csv_w_date_flags()
    start_dash_mask = (df['trip_start_time_dash_flag'] == True)
    start_dash_df = df[start_dash_mask]
    
    stop_dash_mask = (df['trip_stop_time_dash_flag'] == True)
    stop_dash_df = df[stop_dash_mask]
    
    df.loc[start_dash_mask, 'trip_start_time'] = start_dash_df['trip_start_time'].apply(dash_to_slash)
    df.loc[stop_dash_mask, 'trip_stop_time'] = stop_dash_df['trip_stop_time'].apply(dash_to_slash)
    
    save_path = os.path.join('..', '..', 'data', 'interim', '2_just_dashes.csv')
    df.to_csv(save_path)
    
    df_mdy = df.iloc[first_loc_mdy:,:]
    df_dmy = df.iloc[:first_loc_mdy,:]
    
    df_mdy['trip_start_time'] = df_mdy['trip_start_time'].apply(format_slash_date, day_first = False)
    df_mdy['trip_stop_time'] = df_mdy['trip_stop_time'].apply(format_slash_date, day_first = False)
    
    df_dmy['trip_start_time'] = df_dmy['trip_start_time'].apply(format_slash_date, day_first = True)
    df_dmy['trip_stop_time'] = df_dmy['trip_stop_time'].apply(format_slash_date, day_first = True)
    
    df = pd.concat([df_dmy, df_mdy], axis=0)
    
    df = df.drop(['trip_start_time_dash_flag', 'trip_stop_time_dash_flag', 'trip_start_time_slash_flag',
             'trip_stop_time_slash_flag', 'trip_start_timeis_good_date', 'trip_stop_timeis_good_date'], axis=1)
    
    df['day'] = df['trip_start_time'].dt.dayofweek
    df['week'] = df['trip_start_time'].dt.week
    df['month'] = df['trip_start_time'].dt.month
    df['quarter'] = df['trip_start_time'].dt.quarter
    df.loc[(df['quarter'] == 1) & (df['week'] == 52), 'week'] = 0
    
    
    save_path = os.path.join('..', '..', 'data', 'interim', '2_tbs_with_datetime.csv')
    df.to_csv(save_path)
    
if __name__ == '__main__':
    main()