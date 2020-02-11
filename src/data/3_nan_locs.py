#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 12:42:48 2020

@author: brycehaley
"""
import pandas as pd
import numpy as np
import os


def import_csv_from2() -> pd.DataFrame:
    path = os.path.join('..', '..', 'data', 'interim', '2_tbs_with_datetime.csv')
    df =  pd.read_csv(path, index_col = 0)
    df['trip_start_time'] = pd.to_datetime(df['trip_start_time'], format='%Y-%m-%d %H:%M:%S')
    df['trip_stop_time'] = pd.to_datetime(df['trip_stop_time'], format='%Y-%m-%d %H:%M:%S')
    return df

def move_date_fillna(df: pd.DataFrame) -> pd.DataFrame:
    df['nan_row_flag'] = ( df['to_station_name'].isnull()) & (df['user_type'].isnull()) 

    df_badcols = df[df['nan_row_flag'] == True]
    df_badcols.loc[:,'user_type'] = df_badcols.loc[:,'to_station_id']
    df_badcols.loc[:,'to_station_name'] = df_badcols.loc[:,'from_station_name']
    df_badcols.loc[:,'to_station_id'] = np.nan
    df_badcols.loc[:,'from_station_name'] = df_badcols.loc[:,'from_station_id']
    df_badcols.loc[:,'from_station_id'] = np.nan
    
    df.loc[df.index.isin(df_badcols.index), :] = df_badcols
    
    df_good_cols = df[df['nan_row_flag'] == False]
    df_good_cols.loc[:,'to_station_id'] = df_good_cols.loc[:,'to_station_id'].astype(int)
    df_good_cols.loc[:,'from_station_id'] = df_good_cols.loc[:,'from_station_id'].astype(int)
    
    df.loc[df.index.isin(df_good_cols.index), :] = df_good_cols
    
    return df

def is_one_station_for_name(df: pd.DataFrame) -> bool:
    # df_goodcols_big.groupby(['to_station_id','to_station_name']).size().reset_index().rename(columns={0:'count'})
    df_test_from = df.groupby(['from_station_id','from_station_name']).size().reset_index().rename(columns={0:'count'})
    from_bool = df_test_from[df_test_from['from_station_id'].duplicated(keep=False)].shape[0]  == 0
    
    df_test_to = df.groupby(['to_station_id','to_station_name']).size().reset_index().rename(columns={0:'count'})
    to_bool = df_test_to[df_test_to['to_station_id'].duplicated(keep=False)].shape[0]  == 0
    
    return to_bool & from_bool
    
def make_name_id(df: pd.DataFrame) -> pd.DataFrame:
    df_good_cols = df[df['nan_row_flag'] == False]
    
    assert is_one_station_for_name(df_good_cols)
    
    name_id = pd.Series(df_good_cols['from_station_id'].values, index = df_good_cols['from_station_name'])
    return name_id


def main():
    df = import_csv_from2()
    df = move_date_fillna(df)
        
    #name_id_df = make_name_id(df)
    #print(name_id_df)
    save_name_id_path = os.path.join('..', '..', 'data', 'processed', '3_tbs_name_id.csv')
    df.to_csv(save_name_id_path)
    
    df = df.drop(columns =['to_station_id', 'from_station_id', 'nan_row_flag'])
    
    save_path = os.path.join('..', '..', 'data', 'interim', '3_tbs_all_filled.csv')
    df.to_csv(save_path)



if __name__ == '__main__':
    main()