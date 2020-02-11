#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb  8 22:05:01 2020

@author: brycehaley
"""
import os
import pandas as pd
import math

def import_csv() -> pd.DataFrame:
    path = os.path.join('..', '..', 'data', 'interim', '3_tbs_all_filled.csv')
    df =  pd.read_csv(path, index_col = 0)
    df['trip_start_time'] = pd.to_datetime(df['trip_start_time'], format='%Y-%m-%d %H:%M:%S')
    df['trip_stop_time'] = pd.to_datetime(df['trip_stop_time'], format='%Y-%m-%d %H:%M:%S')
    return df

def main():
    df = import_csv()
    df['overages'] = df['trip_duration_seconds'].apply(lambda x: math.floor(max(0, x - 1800) / 1800) * 4)
    save_path = os.path.join('..', '..', 'data', 'processed', 'final.csv')
    df.to_csv(save_path)
    
    
if __name__ == '__main__':
    main()