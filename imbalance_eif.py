# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 17:22:49 2017
1) Merge ForecastedSUPPLY.xlsx and ForecastedDEMAND.xlsx 
2) Convert columns to rows
3) Generates EIF Forecasted Merge.csv
@author: agarwalm
"""
import pandas as pd
import os
# r: Use a raw string, to make sure that Python doesn't try to interpret anything following a \ as an escape sequence.
working_dir = r"D:\BI\dashboards\005 Imbalance" 

os.chdir(working_dir)

supply_df = pd.read_excel("ForecastedSUPPLY.xlsx", dtype='unicode')

new_supply_df = pd.melt(supply_df, id_vars=['GOF', 'FAC', 'EQP Type'], var_name='XNS_DATE', value_name='UNIT')
new_supply_df['CATEGORY']= "FORECAST SUPPLY"
new_supply_df.columns = ['GOF', 'FAC', 'EQP_TYP', 'XNS_DATE', 'UNIT', 'CATEGORY']

demand_df = pd.read_excel("ForecastedDEMAND.xlsx", dtype='unicode')
#print(demand_df.columns.values.tolist())
new_demand_df = pd.melt(demand_df, id_vars=['GOF', 'FAC', 'EQP Type'], var_name='XNS_DATE', value_name='UNIT')
new_demand_df['CATEGORY']= "FORECAST DEMAND"
new_demand_df.columns = ['GOF', 'FAC', 'EQP_TYP', 'XNS_DATE', 'UNIT', 'CATEGORY']

frames = [new_supply_df, new_demand_df]
merge_df = pd.concat(frames)
merge_df [['UNIT']] = merge_df[['UNIT']].astype(float)
merge_df = merge_df[(merge_df['UNIT'] > 0) ]

merge_df['XNS_DATE'] = merge_df.XNS_DATE.str.replace(r'\r\n', '') 

merge_df.to_csv("EIF Forecasted Merge.csv" , index=False)
print ('Merge done.')
