# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 15:06:50 2017

@author: agarwalm
"""
'''
1.	MAIN_CUSTOMER_NAME
2.	MAIN_CUST_CODE         
3.	EQUIPMENT_GROUP     
4.	TRADE_GROUP 
5.	TRADE_DOMINANCE (A dominant and B non-dominant)
6.	AVERAGE STAY DAYS

**	Exclude records without main cust.

'''
import pandas as pd
import os
import shared_functions as sf


key = ['MAIN CONTRACT CUSTOMER', 'MAIN CONTRACT CUSTOMER NAME', 'TRADE MGT GROUP', 'TRADE DOMINANCE', 'CUSTOMER STAY DAYS', 'Equipment_Group', 'TYPE']
# r: Use a raw string, to make sure that Python doesn't try to interpret anything following a \ as an escape sequence.
working_dir = r"D:\BI\dashboards\Customer Ranking"
os.chdir(working_dir)

pdm_df = pd.read_csv("LYM_PDM_FREE_TIME_0.csv", dtype='unicode')
np_df = pd.read_csv("LYM_PDM_NPS_DATA.csv", dtype='unicode')


new_df = pd.merge(pdm_df, np_df, how='inner', left_on=['BL/BKG NO.', 'BL SUFFIX', 'EQUIPMENT NO.'], right_on= ['BKG/BL NO.','BKG/BL SUFFIX','EQUIPMENT NO.'])
new_df  = new_df[(pd.notnull(new_df['MAIN CONTRACT CUSTOMER']))]

# Create Equipment Group column
new_df['Equipment_Group'] = new_df['TYPE'].apply(sf.get_equipment_group)

#print(new_df['Equipment_Group'].unique())

new_df['TRADE DOMINANCE'] = new_df['TRADE DOMINANCE'].apply(lambda x: "A" if x == "Dominant" else "B" )


df = new_df[key]; 
print((df.columns))

df[['CUSTOMER STAY DAYS']] = df[['CUSTOMER STAY DAYS']].astype(float)

# removed from key 'CUSTOMER STAY DAYS', 'TYPE'
key = ['MAIN CONTRACT CUSTOMER', 'MAIN CONTRACT CUSTOMER NAME', 'TRADE MGT GROUP', 'TRADE DOMINANCE', 'Equipment_Group']
df = df.groupby(by= key, as_index=False, sort=True).agg({'CUSTOMER STAY DAYS': 'mean'}) 
sf.df_format_col_name(df)
df.to_csv("cus_evl_stay_days.csv", index=False, encoding="utf-8")
print('Done')