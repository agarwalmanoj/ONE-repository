# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 12:13:59 2017

@author: agarwalm
"""

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
import shared_functions as sf

# r: Use a raw string, to make sure that Python doesn't try to interpret anything following a \ as an escape sequence.
working_dir = r"D:\BI\dashboards\005 Imbalance\accuracy"
os.chdir(working_dir)


act_df = pd.read_excel('Actuals_for_30Oct_to_5Nov_2017.xlsx', sheetname="Actual Demand from Export" , dtype='unicode')
sf.df_format_col_name(act_df)
act_df = act_df.rename(columns={'Atloc_Code' : 'Fac', 'Count': 'Actual', 'Eqp_Typ': 'Eqp_Type', 'Gof_Code':'Gof'})


for1_df = pd.read_excel('ForecastedDEMANDasOf30Oct.xlsx', dtype='unicode')
for1_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for1_df)
for1_df= for1_df.rename(columns={'Current_30-Oct-17_-_05-Nov-17' : 'Forecast'})
for1_df= for1_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


#df[1].fillna(0, inplace=True)
for2_df = pd.read_excel('ForecastedDEMANDasOf23Oct.xlsx', dtype='unicode')
for2_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for2_df)
for2_df= for2_df.rename(columns={'Week_1_30-Oct-17_-_05-Nov-17' : 'Forecast'})
for2_df= for2_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


#df[1].fillna(0, inplace=True)
for3_df = pd.read_excel('ForecastedDEMANDasOf16Oct.xlsx', dtype='unicode')
for3_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for3_df)
for3_df= for3_df.rename(columns={'Week_2_30-Oct-17_-_05-Nov-17' : 'Forecast'})
for3_df= for3_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


for4_df = pd.read_excel('ForecastedDEMANDasOf9Oct.xlsx', dtype='unicode')
for4_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for4_df)
for4_df= for4_df.rename(columns={'Week_3_30-Oct-17_-_05-Nov-17' : 'Forecast'})
for4_df= for4_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


act_for1_df = pd.merge(act_df, for1_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_for1_df ['Weeks Forecast Before'] = "1"

act_for2_df = pd.merge(act_df, for2_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_for2_df ['Weeks Forecast Before'] = "2"

act_for3_df = pd.merge(act_df, for3_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_for3_df ['Weeks Forecast Before'] = "3"

act_for4_df = pd.merge(act_df, for3_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_for4_df ['Weeks Forecast Before'] = "4"

act_for_df = pd.concat([act_for1_df, act_for2_df, act_for3_df, act_for4_df])


act_for_df[['Forecast']] = act_for_df[['Forecast']].astype(float)
act_for_df['Forecast'] = act_for_df['Forecast'].fillna(0)

act_for_df.columns.values[0] = "Demand Supply Type"

print(act_for_df.columns)
act_for_df.to_csv("act_for_df.csv" , index=False)

# Start SUPPLY laden Import part 
act_sup_df = pd.read_excel('Actuals_for_30Oct_to_5Nov_2017.xlsx', sheetname="Actual Supply from IMP" , dtype='unicode')
sf.df_format_col_name(act_sup_df)
act_sup_df = act_sup_df.rename(columns={'Atloc_Code' : 'Fac', 'Count': 'Actual', 'Eqp_Typ': 'Eqp_Type', 'Gof_Code':'Gof'})


for1_sup_df = pd.read_excel('ForecastedSUPPLYasOf30Oct.xlsx', dtype='unicode')
for1_sup_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for1_sup_df)
for1_sup_df = for1_sup_df.rename(columns={'Current_30-Oct-17_-_05-Nov-17_Laden_Import' : 'Forecast'})
for1_sup_df = for1_sup_df [['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


#df[1].fillna(0, inplace=True)
for2_sup_df = pd.read_excel('ForecastedSUPPLYasOf23Oct.xlsx', dtype='unicode')
for2_sup_df .rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for2_sup_df )
for2_sup_df = for2_sup_df .rename(columns={'Week_1_30-Oct-17_-_05-Nov-17_Laden_Import' : 'Forecast'})
for2_sup_df = for2_sup_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


#df[1].fillna(0, inplace=True)
for3_sup_df = pd.read_excel('ForecastedSUPPLYasOf16Oct.xlsx', dtype='unicode')
for3_sup_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for3_sup_df)
for3_sup_df = for3_sup_df.rename(columns={'Week_2_30-Oct-17_-_05-Nov-17_Laden_Import' : 'Forecast'})
for3_sup_df = for3_sup_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


for3_sup_df = pd.read_excel('ForecastedSUPPLYasOf16Oct.xlsx', dtype='unicode')
for3_sup_df .rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for3_sup_df)
for3_sup_df = for3_sup_df.rename(columns={'Week_2_30-Oct-17_-_05-Nov-17_Laden_Import' : 'Forecast'})
for3_sup_df = for3_sup_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 

for4_sup_df = pd.read_excel('ForecastedSUPPLYasOf9Oct.xlsx', dtype='unicode')
for4_sup_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for4_sup_df)
for4_sup_df= for4_sup_df.rename(columns={'Week_3_30-Oct-17_-_05-Nov-17_Laden_Import' : 'Forecast'})
for4_sup_df= for4_sup_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


act_sup_for1_df = pd.merge(act_sup_df, for1_sup_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_for1_df ['Weeks Forecast Before'] = "1"


act_sup_for2_df = pd.merge(act_sup_df, for2_sup_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_for2_df ['Weeks Forecast Before'] = "2"

act_sup_for3_df = pd.merge(act_sup_df, for3_sup_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_for3_df ['Weeks Forecast Before'] = "3"

act_sup_for4_df = pd.merge(act_sup_df, for4_sup_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_for4_df ['Weeks Forecast Before'] = "4"

act_sup_for_df = pd.concat([act_sup_for1_df, act_sup_for2_df, act_sup_for3_df, act_sup_for4_df])


act_sup_for_df [['Forecast']] = act_sup_for_df [['Forecast']].astype(float)

act_sup_for_df ['Forecast'] = act_sup_for_df ['Forecast'].fillna(0)
act_sup_for_df.columns.values[0] = "Demand Supply Type"

print(act_sup_for_df.columns)
act_sup_for_df.to_csv("act_sup_for_df.csv" , index=False)

# End SUPPLY laden Import part 

# Start SUPPLY Empty Import part 



act_sup_emp_df = pd.read_excel('Actuals_for_30Oct_to_5Nov_2017.xlsx', sheetname="Actual Supply from Empty" , dtype='unicode')
sf.df_format_col_name(act_sup_emp_df)
act_sup_emp_df = act_sup_emp_df.rename(columns={'Atloc_Code' : 'Fac', 'Count': 'Actual', 'Eqp_Typ': 'Eqp_Type', 'Gof_Code':'Gof'})


for1_sup_emp_df = pd.read_excel('ForecastedSUPPLYasOf30Oct.xlsx', dtype='unicode')
for1_sup_emp_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for1_sup_emp_df)
for1_sup_emp_df = for1_sup_emp_df.rename(columns={'Current_30-Oct-17_-_05-Nov-17_Empty' : 'Forecast'})
for1_sup_emp_df = for1_sup_emp_df [['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


#df[1].fillna(0, inplace=True)
for2_sup_emp_df = pd.read_excel('ForecastedSUPPLYasOf23Oct.xlsx', dtype='unicode')
for2_sup_emp_df .rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for2_sup_emp_df )
for2_sup_emp_df = for2_sup_emp_df .rename(columns={'Week_1_30-Oct-17_-_05-Nov-17_Empty' : 'Forecast'})
for2_sup_emp_df = for2_sup_emp_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


#df[1].fillna(0, inplace=True)
for3_sup_emp_df = pd.read_excel('ForecastedSUPPLYasOf16Oct.xlsx', dtype='unicode')
for3_sup_emp_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for3_sup_emp_df)
for3_sup_emp_df = for3_sup_emp_df.rename(columns={'Week_2_30-Oct-17_-_05-Nov-17_Empty' : 'Forecast'})
for3_sup_emp_df = for3_sup_emp_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


for3_sup_emp_df = pd.read_excel('ForecastedSUPPLYasOf16Oct.xlsx', dtype='unicode')
for3_sup_emp_df .rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for3_sup_emp_df)
for3_sup_emp_df = for3_sup_emp_df.rename(columns={'Week_2_30-Oct-17_-_05-Nov-17_Empty' : 'Forecast'})
for3_sup_emp_df = for3_sup_emp_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 

for4_sup_emp_df = pd.read_excel('ForecastedSUPPLYasOf9Oct.xlsx', dtype='unicode')
for4_sup_emp_df.rename(columns=lambda x: x.replace('\r\n', ''), inplace=True)
sf.df_format_col_name(for4_sup_emp_df)
for4_sup_emp_df= for4_sup_emp_df.rename(columns={'Week_3_30-Oct-17_-_05-Nov-17_Empty' : 'Forecast'})
for4_sup_emp_df= for4_sup_emp_df[['Forecast', 'Eqp_Type', 'Fac', 'Gof']]; 


act_sup_emp_for1_df = pd.merge(act_sup_emp_df, for1_sup_emp_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_emp_for1_df ['Weeks Forecast Before'] = "1"


act_sup_emp_for2_df = pd.merge(act_sup_emp_df, for2_sup_emp_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_emp_for2_df ['Weeks Forecast Before'] = "2"

act_sup_emp_for3_df = pd.merge(act_sup_emp_df, for3_sup_emp_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_emp_for3_df ['Weeks Forecast Before'] = "3"

act_sup_emp_for4_df = pd.merge(act_sup_emp_df, for4_sup_emp_df, how='left', left_on=['Eqp_Type', 'Fac', 'Gof'], right_on= ['Eqp_Type', 'Fac', 'Gof'])  
act_sup_emp_for4_df ['Weeks Forecast Before'] = "4"

act_sup_emp_for_df = pd.concat([act_sup_emp_for1_df, act_sup_emp_for2_df, act_sup_emp_for3_df, act_sup_emp_for4_df])


print(act_sup_emp_for_df.columns)
act_sup_emp_for_df.to_csv("act_sup_emp_for_df.csv" , index=False)

#print(act_for1_df)


act_sup_emp_for_df = pd.concat([act_sup_emp_for1_df, act_sup_emp_for2_df, act_sup_emp_for3_df, act_sup_emp_for4_df])
act_sup_emp_for_df.columns.values[0] = "Demand Supply Type"
act_sup_emp_for_df [['Forecast']] = act_sup_emp_for_df [['Forecast']].astype(float)
act_sup_emp_for_df ['Forecast'] = act_sup_emp_for_df ['Forecast'].fillna(0)



eif_accuracy = pd.concat([act_for_df, act_sup_for_df, act_sup_emp_for_df])
eif_accuracy.to_csv("eif_accuracy.csv" , index=False)


'''
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
'''