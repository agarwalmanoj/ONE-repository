# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

xx1) Merge 3 files
2) Filter only Asia data
xx2) Unify date range for 3J
xx3) Location code conversion to ONE code
xx5) Line code conversion to ONE code
xx6) Handle VVD error - no such problem in NYK data

xx8) Find conversion do not exists for which line 

4) Crosstab
xx7) Excel formatting
xx# Check whether 3 months data exists for 3J or not
#9) Kline - line conversion
# Excluded MOL TRN TXN - check lifting again 

"""

import pandas as pd
import os
import time

def merge_files():
    m_repo = pd.read_excel('MOL Empty Move (201701-12).xls', 'data', index_col=None)
    k_repo = pd.read_excel('Empty flow including inland for Asia, Africa and Australia.xlsx', 'Sheet1', index_col=None)
    n_repo = pd.read_excel('NYK_Actual EP repositioning(Inter ECC)_201701_201712.xlsx', 'Actual EP repositioning(Inter E', index_col=None)
    
    # Process MOL 
    print(m_repo.columns)
    m_repo= m_repo[(m_repo['EQP XNS Code'] == 'TRN')]    
    m_repo = m_repo[['Gains Month', 'EQP Type ONE', 'TEU SUM', 'AT Location GOF', 'To Location', 'Line Code', 'Gains Week']];
    
    print(m_repo.columns)
    
    m_repo = m_repo.rename(columns={'Gains Month': 'MONTH', 'Gains Week': 'WEEK', 'EQP Type ONE': 'EQP_TYPE', 'TEU SUM': 'TOTAL_TEU', 'AT Location GOF': 'FROM_LOCATION', 'To Location':'TO_LOCATION', 'Line Code': 'LINE'})
    m_repo.FROM_LOCATION = m_repo.FROM_LOCATION.str.slice(0, 5)
    m_repo.TO_LOCATION = m_repo.TO_LOCATION.str.slice(0, 5)
    m_repo['CARRIER'] = 'M'
    print(m_repo.head())
    # Process MOL 
    
    # Process KLINE
    print(k_repo.columns)
    
    k_repo = k_repo[['POLMonth', 'TYPE', 'TEU', 'POL', 'POD', 'Line', 'ETDFrom']];
    #print(m_repo.columns)
    
    k_repo = k_repo[['POLMonth', 'TYPE', 'TEU', 'POL', 'POD', 'Line']];
    
    k_repo = k_repo.rename(columns={'POLMonth': 'MONTH', 'TYPE': 'EQP_TYPE', 'TEU': 'TOTAL_TEU', 'POL': 'FROM_LOCATION', 'POD':'TO_LOCATION', 'Line': 'LINE'})
    
    k_repo['EQP_TYPE'] = k_repo['EQP_TYPE'].apply(get_ONE_eqp_type)
    
    k_repo['WEEK'] = k_repo['MONTH'].apply(lambda x:x.strftime('%Y%w'))
    k_repo['MONTH'] = k_repo['MONTH'].apply(lambda x:x.strftime('%Y%m'))
    k_repo['CARRIER'] = 'K'
    
    print(k_repo.head())
    # Process KLINE
    
    
    # Process NYK
    print(n_repo.columns)
    # Keep mother vessel repositioning only
    n_repo= n_repo[(n_repo['Trans Mode'] == 'MV')]
    n_repo = n_repo[['From ECC Date', 'Type-Size', 'Qty', 'From ECC', 'To ECC', 'Loading VVD']];
    
    n_repo = n_repo.rename(columns={'From ECC Date': 'MONTH', 'Type-Size': 'EQP_TYPE', 'Qty': 'TOTAL_TEU', 'From ECC': 'FROM_LOCATION', 'To ECC':'TO_LOCATION', 'Loading VVD': 'LINE'})
    
    print(n_repo.columns)
    
    n_repo['WEEK'] = n_repo['MONTH'].apply(lambda x:x.strftime('%Y%w'))
    n_repo['MONTH'] = n_repo['MONTH'].apply(lambda x:x.strftime('%Y%m'))
    grp_by_key = ['MONTH', 'WEEK', 'EQP_TYPE','FROM_LOCATION','TO_LOCATION','LINE']
    n_repo= n_repo.groupby(by=grp_by_key, as_index=False, sort=True).agg({'TOTAL_TEU': 'sum'})
    n_repo.LINE = m_repo.LINE.str.slice(0, 3)
    n_repo['CARRIER'] = 'N'    
    df = pd.concat([m_repo,n_repo,k_repo])    
    print(df.head())
    
    df['CARRIER_FROM_LOCATION'] = df['CARRIER'] + df['FROM_LOCATION']
    df['CARRIER_TO_LOCATION'] = df['CARRIER'] + df['TO_LOCATION']
     
    writer = pd.ExcelWriter('repo_guide.xlsx')
    df.to_excel(writer,'Sheet1',index=False)
    writer.save()
    
def get_ONE_eqp_type(p_legacy_eqp_type):    
    if p_legacy_eqp_type == '20DC':
       return 'D2'
    elif p_legacy_eqp_type ==  '20RF':
       return 'R2'
    elif p_legacy_eqp_type ==  '40DC':
       return 'D4'
    elif p_legacy_eqp_type ==  '45HC':
       return 'D7'
    elif p_legacy_eqp_type ==  'HCDC':
       return 'D5'
    elif p_legacy_eqp_type == 'HCRF':
       return 'R5'   
    else:    
       raise ValueError('Cannot convert to ONE eqp type:' + p_legacy_eqp_type)

  
print(os.getcwd())

from_date = 201709 # Sep, Oct, Nov
to_date = 201711

working_dir = r"C:\BI\python_scripts\repo_guideline"
os.chdir(working_dir)

#merge_files()
# Location code conversion to ONE code
df = pd.read_excel('repo_guide.xlsx', 'Sheet1', index_col=None)
df_lcn_map = pd.read_excel('KMN Location_Yard Conversion table_ver.3.3_20171101.xlsx', 'Location (0122)', index_col=None)

new_df = pd.merge(df, df_lcn_map, how='left', left_on=['CARRIER_FROM_LOCATION'], right_on= ['Carrier+Legacy Location Code'])  
new_df = new_df[['CARRIER', 'MONTH', 'WEEK', 'EQP_TYPE', 'TOTAL_TEU', 'FROM_LOCATION', 'TO_LOCATION', 'LINE', 'CARRIER_FROM_LOCATION', 'CARRIER_TO_LOCATION', 'ONE Loc Code',	'ONE Description', 'Country Name']];
new_df = new_df.rename(columns={'ONE Loc Code': 'ONE_FROM_LOCATION', 'ONE Description': 'ONE_FROM_LOCATION_DSCR', 'Country Name': 'ONE_FROM_LOCATION_CTRY'})

new_df = pd.merge(new_df, df_lcn_map, how='left', left_on=['CARRIER_TO_LOCATION'], right_on= ['Carrier+Legacy Location Code'])  
new_df = new_df[['CARRIER', 'MONTH', 'WEEK', 'EQP_TYPE', 'TOTAL_TEU', 'FROM_LOCATION', 'TO_LOCATION', 'LINE', 'CARRIER_FROM_LOCATION', 'CARRIER_TO_LOCATION', 
                 'ONE_FROM_LOCATION',	'ONE_FROM_LOCATION_DSCR', 'ONE_FROM_LOCATION_CTRY',
                 'ONE Loc Code',	'ONE Description', 'Country Name']];


new_df = new_df.rename(columns={'ONE Loc Code': 'ONE_TO_LOCATION', 'ONE Description': 'ONE_TO_LOCATION_DSCR', 'Country Name': 'ONE_TO_LOCATION_CTRY'})

new_df = new_df[['CARRIER', 'MONTH', 'WEEK', 'EQP_TYPE', 'TOTAL_TEU', 'LINE', 
                 'ONE_FROM_LOCATION',	'ONE_FROM_LOCATION_DSCR', 'ONE_FROM_LOCATION_CTRY',
                 'ONE_TO_LOCATION',	'ONE_TO_LOCATION_DSCR', 'ONE_TO_LOCATION_CTRY']];


# Unify date range for 3J
new_df= new_df[(new_df['MONTH'] >= from_date)]
new_df= new_df[(new_df['MONTH'] <= to_date)]

print(new_df.MONTH.value_counts())
# end -- Unify date range for 3J

# End - Location code conversion to ONE code
# Line code conversion to ONE code
df_line_map = pd.read_excel('Lane Group.xlsx', 'Sheet1', index_col=None)
new_df = pd.merge(new_df, df_line_map, how='left', left_on=['LINE'], right_on= ['Service Code_3J'])  

new_df = new_df.rename(columns={'Service Code_ONE': 'ONE_LINE'})


new_df = new_df[['CARRIER', 'MONTH', 'WEEK', 'EQP_TYPE', 'TOTAL_TEU', 'LINE',  
                 'ONE_FROM_LOCATION',	'ONE_FROM_LOCATION_DSCR', 'ONE_FROM_LOCATION_CTRY',
				 'ONE_TO_LOCATION',	'ONE_TO_LOCATION_DSCR', 'ONE_TO_LOCATION_CTRY',
                 'ONE_LINE']];
print(new_df.columns)
current_milli_time = int(round(time.time() * 1000))
file_name = 'repo_guide_' + str(current_milli_time) + '.csv'
new_df.to_csv(file_name, index=False)



print('DONE.................................................................')
