# -*- coding: utf-8 -*-
# reposition prg
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

xx4) Crosstab
xx7) Excel formatting
xx# Check whether 3 months data exists for 3J or not
#9) Kline - line conversion
# Excluded MOL TRN TXN - check lifting again 
# Weekly average is required 
# TEU to Count conversion
# Provide weekly average for each line
# convert kline line code
"""

import pandas as pd

import os
import time

def merge_files():
    m_repo = pd.read_excel('MOL Empty Move (201701-12).xls', 'data', index_col=None)
    k_repo = pd.read_excel('KL_empty_BL_data.xlsx', 'Sheet1', index_col=None)
    n_repo = pd.read_excel('NYK_Actual EP repositioning(Inter ECC)_201701_201712.xlsx', 'Actual EP repositioning(Inter E', index_col=None)
    
    # Process MOL 
    print(m_repo.columns)
    m_repo= m_repo[(m_repo['EQP XNS Code'] == 'LDG')]    
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

    df_line_map = pd.read_excel('Lane Group.xlsx', 'Kline', index_col=None)
    
    k_repo = pd.merge(k_repo, df_line_map, how='left', left_on=['SERVICE_CD'], right_on= ['Line'])  
    
    k_repo  = k_repo.rename(columns={'Line': 'LINE'})
    
    
    k_repo["Line_Calc"] = k_repo["Line_Legacy"].fillna(k_repo["SERVICE_CD"])
    
    #k_repo = k_repo[['POLMonth', 'TYPE', 'TEU', 'POL', 'POD', 'Line', 'ETDFrom']];
    k_repo = k_repo[['YYYY/MM/DD', 'TpSz', 'TEU_count', 'POL_LOCATION_CD', 'POD_LOCATION_CD', 'Line_Calc', 'WK']];
    #print(m_repo.columns)
    
    #BL_TYPE_CD	SERVICE_CD	VESSEL_CD	VOYAGE_NUM	LEG_CD	POL_LOCATION_CD	POD_LOCATION_CD	YYYY/MM/DD	WK	DELETED_FLG	BL_OK_FLG	TpSz	Unit_count	TEU_count

    
    #k_repo = k_repo[['POLMonth', 'TYPE', 'TEU', 'POL', 'POD', 'Line']];
    
    k_repo = k_repo.rename(columns={'YYYY/MM/DD': 'MONTH', 'WK': 'WEEK', 'TpSz': 'EQP_TYPE', 'TEU_count': 'TOTAL_TEU', 'POL_LOCATION_CD': 'FROM_LOCATION', 'POD_LOCATION_CD':'TO_LOCATION', 'Line_Calc': 'LINE'})
    
    k_repo [['WEEK']] = k_repo [['WEEK']].astype(str)
        
    k_repo['EQP_TYPE'] = k_repo['EQP_TYPE'].apply(get_ONE_eqp_type)
    
   
    
    k_repo['WEEK'] = k_repo['WEEK'].apply(lambda x: ('0' + x) if len(x) == 1 else x)
    
    
    k_repo['WEEK'] = '2017' + k_repo['WEEK']
    
    k_repo['MONTH'] = k_repo['MONTH'].apply(lambda x:x.strftime('%Y%m'))
    k_repo['CARRIER'] = 'K'
    
    print(k_repo['MONTH'].value_counts())
    
    print(k_repo.head())
    # Process KLINE
    
    
    # Process NYK
    print(n_repo.columns)
    # Keep mother vessel repositioning only
    n_repo= n_repo[(n_repo['Trans Mode'] == 'MV')]
    n_repo.shape
    
    n_repo = n_repo[['From ECC Date', 'Type-Size', 'Qty', 'From ECC', 'To ECC', 'Loading VVD']];
    
    n_repo = n_repo.rename(columns={'From ECC Date': 'MONTH', 'Type-Size': 'EQP_TYPE', 'Qty': 'TOTAL_TEU', 'From ECC': 'FROM_LOCATION', 'To ECC':'TO_LOCATION', 'Loading VVD': 'LINE'})
    
    print(n_repo.columns)
    
    n_repo['WEEK'] = n_repo['MONTH'].apply(lambda x:x.strftime('%Y%W'))
    
    
    n_repo['MONTH'] = n_repo['MONTH'].apply(lambda x:x.strftime('%Y%m'))
   
    #n_repo.to_csv('before.csv')
    grp_by_key = ['MONTH', 'WEEK', 'EQP_TYPE','FROM_LOCATION','TO_LOCATION','LINE']
    n_repo= n_repo.groupby(by=grp_by_key, as_index=False, sort=True).agg({'TOTAL_TEU': 'sum'})
    #n_repo.to_csv('after.csv')
    
    n_repo.LINE = n_repo.LINE.str.slice(0, 3)
    #n_repo.to_csv('after_slice.csv')
    
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
    elif p_legacy_eqp_type == '20DRY86':
       return 'D2'   
    elif p_legacy_eqp_type == '40DRY86':
       return 'D4'       
    elif p_legacy_eqp_type == '40DRY96':
       return 'D5'
    elif p_legacy_eqp_type ==  '45DRY96':
       return 'D7'     
    elif p_legacy_eqp_type == '20RFR86':
       return 'R2'       
    elif p_legacy_eqp_type == '40RFR86':
       return 'R5'
    elif p_legacy_eqp_type == '40RFR96':
       return 'R5'     
    elif p_legacy_eqp_type == '20OTP86':
       return 'O2'
    elif p_legacy_eqp_type == '40OTP86':
       return 'O4'       
    elif p_legacy_eqp_type == '20FLR86':
       return 'F2'
    elif p_legacy_eqp_type == '40FLR86':
       return 'F4'       
    elif p_legacy_eqp_type == '40PLW96':
       return 'P5'   
    elif p_legacy_eqp_type == '45PLW96':
       return 'P7'    
    else:    
       raise ValueError('Cannot convert to ONE eqp type:' + p_legacy_eqp_type)

def TEU2Cnt(p_eqp_typ):    
    if p_eqp_typ == 'D2' : return 1  
    elif p_eqp_typ == 'D4' : return 2
    elif p_eqp_typ == 'D5' : return 2
    elif p_eqp_typ == 'D7' : return 2
    elif p_eqp_typ == 'R2' : return 1
    elif p_eqp_typ == 'R5' : return 2
    elif p_eqp_typ == 'R7' : return 2
    elif p_eqp_typ == 'R8' : return 2
    elif p_eqp_typ == 'O2' : return 1
    elif p_eqp_typ == 'O4' : return 2
    elif p_eqp_typ == 'O5' : return 2
    elif p_eqp_typ == 'F2' : return 1
    elif p_eqp_typ == 'F4' : return 2
    elif p_eqp_typ == 'F5' : return 2
    elif p_eqp_typ == 'P5' : return 2
    elif p_eqp_typ == 'P7' : return 2
    elif p_eqp_typ == 'T2' : return 1
    elif p_eqp_typ == 'T4' : return 2
    elif p_eqp_typ == 'TA' : return 1
    elif p_eqp_typ == 'OB' : return 2
    elif p_eqp_typ == 'TB' : return 2
    elif p_eqp_typ == 'FA' : return 2
    elif p_eqp_typ == 'FB' : return 2
    elif p_eqp_typ == 'DB' : return 2
    elif p_eqp_typ == 'OA' : return 1
    elif p_eqp_typ == 'DA' : return 1
    elif p_eqp_typ == 'FA' : return 1
    else: return 1
  
print(os.getcwd())

from_date = 201735 # Sep, Oct, Nov
to_date = 201748

working_dir = r"C:\BI\python_scripts\repo_guideline"
os.chdir(working_dir)

#merge_files()
# Location code conversion to ONE code
df = pd.read_excel('repo_guide.xlsx', 'Sheet1', index_col=None)

# Unify date range for 3J
df['WEEK'] = df['WEEK'].astype(int)
df= df[(df['WEEK'] >= from_date)]
df= df[(df['WEEK'] <= to_date)]

#df['WEEK'] = new_df['WEEK'].astype(str)

#df['MONTH'] = df['WEEK'].apply(lambda x:x.strftime('%Y%m'))

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



#print(new_df.MONTH.value_counts())
# end -- Unify date range for 3J

# End - Location code conversion to ONE code
# Line code conversion to ONE code
df_line_map = pd.read_excel('Lane Group.xlsx', 'Sheet1', index_col=None)
new_df = pd.merge(new_df, df_line_map, how='left', left_on=['LINE'], right_on= ['Service Code_3J'])

new_df = new_df.rename(columns={'Service Code_ONE': 'ONE_LINE'})


new_df = new_df[['CARRIER', 'WEEK', 'EQP_TYPE', 'TOTAL_TEU', 'LINE',  
                 'ONE_FROM_LOCATION',	'ONE_FROM_LOCATION_DSCR', 'ONE_FROM_LOCATION_CTRY',
				 'ONE_TO_LOCATION',	'ONE_TO_LOCATION_DSCR', 'ONE_TO_LOCATION_CTRY',
                 'ONE_LINE']];
print(new_df.columns)
new_df.to_csv('before_unit_cnt.csv')
        
new_df['UNIT_CNT'] = new_df['TOTAL_TEU']/new_df['EQP_TYPE'].apply(TEU2Cnt)

#new_df['WEEK'] = new_df['WEEK'].astype(int)

#new_df.loc[new_df['CARRIER'] == 'M', "WEEK"] = new_df['WEEK'] +2


#data.loc[data['id'] > 2000, "first_name"] = "John"

current_milli_time = int(round(time.time() * 1000))
file_name = 'repo_guide_' + str(current_milli_time) + '.csv'
new_df.to_csv(file_name, index=False)



print('DONE.................................................................')
