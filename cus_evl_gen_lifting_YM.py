# YM - Lifting data is joined with other data (dso, cancellation, stay days) in Python and scored

# TODO:
# ['Customer_Stay_Days', 'Equipment_Group', 'Main_Contract_Customer', 'Main_Contract_Customer_Name', 'Trade_Dominance', 'Trade_Mgt_Group']
# ['Bl_No', 'Bound', 'Caso', 'Cns_Name', 'Contract_No_12D', 'Cust_Attribution', 'Cust_Grp_Name', 'Cust_Sub_Grp_Name', 'Customer_Segregation', 
# 'Customer_Segregation_Fmc', 'Customer_Segregation_Status', 'Dest_Country', 'Dest_Facility', 'Dest_Gof_Code', 'Dest_Roc_Code', 'Dry_Live_Rad', 
# 'Dsch_Port1', 'Dsch_Port2', 'Dsch_Port3', 'Dsch_Port4', 'Dsch_Port5', 'Equipment_Group', 'Equipment_Size', 'Line_Code', 'Line_Code1', 'Line_Code2', 
# 'Line_Code3', 'Line_Code4', 'Line_Code5', 'Load_Port1', 'Load_Port2', 'Load_Port3', 'Load_Port4', 'Load_Port5', 'Main_Cust_Code', 'Main_Cust_Name', 
# 'Mol_Cmd_Dscr', 'Month', 'N1P_Name', 'Named_Cust_Name', 'Npi_Indicator', 'Onboard_Dt', 'Orig_Country', 'Orig_Facility', 'Orig_Gof_Code', 'Orig_Roc_Code', 
# 'Ppd_Col', 'Ref_As_Dry_Flg', 'Rhq', 'Rli_Eff_Dt', 'Rli_Exp_Dt', 'Shp_Name', 'Teu', 'Teus_Void', 'Total_Freight', 'Total_Np', 'Total_Npi', 'Total_Ofr', 
# 'Total_Wt', 'Trade_Dominant_Flg', 'Trade_Group', 'Transit_Time', 'Week_No', 'Weight_Indicator']

# Stay days - ['Customer_Stay_Days', 'Equipment_Group', 'Main_Contract_Customer', 'Main_Contract_Customer_Name', 'Trade_Dominance', 'Trade_Mgt_Group']

# DSO - ['Eqp_Group', 'Inv_Amt_Total', 'Main_Code', 'Main_Name', 'Os_Amt_Total', 'Trade_Dominant_Id', 'Trade_Group']
    
import pandas as pd
from scipy import stats
import os
import shared_functions as sf

# function area start 
# following normal distribution
def get_percentile_score(p_percentile):    
    if p_percentile >= 85:
       return 5
    elif p_percentile >= 65:
       return 4
    elif p_percentile >= 35:
       return 3
    elif p_percentile >= 15:
       return 2   
    else:    
       return 1    

def get_wt_per_teu_score(p_wt_per_teu):
    
    if p_wt_per_teu  < 5:
       return 5
    elif p_wt_per_teu < 10:
       return 4
    elif p_wt_per_teu < 15:
       return 3
    elif p_wt_per_teu < 20:
       return 2   
    else:    
       return 1

def get_dso_score(p_dso):    
    if p_dso  < 7: # IF p_dso is 0, then give score 5. Assuming cash customer or credit customer with no outstanding.    
       return 5
    elif p_dso  < 14:
       return 4
    elif p_dso  < 21:
       return 3
    elif p_dso  < 28:
       return 2   
    else:    
       return 1

def get_cancel_score(p_cancel_percentage):    
    if p_cancel_percentage < .05: # IF p_dso is 0, then give score 5. Assuming cash customer or credit customer with no outstanding.    
       return 5
    elif p_cancel_percentage < .1:
       return 4
    elif p_cancel_percentage < .2:
       return 3
    elif p_cancel_percentage < .25:
       return 2   
    else:    
       return 1

def get_stay_days_score(p_stay):
    if p_stay  < 7: # If p_dso is 0, then give score 5. Assuming cash customer or credit customer with no outstanding.
       return 5
    elif p_stay  < 14:
       return 4
    elif p_stay  < 21:
       return 3
    elif p_stay  < 28:
       return 2   
    else:    
       return 1
 
def get_stability_score(p_lifting_mth):    
    if p_lifting_mth >= 11:
       return 5
    elif p_lifting_mth >= 9:
       return 4
    elif p_lifting_mth >= 6:
       return 3
    elif p_lifting_mth >= 3:
       return 2   
    else:    
       return 1    
   
    
    
# function area end
   
print(os.getcwd())
#lifting_df = pd.read_csv("NPS_DATA.csv")
#lift_df = pd.read_csv("NPS_DATA-000.csv", dtype='unicode')
print('before reading csv')

working_dir = r"D:\BI\dashboards\Customer Ranking" 
os.chdir(working_dir)


#lift_df = pd.read_csv("cus_evl_lifting_trade_mgt.csv", dtype='unicode')

#lift_df = pd.read_csv("cus_evl_lifting_trade_mgt.csv", encoding = 'utf8', dtype='unicode')
print ('cus_evl_lifting_trade_mgt-000 before')
#lift_df = pd.read_csv("cus_evl_lifting_trade_mgt-000.csv", encoding = "ISO-8859-1", dtype='unicode')
lift_df = pd.read_csv("cus_evl_lifting_trade_mgt.csv", encoding = "ISO-8859-1", dtype='unicode')
print ('cus_evl_lifting_trade_mgt-000 done')

dso_df = pd.read_csv("cus_evl_dso.csv", encoding = "ISO-8859-1", dtype='unicode')
print ('dso read done')
bkg_df = pd.read_csv("cus_evl_cancellation.csv", encoding = "ISO-8859-1", dtype='unicode')
print ('bkg read done')
stay_df = pd.read_csv("cus_evl_stay_days.csv", encoding = "ISO-8859-1", dtype='unicode')
print ('stay days read done')

sf.df_format_col_name(dso_df)
sf.df_format_col_name(bkg_df)
sf.df_format_col_name(stay_df) # ['Customer_Stay_Days', 'Equipment_Group', 'Main_Contract_Customer', 'Main_Contract_Customer_Name', 'Trade_Dominance', 'Trade_Mgt_Group']


print('after reading csv')


lift_df[['Total_Wt','Teu','Total_Npi', 'Transit_Time']] = lift_df[['Total_Wt','Teu','Total_Npi', 'Transit_Time']].astype(float)

# Start Form df to get the stability score
lift_df_4_stability = lift_df
lift_df_4_stability = lift_df[['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group_Rad', 'Month']]; 
key = ['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group_Rad']
lift_df_4_stability = lift_df_4_stability.groupby(by=key, as_index=False, sort=True).agg({'Month': pd.Series.nunique}) # Need distinct count so not using count
lift_df_4_stability['Stability_Score'] = lift_df_4_stability['Month'].apply(get_stability_score)


#tmp= lift_df[['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group', 'Month', 'Teu']];
#key = ['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group', 'Month']
#tmp= tmp.groupby(by=key, as_index=False, sort=True).agg({'Teu': 'sum'})
#tmp[(tmp['Cust_Grp_Name'] == 'ZWANENBERG FOOD GROUP')]


#lift_df_4_stability[(lift_df_4_stability['Cust_Grp_Name'] == 'ZWANENBERG FOOD GROUP')]


# Form df to get the stability score End


df = lift_df[['Main_Cust_Code', 'Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group',  'Equipment_Group_Rad', 'Teu', 'Total_Npi', 'Total_Wt']];
# group & summarize "lifting" data only on required columns
key = ['Main_Cust_Code', 'Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group', 'Equipment_Group_Rad']
df = df.groupby(by= key, as_index=False, sort=True).agg({'Teu': 'sum', 'Total_Npi': 'sum', 'Total_Wt': 'sum'})
#df = df[(df['Teu'] >= 100)] # exclude customers with lifting less than 100 TEUS

#Merge with DSO
#grp_by_cols = ['Cust_Grp_Name', 'Trade_Group','Main_Cust_Code', 'Equipment_Group', 'Trade_Dominant_Flg', 'Trade_Group','Main_Code', 'Eqp_Group', 'Trade_Dominant_Id']
#new_df = pd.merge(df, dso_df, how='left', left_on=['Trade_Group','Main_Cust_Code', 'Equipment_Group', 'Trade_Dominant_Flg' ], right_on= ['Trade_Group','Main_Code', 'Eqp_Group', 'Trade_Dominant_Id']).groupby(grp_by_cols).agg({'Teu': 'sum', 'Total_Npi': 'sum', 'Total_Wt': 'sum', 'Inv_Amt_Total': 'sum', 'Os_Amt_Total': 'sum'})  # TODO: change columns

# Merging & Grouping in different steps
# Merging
new_df = pd.merge(df, dso_df, how='left', left_on=['Trade_Group','Main_Cust_Code', 'Equipment_Group', 'Trade_Dominant_Flg' ], right_on= ['Trade_Group','Main_Code', 'Eqp_Group', 'Trade_Dominant_Id'])  
print('Merge with stay days')
new_df = pd.merge(new_df, stay_df, how='left', left_on=['Trade_Group','Main_Cust_Code', 'Equipment_Group', 'Trade_Dominant_Flg' ], right_on= ['Trade_Mgt_Group','Main_Contract_Customer', 'Equipment_Group', 'Trade_Dominance'])  
#new_df= new_df[['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group',  'Teu', 'Total_Npi', 'Total_Wt', 'Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days']]
print(sorted(new_df.columns))
print(sorted(bkg_df.columns))
print('Merge with bkg')
#Main_Customer_Name	Main_Cust_Code	Equipment_Group	Trade_Group	Trade_Dominance	Booked_Teus	Cancelled_Teus	
#bkg_df = bkg_df.rename(columns={'Trade_Group': 'Bkg_Trade_Group'})
new_df = pd.merge(new_df, bkg_df, how='left', left_on=['Trade_Group','Main_Cust_Code', 'Equipment_Group', 'Trade_Dominant_Flg' ], right_on= ['Trade_Group','Main_Cust_Code', 'Equipment_Group', 'Trade_Dominance'])
print('After Merge with bkg')
new_df= new_df[['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group',  'Equipment_Group_Rad', 'Teu', 'Total_Npi', 'Total_Wt', 'Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days', 'Booked_Teus','Cancelled_Teus']]
print('After Merge with bkg2')
new_df[['Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days', 'Booked_Teus','Cancelled_Teus']] = new_df[['Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days', 'Booked_Teus','Cancelled_Teus']].astype(float)
print('After Merge with bkg3')
print(sorted(new_df.columns))

#new_df.to_csv("df_merged_dso_new2.csv")

#df = new_df.groupby(by= key, as_index=False, sort=True).agg({'Teu': 'sum', 'total_npi': 'sum', 'total_wt': 'sum', })

#Merge with stay days
# Merging
#['Customer_Stay_Days', 'Equipment_Group', 'Main_Contract_Customer', 'Main_Contract_Customer_Name', 'Trade_Dominance', 'Trade_Mgt_Group']

#new_df= new_df[['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group',  'Teu', 'Total_Npi', 'Total_Wt', 'Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days']];
#new_df[['Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days']] = new_df[['Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days']].astype(float)
#print(sorted(new_df.columns))

# Grouping is on Cust_Grp_Name and NOT on main_cust_code, so their will be fewer records than in lifting. 
key = ['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group', 'Equipment_Group_Rad']
new_df= new_df.groupby(by= key, as_index=False, sort=True).agg({'Teu': 'sum', 'Total_Npi': 'sum', 'Total_Wt': 'sum', 'Inv_Amt_Total': 'sum', 'Os_Amt_Total': 'sum', 'Customer_Stay_Days': 'mean',  'Booked_Teus':'sum','Cancelled_Teus':'sum'})
new_df[['Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days', 'Booked_Teus','Cancelled_Teus']] = new_df[['Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days', 'Booked_Teus','Cancelled_Teus']].astype(float)

key = ['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group_Rad']
# merge with stability_df based on Cust_Grp_Name and not on main_cust_code
new_df = pd.merge(new_df, lift_df_4_stability, how='left', left_on=key, right_on=key)
new_df= new_df[['Cust_Grp_Name', 'Trade_Group', 'Trade_Dominant_Flg','Equipment_Group',  'Equipment_Group_Rad', 'Teu', 'Total_Npi', 'Total_Wt', 'Inv_Amt_Total', 'Os_Amt_Total', 'Customer_Stay_Days', 'Booked_Teus','Cancelled_Teus', 'Stability_Score', 'Month']]

print('After Merge with stability')
print(sorted(new_df.columns))


df = new_df;

df['Npi_Per_Teu'] = df['Total_Npi']//df['Teu']
df['Total_Wt'] = df['Total_Wt']/1000 # convert to ton 
df['Weight_Per_Teu'] = df['Total_Wt']//df['Teu']
#df.groupby(by= key, as_index=False, sort=True).agg({'Teu': 'sum'})

#df['Volume_Percentile'] = stats.rankdata(df['Teu'], "max")/len(df['Teu'])

#grp.apply(lambda x: stats.rankdata(df[x], "max")/len(df[x]))
df2 = pd.DataFrame()
key = ['Trade_Group', 'Trade_Dominant_Flg','Equipment_Group_Rad'] # removed 'Cust.Grp.Name'

for name, group in df.groupby(key):
    #nm = name
    #print('name:')
    #print(name)
    #print(group)
    #name[0] # 'Trade_Group', 
    #name[1] # 'Trade_Dominant_Flg'
    #name[2] # 'Equipment_Group'    
    #temp_df = df[(df['Trade_Group'] == name[0]) & (df['Trade_Dominant_Flg']==name[1]) & (df['Equipment_Group']==name[2])]        
    
    # filtering for the group
    temp_df = df[(df['Trade_Group'] == name[0])]
    temp_df = temp_df[(temp_df['Trade_Dominant_Flg']==name[1])]
    temp_df = temp_df[(temp_df['Equipment_Group_Rad']==name[2])]
    
    
    temp_df['Volume_Rank_Dense'] = stats.rankdata(temp_df['Teu'], "dense")
    #temp_df['Volume_Percentile_Dense'] = stats.rankdata(temp_df['Teu'], "dense")/len(temp_df['Teu']) # test    
    #temp_df['Volume_Rank'] = stats.rankdata(temp_df['Teu'], "max")# test
    
    temp_df['Volume_Percentile'] = temp_df['Volume_Rank_Dense']/max(temp_df['Volume_Rank_Dense'])
    temp_df['Volume_Percentile'] = temp_df['Volume_Percentile']*100
    temp_df['Volume_Percentile']  = temp_df['Volume_Percentile'].astype(int)
    
    
    temp_df['Npi_Per_Teu_Rank_Dense'] = stats.rankdata(temp_df['Npi_Per_Teu'], "dense")
    temp_df['Npi_Per_Teu_Percentile'] = temp_df['Npi_Per_Teu_Rank_Dense']/max(temp_df['Npi_Per_Teu_Rank_Dense'])
    temp_df['Npi_Per_Teu_Percentile'] = temp_df['Npi_Per_Teu_Percentile']*100
    temp_df['Npi_Per_Teu_Percentile']  = temp_df['Npi_Per_Teu_Percentile'].astype(int)
    
    # add logic to get stabilit score
    #temp_df['Stability_Score']  =     
    
    df2 = df2.append(temp_df, ignore_index=True)    
    
    
    #print('group:')
    #print(group) 
    


#convert percentile to score
df2['Volume_Score'] = df2['Volume_Percentile'].apply(get_percentile_score)
df2['Npi_Per_Teu_Score'] = df2['Npi_Per_Teu_Percentile'].apply(get_percentile_score)
df2['Weight_Per_Teu_Score'] = df2['Weight_Per_Teu'].apply(get_wt_per_teu_score)


df2['DSO'] = round((df2['Os_Amt_Total']/df2['Inv_Amt_Total']) * 180, 2)

# IF p_dso is non existing, then assign 0 and give score 5. Assuming cash customer or credit customer with no outstanding.
df2['DSO'].fillna(0, inplace=True) 

print('before DSO_Score')  
df2['DSO_Score'] = df2['DSO'].apply(get_dso_score)


df2['DSO'] = round((df2['Os_Amt_Total']/df2['Inv_Amt_Total']) * 180, 2)

df2['Cancel_Percent'] = round((df2['Cancelled_Teus']/df2['Booked_Teus']), 2)
df2['Cancellation_Score'] = df2['Cancel_Percent'].apply(get_cancel_score)
    
df2['Stay_Days_Score'] = df2['Customer_Stay_Days'].apply(get_stay_days_score)
    
    
df2.to_csv("cus_evl_lifting_yield_mgt.csv", index=False)
print("Done")


    #temp_df = df.groupby(by= key, as_index=False)
    #type(temp_df)
    #temp_df['Volume_Percentile'] = stats.rankdata(df['Teu'], "max")/len(temp_df['Teu'])
    #print(temp_df)
    #break
    #df = df1.append([df2, df3])
    
    #final_df = 
    
    #df['Volume_Percentile'] = stats.rankdata(df['Teu'], "max")/len(df['Teu'])
    #print(name)
    # print the data of that regiment
    



# for each group set percentile

#df1.groupby(by=['A','C'],as_index=False,sort=True).agg({'B': lambda x: tuple(sum(x, []))})

#print(df.columns.values.tolist())
#print(df)


#df['Volume_Percentile'] = stats.rankdata(lift_df['Teu'], "max")/len(lift_df['Teu'])

#print(lift_df.describe())


#lift_df['wt_per_teu'] = (lift_df.total_wt/1000)/lift_df.Teu
#print('after wt_per_teu')
#wt_per_teu = lift_df['wt_per_teu']   
#df['newcolumn'] = df.apply(fab, axis=1)
#lift_df['wt_score'] = lift_df['wt_per_teu'].apply(get_wt_score)

#lift_df['Npi_Per_Teu'] = lift_df.total_npi/lift_df.Teu
#lift_df['npi_per_day'] = lift_df.total_npi/lift_df['Transit.Time']
# print(lift_df[['Npi_Per_Teu', 'npi_per_day']])
#print('after Transit.Time')
#lift_df['Npi_Per_Teu_Percentile'] = [stats.percentileofscore(lift_df['Npi_Per_Teu'], i) for i in lift_df['Npi_Per_Teu']]

#lift_df['Npi_Per_Teu_Percentile'] = stats.rankdata(lift_df['Npi_Per_Teu'], "max")/len(lift_df['Npi_Per_Teu'])
#lift_df['Npi_Per_Teu_Percentile'] = lift_df['Npi_Per_Teu_Percentile']*100
#print('after percentile 1')
#lift_df['npi_per_day_percentile'] = stats.rankdata(lift_df['npi_per_day'], "max")/len(lift_df['npi_per_day'])
#lift_df['npi_per_day_percentile'] = lift_df['npi_per_day_percentile']*100
#lift_df['npi_per_day_percentile'] = [stats.rankdata(lift_df['npi_per_day'], i) for i in lift_df['npi_per_day']]
#print('after percentile 2')





#lift_df['npi_per_day_score'] = lift_df['npi_per_day_percentile'].apply(get_percentile_score)

#print(lift_df['Npi_Per_Teu'].describe())
#print(lift_df['npi_per_day'].describe())

#lift_df['dso_score'] = randint(1,5)
#lift_df['dso_score'] = lift_df.apply (lambda row: randint(1,5), axis=1)  
#lift_df['cancellation_score'] = lift_df.apply (lambda row: randint(1,5), axis=1)  
#lift_df['Volume_Score'] = lift_df.apply (lambda row: randint(1,5), axis=1)  

#df['C'] = df.apply(lambda row: randint(1,5), axis=1)
# applying weighted average
#lift_df['Npi_Per_Teu_Score_wa'] = lift_df['Npi_Per_Teu_Score'] * 0.3
#lift_df['npi_per_day_score_wa'] = lift_df['npi_per_day_score'] * 0.3
#lift_df['Volume_Score_wa'] = lift_df['Volume_Score']  * 0.2
#lift_df['stay_score_wa'] = lift_df['Stay_Score'] * 0.15
#lift_df['cancellation_score_wa'] = lift_df['cancellation_score'] * 0.15
#lift_df['dso_score_wa'] = lift_df['dso_score'] * 0.1
#lift_df['wt_score_wa'] = lift_df['wt_score'] * 0.1


#lift_df['stay_score_wa'].fillna(0, inplace=True)

#print('stay_score-')

#print(nvl(lift_df['stay_score_wa']))

#total_score = lift_df['Volume_Score_wa'] + lift_df['stay_score_wa'] + lift_df['cancellation_score_wa'] + lift_df['dso_score_wa'] + lift_df['wt_score_wa']
#print(total_score)

#lift_df['total_score_by_Npi_Per_Teu'] =  total_score + nvl(lift_df['Npi_Per_Teu_Score_wa']) 
#lift_df['total_score_by_npi_per_day']=  total_score + nvl(lift_df['npi_per_day_score_wa'])

#print(lift_df[['Npi_Per_Teu', 'Npi_Per_Teu_Percentile', 'Npi_Per_Teu_Score']])
#print(lift_df[['npi_per_day', 'npi_per_day_percentile', 'npi_per_day_score']])
#print(df2)
