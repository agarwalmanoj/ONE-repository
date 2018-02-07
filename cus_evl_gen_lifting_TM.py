# TM - Lifting data is joined with other data in Tableau
import pandas as pd
import os
import datetime
import shared_functions as sf

def get_equipment_group_rad(p_row):    
    if p_row['Dry_Live_Rad'] == "RAD":
        return "RAD"
    elif p_row['Dry_Live_Rad'] == "LIVE":
        return "REEFER LIVE"    
    else:
        return p_row['Equipment_Group']
    
def exclude_small_customer(p_lift_df):        
    # Group on Cust_Grp_Name
    tmp_lift_df = p_lift_df
    tmp_lift_df = p_lift_df[['Cust_Grp_Name', 'Teu']]
    tmp_lift_df[['Teu']] = lift_df[['Teu']].astype(float)
    
    key = ['Cust_Grp_Name']
    tmp_lift_df = tmp_lift_df.groupby(by=key, as_index=False, sort=True).agg({'Teu': 'sum'})
    tmp_lift_df = tmp_lift_df[(tmp_lift_df['Teu'] < 100)]
    #tmp_lift_df = tmp_lift_df [['Cust_Grp_Name']]
    #print(type(tmp_lift_df))
    #print(tmp_lift_df)
    tmp_lift_df.to_csv("small_customer.csv", index=False, encoding="utf-8")
    p_lift_df = p_lift_df[~p_lift_df.Cust_Grp_Name.isin(tmp_lift_df.Cust_Grp_Name)]
    return p_lift_df


start_date = datetime.date(2016, 9, 1) # YMD - 01-Sep-2016

# r: Use a raw string, to make sure that Python doesn't try to interpret anything following a \ as an escape sequence.
working_dir = r"D:\BI\dashboards\Customer Ranking" 
os.chdir(working_dir)

# Merging the lifting files
print('before reading csv')
df0 = pd.read_csv("NPS DATA_20162H.csv", dtype='unicode')
df1 = pd.read_csv("NPS DATA_20171H.csv", dtype='unicode')
df2 = pd.read_csv("NPS DATA_20172H.csv", dtype='unicode')

#df0 = pd.read_csv("NPS DATA_20162H-000.csv", dtype='unicode')
#df1 = pd.read_csv("NPS DATA_20171H-000.csv", dtype='unicode')
#df2 = pd.read_csv("NPS DATA_20172H-000.csv", dtype='unicode')
lift_df = pd.concat([df0,df1,df2])
#print('after reading csv')

# replace space in column name with underscore
#print(sorted(lift_df.columns))
sf.df_format_col_name(lift_df)
lift_df = lift_df.rename(columns={'Item1': 'Teu'})
#print(sorted(lift_df.columns))


# filter data older than 12 months
lift_df['Onboard_Dt'] = pd.to_datetime(lift_df['Onboard_Dt'])
lift_df = lift_df[lift_df['Onboard_Dt'] >= start_date]

        
lift_df[['Total_Wt','Teu','Total_Npi']] = lift_df[['Total_Wt','Teu','Total_Npi']].astype(float)
lift_df[['Main_Cust_Code','Trade_Group']] = lift_df[['Main_Cust_Code','Trade_Group']].astype(str)

# remove rows with TEU less than 1 or cust.grp.name is blank

lift_df = lift_df[~lift_df.Cust_Grp_Name.isin(['.', '..',  '...', '-', 'n/a', 'tba'])]
lift_df = lift_df[(pd.notnull(lift_df['Cust_Grp_Name']))]
print(len(lift_df))
lift_df = exclude_small_customer(lift_df)
print(len(lift_df))

# Remove all customers group name that is numeric
lift_df = lift_df[~lift_df.Cust_Grp_Name.str.isnumeric()]
    
# Create Equipment Group column
lift_df['Equipment_Group'] = lift_df['Equipment_Size'].apply(sf.get_equipment_group)
#print(lift_df['Equipment_Group'].unique())
print(lift_df.Equipment_Group.value_counts())


lift_df['Equipment_Group_Rad'] = lift_df.apply(get_equipment_group_rad, axis=1)
print(lift_df.Equipment_Group_Rad.value_counts())


# change all customer name to upper case
lift_df['Cust_Grp_Name'] = lift_df['Cust_Grp_Name'].str.upper()

lift_df.to_csv("cus_evl_lifting_trade_mgt.csv", index=False, encoding="utf-8")
print("Done")

#lift_df['wt_per_teu'] = (lift_df.total_wt/1000)/lift_df.Item1
#print('after wt_per_teu')
#wt_per_teu = lift_df['wt_per_teu']   
#df['newcolumn'] = df.apply(fab, axis=1)
#lift_df['wt_score'] = lift_df['wt_per_teu'].apply(get_wt_score)

#lift_df['npi_per_teu'] = lift_df.total_npi/lift_df.Item1
#lift_df['npi_per_day'] = lift_df.total_npi/lift_df['Transit.Time']
# print(lift_df[['npi_per_teu', 'npi_per_day']])
#print('after Transit.Time')
#lift_df['npi_per_teu_percentile'] = [stats.percentileofscore(lift_df['npi_per_teu'], i) for i in lift_df['npi_per_teu']]

#lift_df['npi_per_teu_percentile'] = stats.rankdata(lift_df['npi_per_teu'], "max")/len(lift_df['npi_per_teu'])
#lift_df['npi_per_teu_percentile'] = lift_df['npi_per_teu_percentile']*100
#print('after percentile 1')
#lift_df['npi_per_day_percentile'] = stats.rankdata(lift_df['npi_per_day'], "max")/len(lift_df['npi_per_day'])
#lift_df['npi_per_day_percentile'] = lift_df['npi_per_day_percentile']*100
#lift_df['npi_per_day_percentile'] = [stats.rankdata(lift_df['npi_per_day'], i) for i in lift_df['npi_per_day']]
#print('after percentile 2')


# convert percentile to score
#lift_df['npi_per_teu_score'] = lift_df['npi_per_teu_percentile'].apply(get_percentile_score)
#lift_df['npi_per_day_score'] = lift_df['npi_per_day_percentile'].apply(get_percentile_score)

#print(lift_df['npi_per_teu'].describe())
#print(lift_df['npi_per_day'].describe())

#lift_df['dso_score'] = randint(1,5)
#lift_df['dso_score'] = lift_df.apply (lambda row: randint(1,5), axis=1)  
#lift_df['cancellation_score'] = lift_df.apply (lambda row: randint(1,5), axis=1)  
#lift_df['volume_score'] = lift_df.apply (lambda row: randint(1,5), axis=1)  

#df['C'] = df.apply(lambda row: randint(1,5), axis=1)


'''
Factors	Ratio
NPI/TEU - Bottom%	30%
NPI/Day - Bottom%	30%
Volume 	20%
Stay Day	15%
Cancellation	15%
DSO	10%
Cargo Weight	10%
'''
# applying weighted average
'''
lift_df['npi_per_teu_score_wa'] = lift_df['npi_per_teu_score'] * 0.3
lift_df['npi_per_day_score_wa'] = lift_df['npi_per_day_score'] * 0.3
lift_df['volume_score_wa'] = lift_df['volume_score']  * 0.2
lift_df['stay_score_wa'] = lift_df['Stay_Score'] * 0.15
lift_df['cancellation_score_wa'] = lift_df['cancellation_score'] * 0.15
lift_df['dso_score_wa'] = lift_df['dso_score'] * 0.1
lift_df['wt_score_wa'] = lift_df['wt_score'] * 0.1


lift_df['stay_score_wa'].fillna(0, inplace=True)
'''
#print('stay_score-')

#print(nvl(lift_df['stay_score_wa']))

#total_score = lift_df['volume_score_wa'] + lift_df['stay_score_wa'] + lift_df['cancellation_score_wa'] + lift_df['dso_score_wa'] + lift_df['wt_score_wa']
#print(total_score)

#lift_df['total_score_by_npi_per_teu'] =  total_score + nvl(lift_df['npi_per_teu_score_wa']) 
#lift_df['total_score_by_npi_per_day']=  total_score + nvl(lift_df['npi_per_day_score_wa'])

#print(lift_df[['npi_per_teu', 'npi_per_teu_percentile', 'npi_per_teu_score']])
#print(lift_df[['npi_per_day', 'npi_per_day_percentile', 'npi_per_day_score']])

