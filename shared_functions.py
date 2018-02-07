# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 15:15:16 2017

@author: agarwalm
"""

def get_equipment_group(p_eqp_size):
    if p_eqp_size in ("20", "40", "C4", "45", 'S2', 'S4', 'C5'):
        return "DRY"
    elif p_eqp_size in ("2R", "4R", "R2", "Z4"):
        return "REEFER"
    elif p_eqp_size in ("2F", "4F", "2P", "4P", "P2", "P4", "F2", "F4"):
        return "SPECIAL"
    else:
        return "TANK"

    
def df_format_col_name(p_df):    
    p_df.rename(columns=lambda x: x.replace(' ', '_').replace('.','').title(), inplace=True)    
    print(sorted(p_df.columns))
