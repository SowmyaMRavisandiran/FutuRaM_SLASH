#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 13 11:24:44 2025

@author: marriyapillais

Code used for multiplying SLASH TCswith plug in data
"""

import pandas as pd
import numpy as np

#function to take TC data and Stock&Flow + composition plugin (WP4+WP3) information from the sheets and calculate recovery potential of elements from different flows
#takes: years = years to be assessed
#code: EWC code for the waste stream to be assessed (used in WP3*WP4)
#code_tc: EWC code for the waste stream to be assessed (used in TC data)
#data_sf_cmp: extracted dataframe from WP3+WP4 plugin
#sf_cmp: extracted SF-composition (WP4+WP3) plugin data
#to_drop = elements to drop from the assessment (to exclude)
#ws_name = Waste stream name


#Process TC data
def get_tcs(tc_df, trl_bau):
    tc_df = tc_df[['LoW code', 'Recover applications', 'max TRL','TC_1, % of the stock is applied in this application (sum <=100% )', 'Element', 'Overall avg TC', 'expert judgement TC - BAU']]

    tc_df.rename(columns={'TC_1, % of the stock is applied in this application (sum <=100% )':'preprocess_TC','LoW code':'Layer 1','Element':'Layer 4','Overall avg TC': 'avg_TC',
                      'expert judgement TC - BAU':'exp_TC'},inplace=True)
    tc_df = tc_df[tc_df['Recover applications']=='Metal recovery']
    
    #assigning exp TC as avg TC when it is missing
    tc_df.loc[tc_df['exp_TC'].isna(),'exp_TC']=tc_df['avg_TC']
    tc_df=tc_df[~tc_df['exp_TC'].isna()]
    
    #only choose preprocess TC which is of float dtype
    tc_df=tc_df[(tc_df['preprocess_TC'].apply(lambda x: isinstance(x, float)))|(tc_df['preprocess_TC']==0)]
    
    #BAU TC = preprocess TC * expert TC
    tc_df['BAU_TC'] = tc_df['exp_TC']*tc_df['preprocess_TC']
    
    #in the REC scenario, we assume that all waste is sent for metal reovery
    tc_df['REC_TC'] = tc_df['exp_TC']
    
    #sometimes expert TC is zero for BAU scenario as TRL might be low
    tc_df.loc[(tc_df['REC_TC']==0)&(tc_df['avg_TC']>tc_df['exp_TC']),'REC_TC']=tc_df['avg_TC']
    
    #Group by TRL level
    tc_df_grouped=tc_df.groupby(['Layer 1','Recover applications','max TRL','Layer 4'], as_index = False).mean()

    # Group by element, LoW code
    group_cols = ['Layer 1', 'Recover applications', 'Layer 4']  # modify if needed

    # Calculate mean BAU_TC for rows with max TRL > 6
    bau_tc_mean = tc_df_grouped[tc_df_grouped['max TRL'].isin(trl_bau)].groupby(group_cols, as_index=False)['BAU_TC'].mean()

    # Calculate mean REC_TC for all rows
    rec_tc_mean = tc_df_grouped.groupby(group_cols,as_index=False)['REC_TC'].mean()

    # Merge the two results
    tc_df_grouped = pd.merge(rec_tc_mean, bau_tc_mean, on=group_cols, how='outer')

    tc_df_grouped['BAU_TC'] = tc_df_grouped['BAU_TC'].fillna(0) # nans would correspond to rows which dont have any TRL >6. We can safely assume that BAU TC is zero in that case

    return tc_df_grouped[['Layer 1','Layer 4','BAU_TC','REC_TC']]


def get_flow_ID(df):
     flow_ID = pd.unique(df['Stock/Flow ID'])
     if len(pd.unique(df['Stock/Flow ID'])) > 1:
         print('not unique ', pd.unique(df['Stock/Flow ID']))
     return(flow_ID)

def populate(column_names, yearlist, valuelist, el_list, ws_name, country_l, scenlist, unilist, code, flow_ID):
    res_df = pd.DataFrame(columns=column_names)
    res_df['Year'] = yearlist
    res_df['Value'] = valuelist
    res_df['Layer 4'] = el_list
    res_df['Waste Stream'] = ws_name
    if sum_EU:
        res_df['Location'] = 'EU27+4'
    else:
        res_df['Location'] = country_l
    res_df['Scenario'] = scenlist
    res_df['additionalSpecification'] = ''
    res_df['Layer 1'] = code
    res_df['Layer 2'] = ''
    res_df['Layer 3'] = ''
    res_df['Stock/Flow ID'] = flow_ID
    res_df['Unit'] = unilist
    return(res_df)

#aggregates to EU level
def element_for_code_single(years, code_tc, code_sf_cmp, tc_data, sf_cmp, to_drop, ws_name, sum_EU, nation, col_names):
    #subset stock-flow-composition (WP4+WP3) and TC data to the specific EWC waste code
    
    sf_cmp_code = sf_cmp[sf_cmp['Layer 1'] == code_sf_cmp]
    tc_data_code = tc_data[tc_data['Layer 1'] == code_tc]
    
    if len(sf_cmp_code.index) == 0:
        flow_ID = ''
    else:
        flow_ID = get_flow_ID(sf_cmp_code)
    
    #get a list of unique elements in the composition sheet (WP3)
    el_list = tc_data_code['Layer 4'].drop_duplicates().to_list()

    #create a list that excludes elements to drop
    element_list = [el for el in el_list if el not in to_drop]

    #subset stock-flow data to the specific years
    sf_cmp_code_year = sf_cmp_code[sf_cmp_code['Year'].isin(years)]
    
    #sum all countries to EU27 level
    data_sum = sf_cmp_code_year.groupby(['Waste Stream','Year', 'Scenario', 'Stock/Flow ID','Layer 4','Unit'], as_index=False)['Value'].sum() 
    data_sum['Location']='EU27+4'

    if sum_EU:
        data_grouped = data_sum
    else:
        data_grouped = sf_cmp_code_year

    #calculate the element-level flows and put them into a dataframe with the headers of column_names
    #print(data_sf_eu27_code_year)
    yearlist = []
    valuelist = []
    el_list = []
    country_l = []
    unilist = []
    scenlist = []
    
    if len(flow_ID)>1:
        fi_2 = True
        for f_id in flow_ID:
            data_grouped_fid = data_grouped[data_grouped['Stock/Flow ID']==f_id]
            for year in years:
                if len(element_list)==0:
                    value = [np.nan] #Adding this as extend needs this
                    scen = ['Missing']
                    element = [np.nan]
                    scenlist.extend(scen)
                    valuelist.extend(value)
                    yearlist.extend([year])
                    el_list.extend(element)
                    unilist.extend(['Mg'])
                    country_l.extend([nation])
                for element in element_list:
                    for scenario in ['OBS', 'BAU', 'REC', 'CIR']:
                        data_scenario_element = data_grouped_fid.loc[(data_grouped_fid['Year']==year)&(data_grouped_fid['Scenario']==scenario)&(data_grouped_fid['Layer 4']==element)]
                        if scenario in ['OBS','BAU']:
                            tc_element = tc_data_code.loc[tc_data_code['Layer 4']==element]['BAU_TC']
                        elif scenario == 'REC':
                            tc_element = tc_data_code.loc[tc_data_code['Layer 4']==element]['REC_TC']
                            if len(data_scenario_element)==0:
                                data_scenario_element = data_grouped_fid.loc[(data_grouped_fid['Year']==year)&(data_grouped_fid['Scenario']=='BAU')&(data_grouped_fid['Layer 4']==element)]
                            
                        elif scenario == 'CIR':
                            tc_element = tc_data_code.loc[tc_data_code['Layer 4']==element]['REC_TC']
                            
                      
                        
                        
                        if len(data_scenario_element)==0:
                            value = [np.nan] #Adding this as extend needs this
                            scen = ['Missing']
                        
                        else:
                            scen = [scenario]
                            value = [float(tc_element)*float(data_scenario_element['Value'])]
                        scenlist.extend(scen)
                        valuelist.extend(value)
                        yearlist.extend([year])
                        el_list.extend([element])
                        unilist.extend(['Mg'])
                        country_l.extend([nation])
                       
            if fi_2:
               
    
                result_df = populate(col_names, yearlist, valuelist, el_list, ws_name, country_l, scenlist, unilist, code_sf_cmp, f_id)
                fi_2=False
            else:
                result_df = pd.concat([result_df,populate(col_names, yearlist, valuelist, el_list, ws_name, country_l, scenlist, unilist, code_sf_cmp, f_id)],ignore_index=True)
        
    else:
        
        for year in years:
            
            if len(element_list)==0:
                value = [np.nan] #Adding this as extend needs this
                scen = ['Missing']
                element = [np.nan]
                scenlist.extend(scen)
                valuelist.extend(value)
                yearlist.extend([year])
                el_list.extend(element)
                unilist.extend(['Mg'])
                country_l.extend([nation])
                
            for element in element_list:
                
                for scenario in ['OBS', 'BAU', 'REC', 'CIR']:
                    
                    data_scenario_element = data_grouped.loc[(data_grouped['Year']==year)&(data_grouped['Scenario']==scenario)&(data_grouped['Layer 4']==element)]
                    
                    if scenario in ['OBS','BAU']:
                        tc_element = tc_data_code.loc[tc_data_code['Layer 4']==element]['BAU_TC']
                    elif scenario == 'REC':
                        tc_element = tc_data_code.loc[tc_data_code['Layer 4']==element]['REC_TC']
                        if len(data_scenario_element)==0:
                            data_scenario_element =data_grouped.loc[(data_grouped['Year']==year)&(data_grouped['Scenario']=='BAU')&(data_grouped['Layer 4']==element)]
                        
                    elif scenario == 'CIR':
                        tc_element = tc_data_code.loc[tc_data_code['Layer 4']==element]['REC_TC']
                      
                    
                
                    if len(data_scenario_element)==0:
                        value = [np.nan] #Adding this as extend needs this
                        scen = ['Missing']
                        
                    
                    else:
                        scen = [scenario]
                        value = [float(tc_element)*float(data_scenario_element['Value'])]
                      
                        
                    scenlist.extend(scen)
                    valuelist.extend(value)
                    yearlist.extend([year])
                    el_list.extend([element])
                    unilist.extend(['Mg'])
                    country_l.extend([nation])
                  
        result_df = populate(col_names, yearlist, valuelist, el_list, ws_name, country_l, scenlist, unilist, code_sf_cmp, flow_ID[0])
    
    return(result_df)


#*********************Change parameters here******************************************#


file_name_write = 'SLASH_RM_msw.csv'

tc_file_name = 'Transfer coefficients_SLASH.xlsx'
tc_sheet_name = 'TCs'

trl_bau = [6,7,8,9,'>9']

sum_EU = True

sf_cmp_file_name = 'plugin_msw.csv'
#'SLASH_SF_cmp_version12.csv'

#year(s) to be evaluated, provide in list-like form
years = [i for i in range(2010,2051)]

#codes to be used provided in list format. Make sure the order of the list matches for WP3 codes (code_list), WP4 codes (code_sf_list) and the names of the processes (process_list).

code_tc_list = ['19 01 11*']
#['01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*', '10 03 08']
#['19 01 12','19 01 14', '19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P', "10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B", '10 01 01_C','10 01 02', '01 03 09', '10 04 01*', '10 06 01', '11 02 02*']
code_sf_cmp_list = ['19 01 11*']
#['01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*', '10 03 08']
#['19 01 12','19 01 14']
#['19 01 12','19 01 14','19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P', "10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B", '10 01 01_C','10 01 02', '01 03 09', '10 04 01*', '10 06 01', '11 02 02*',]


#Provide the waste stream name (ws_name) to include in the output sheet
ws_name = "SLASH"

#List any elements to be excluded from the analysis. Provide in list format
to_drop = [] #['otherOrUndefinedElements']

#Countries to use of the aggregation 
country_list = ['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR']
#['EU27+4']

column_names = ['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification', 'Stock/Flow ID', 'Layer 1','Layer 2', 'Layer 3', 'Layer 4', 'Value', 'Unit']

#*********************End Change parameters******************************************#

tc_df = pd.read_excel(tc_file_name, sheet_name = tc_sheet_name)
tc_df_grouped = get_tcs(tc_df,trl_bau)

sf_cmp = pd.read_csv(sf_cmp_file_name)

sf_cmp = sf_cmp[sf_cmp['Location'].isin(country_list)]

rm_id_mapping = {'SLASH_SteelSlagsMetallurgical': 'SLASH_2RM_SteelSlagsMetallurgical',
       'SLASH_ashesEnergyProductionGenerated': 'SLASH_2RM_ashesEnergyProductionGenerated',
       'SLASH_bottomAshesWasteInc':'SLASH_2RM_bottomAshesWasteInc', 'SLASH_flyAshesWasteInc': 'SLASH_2RM_flyAshesWasteInc',
       'SLASH_slagsMetallurgical':'SLASH_2RM_slagsMetallurgical', 'SLASH_sludgesMetallurgical': 'SLASH_2RM_sludgesMetallurgical',
       'SLASH_historicalStockBottomAndFlyAshEnergyProd':  'SLASH_2RM_historicalStockBottomAndFlyAshEnergyProd'}

sf_id_mapping = {'SLASH_SteelSlagsMetallurgical': 'SLASH_SteelSlagsMetallurgical_EUagg',
       'SLASH_ashesEnergyProductionGenerated': 'SLASH_ashesEnergyProductionGenerated_EUagg',
       'SLASH_bottomAshesWasteInc':'SLASH_bottomAshesWasteInc_EUagg', 'SLASH_flyAshesWasteInc': 'SLASH_flyAshesWasteInc_EUagg',
       'SLASH_slagsMetallurgical':'SLASH_slagsMetallurgical_EUagg', 'SLASH_sludgesMetallurgical': 'SLASH_sludgesMetallurgical_EUagg',
       'SLASH_historicalStockBottomAndFlyAshEnergyProd':  'SLASH_historicalStockBottomAndFlyAshEnergyProd_EUagg'}

fi = True
for i in range(0, len(code_tc_list)):
    code_tc = code_tc_list[i]
    code_sf_cmp = code_sf_cmp_list[i]
    if sum_EU == False:
        for nation in country_list:
            sf_cmp_loc = sf_cmp.loc[sf_cmp['Location']==nation]
            if fi == True:
                final_result_df = element_for_code_single(years, code_tc, code_sf_cmp, tc_df_grouped, sf_cmp_loc, to_drop, ws_name, sum_EU, nation, column_names)
                fi = False
            else:
                final_result_df = pd.concat([final_result_df,element_for_code_single(years, code_tc, code_sf_cmp,tc_df_grouped, sf_cmp_loc, to_drop, ws_name, sum_EU, nation, column_names)])
    else:
        nation = 'EU27+4'
        if fi == True:
            final_result_df = element_for_code_single(years, code_tc, code_sf_cmp, tc_df_grouped, sf_cmp, to_drop, ws_name, sum_EU, nation, column_names)
            fi = False
        else:
            final_result_df = pd.concat([final_result_df,element_for_code_single(years, code_tc, code_sf_cmp,tc_df_grouped, sf_cmp, to_drop, ws_name, sum_EU, nation, column_names)])
    
    
    
final_result_df = final_result_df[~final_result_df["Value"].isna()]

final_result_df['Stock/Flow ID'] = final_result_df['Stock/Flow ID'].replace(rm_id_mapping)

sf_eu = sf_cmp.groupby(['Waste Stream','Year', 'Scenario', 'Stock/Flow ID','Layer 1','Layer 4','Unit'], as_index=False)['Value'].sum() 
sf_eu['Location']='EU27+4'
sf_eu['Layer 2']=''
sf_eu['Layer 3']=''
sf_eu['additionalSpecification']=''
sf_eu['Stock/Flow ID'] = sf_eu['Stock/Flow ID'].replace(sf_id_mapping)

sf_eu=sf_eu[column_names]

final_result_df = pd.concat([final_result_df,sf_eu],ignore_index = True)
# final_result_df.to_csv(file_name_write, index=False)

