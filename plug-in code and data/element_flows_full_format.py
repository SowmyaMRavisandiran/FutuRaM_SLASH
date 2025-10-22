#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: jmogollon
Code used for calculating element-level flows (currently tailored to SLASH)
"""
import pandas as pd
import numpy as np


#function to take WP4 and WP3 information from the sheets and calculate element level flows
#takes: years = years to be assessed
#code: EWC code for the waste stream to be assessed (used in WP3)
#code_sf: EWC code for the waste stream to be assessed (used in WP3)
#data_cons: extracted dataframe from WP3
#data_sf_eu27: extracted (aggregated EU27 specific SF from WP4
#to_drop = elements to drop from the assessment (to exclude)
#ws_name = Waste stream name
#process = waste stream process name

def get_flow_ID(df):
    flow_ID = pd.unique(df['Stock/Flow ID'])[0]
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
        res_df['Location'] = 'EU27'
    else:
        res_df['Location'] = country_l
    res_df['Scenario'] = scenlist
    res_df['additionalSpecification'] = ' '
    res_df['Layer 1'] = code
    res_df['Layer 2'] = ' '
    res_df['Layer 3'] = ' '
    res_df['Stock/Flow ID'] = flow_ID
    res_df['Unit'] = unilist
    return(res_df)

#aggregates to EU level
def element_for_code_single(years, code, code_sf, data_cons, data_sf_eu27, to_drop, ws_name, process, sum_EU, nation, col_names):
    #subset stock-flow (WP4) and concentration (WP3) data to the specific EWC waste code
    data_sf_eu27_code = data_sf_eu27[data_sf_eu27['Substance_main_parent'] == code_sf]
    data_cons_code = data_cons[data_cons['Layer 1'] == code]
    
    if len(data_sf_eu27_code.index) == 0:
        flow_ID = ' '
    else:
        flow_ID = get_flow_ID(data_sf_eu27_code)
    
    #get a list of unique elements in the composition sheet (WP3)
    el_list = data_cons_code['Layer 4'].drop_duplicates().to_list()

    #create a list that excludes elements to drop
    element_list = [el for el in el_list if el not in to_drop]

    #subset stock-flow data to the specific years
    data_sf_eu27_code_year = data_sf_eu27_code[data_sf_eu27_code['Year'].isin(years)][['Year', 'Value', 'Location', 'Unit', 'Scenario', 'Stock/Flow ID']]
    
    #sum all countries to EU27 level
    data_sum = data_sf_eu27_code_year.groupby(['Year'], as_index=False).sum() #!!!!Note: this sums all scenarios also!!!!# 

    if sum_EU:
        data_grouped = data_sum
    else:
        data_grouped = data_sf_eu27_code_year

    #calculate the element-level flows and put them into a dataframe with the headers of column_names
    #print(data_sf_eu27_code_year)
    yearlist = []
    valuelist = []
    el_list = []
    country_l = []
    unilist = []
    scenlist = []
    for year in years:
        for element in element_list:
            if sum_EU:
                value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*data_grouped.loc[data_grouped['Year']==year]['Value']
                scen = data_grouped.loc[data_grouped['Year']==year]['Scenario']
                scenlist.extend(scen)
                valuelist.extend(value)
                yearlist.extend([year]*len(scen))
                el_list.extend([element]*len(scen))
                unilist.extend(['Mg']*len(scen))
                
                print('working')
                #print(data_grouped)
                print(code)
            else:
                
                if isinstance(data_grouped.loc[data_grouped['Year']==year]['Value'], pd.core.series.Series):
                    #value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*float(data_grouped.loc[data_grouped['Year']==year]['Value'].mean())
                    value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*data_grouped.loc[data_grouped['Year']==year]['Value']
                    
                    #print(data_grouped.loc[data_grouped['Year']==year])
                    if len(data_grouped.loc[data_grouped['Year']==year])==0:
                        value = [np.nan] #Adding this as extend needs this
                        uni = 'Mg'
                        #scen = 'BAU'
                        scen = ['Missing']
                    else:
                        #uni = data_grouped.loc[data_grouped['Year']==year]['Unit'].iloc[0]
                        #scen = data_grouped.loc[data_grouped['Year']==year]['Scenario'].iloc[0]
                        uni = 'Mg'
                        scen = data_grouped.loc[data_grouped['Year']==year]['Scenario']
                else:
                    value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*float(data_grouped.loc[data_grouped['Year']==year]['Value'])
                    #uni = data_grouped.loc[data_grouped['Year']==year]['Unit']
                    uni='Mg'
                    scen = data_grouped.loc[data_grouped['Year']==year]['Scenario']
                
                
                valuelist.extend(value)
                scenlist.extend(scen)
                
                
                yearlist.extend([year]*len(scen))
                unilist.extend([uni]*len(scen))
                el_list.extend([element]*len(scen))
                country_l.extend([nation]*len(scen))
                
                
    result_df = populate(col_names, yearlist, valuelist, el_list, ws_name, country_l, scenlist, unilist, code, flow_ID)
    return(result_df)

#calculates for every country
def element_for_code_EU(years, code, code_sf, data_cons, data_sf_eu27, to_drop, ws_name, process, sum_EU, country_list,  col_names):
    
    #subset stock-flow (WP4) and concentration (WP3) data to the specific EWC waste code
    data_sf_eu27_code = data_sf_eu27[data_sf_eu27['Substance_main_parent'] == code_sf]
    data_cons_code = data_cons[data_cons['Layer 1'] == code]

    flow_ID = get_flow_ID(data_sf_eu27_code)

    #get a list of unique elements in the composition sheet (WP3)
    el_list = data_cons_code['Layer 4'].drop_duplicates().to_list()

    #create a list that excludes elements to drop
    element_list = [el for el in el_list if el not in to_drop]


    #subset stock-flow data to the specific years
    data_sf_eu27_code_year = data_sf_eu27_code[data_sf_eu27_code['Year'].isin(years)][['Year', 'Value', 'Location', 'Unit', 'Scenario', 'Stock/Flow ID']]
    
    #sum all countries to EU27 level
    data_sum = data_sf_eu27_code_year.groupby(['Year'], as_index=False).sum()

    if sum_EU:
        data_grouped = data_sum
    else:
        data_grouped = data_sf_eu27_code_year

    #calculate the element-level flows and put them into a dataframe with the headers of column_names
    print(data_sf_eu27_code_year)
    yearlist = []
    valuelist = []
    el_list = []
    country_l = []
    unilist = []
    scenlist = []
    for year in years:
        for element in element_list:
            if sum_EU:
                #value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*float(data_grouped.loc[data_grouped['Year']==year]['Value'])
                value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*data_grouped.loc[data_grouped['Year']==year]['Value']
                #valuelist.append(value)
                #yearlist.append(year)
                #el_list.append(element)
                #unilist.append('Mg')
                #scenlist.append('BAU')
                
                scen = data_grouped.loc[data_grouped['Year']==year]['Scenario']
                scenlist.extend(scen)
                valuelist.extend(value)
                yearlist.extend([year]*len(scen))
                el_list.extend([element]*len(scen))
                unilist.extend(['Mg']*len(scen))

            else:
                for nation in country_list:
                    if isinstance(data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Value'], pd.core.series.Series):
                        #value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*float(data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Value'].mean())
                        value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Value']
                        if len(data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)])==0:
                            uni = 'Mg'
                            #scen = 'BAU'
                            scen = ['Missing']
                            value = [np.nan] #adding this as extend needs this
                        else:
                            #uni = data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Unit'].iloc[0]
                            #scen = data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Scenario'].iloc[0]
                            scen = data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Scenario']
                            uni = 'Mg'
                    else:
                        value = 0.01*float(data_cons_code.loc[data_cons_code['Layer 4']==element]['Value'])*float(data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Value'])
                        #uni = data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Unit']
                        uni='Mg'
                        scen = data_grouped.loc[(data_grouped['Year']==year) & (data_grouped['Location']==nation)]['Scenario']
                    #valuelist.append(value)
                    #yearlist.append(year)
                    #unilist.append(uni)
                    #scenlist.append(scen)
                    #el_list.append(element)
                    #country_l.append(nation)
                    
                    valuelist.extend(value)
                    scenlist.append(scen)
                    yearlist.extend([year]*len(scen))
                    unilist.append([uni]*len(scen))
                    el_list.append([element]*len(scen))
                    country_l.append([nation]*len(scen))

    result_df = populate(col_names, yearlist, valuelist, el_list, ws_name, country_l, scenlist, unilist, code, flow_ID)
    return(result_df)


#*********************Change parameters here******************************************#

#filename for output file
filename_write = 'plugin_msw.xlsx'#"plugin_coal_msw.xlsx"

#if sum_EU == True, all locations aggregated
sum_EU = False #Note: SUM_EU code is not corrected for different scenario multiplication

#if method_single == True, use a faster calculation method via a separate calculation method
method_single = True


#composition file (WP3)
filename_comp = "WP3_MSW.csv"
#'240419_consolidation_dataset_template_flow-deposit_WP3_version3_SLASH_v4(SLASH_consolidated_inputForSF).csv' #'240323_consolidation_dataset_template_flow-deposit_WP3_version3_SLASH_v3.xlsx'
#consolidated data sheet name
#sheetname_cons = 'SLASH_consolidated_inputForSF'
cons_cols = ['Layer 1','Layer 4', 'Value']

#stock-flow file and sheet
filename_sf = 'MS18 _SLASH dataset_FINAL.xlsx' 
#'Data_Structure_Task4.1_Task4.2_WP6_SLASH_v3-coal_only.xlsx'
#'agriculture_combined.xlsx' #'clean_use.xlsx'#'Data_Structure_Task4.1_Task4.2_WP6_SLASH_v3-coal_only.xlsx'#'clean_use.xlsx' #'Data_Structure_Task4.1_Task4.2_WP6_SLASH_v3-coal_merged.xlsx'
sheetname_sf = 'Sheet1'

#year(s) to be evaluated, provide in list-like form
years = [i for i in range(2010,2051)]

#codes to be used provided in list format. Make sure the order of the list matches for WP3 codes (code_list), WP4 codes (code_sf_list) and the names of the processes (process_list).

code_list = ['19 01 11*']
#['01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*', '10 03 08']
#['19 01 12','19 01 14']
#['19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P', "10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B"]##["10 01 01", "10 01 03", "10 01 15", "10 01 15", "10 01 17", "10 01 17"]
#['10 01 01_C','10 01 02']
#['01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*']
#['19 01 12','19 01 14']#['10 01 01_C','10 01 02','19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P']#["10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B"]##["10 01 01", "10 01 03", "10 01 15", "10 01 15", "10 01 17", "10 01 17"]
code_sf_list = ['19 01 11*']
#['01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*', '10 03 08']
#['19 01 12','19 01 14']
#['19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P', "10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B"]
#['10 01 01_C','10 01 02_C']
#['01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*']
#['19 01 12','19 01 14']#['10 01 01_C','10 01 02_C','19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P'] #["10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B"]
process_list =  ['MSWI bottom ash']
#['Red mud from alumina production','Primary Al production slags', 'End-slag from Pb metallurgy',"End-slag from Cu metallurgy",'Sludges from Zn metallurgy', 'Al salt slags from secondary production']
#['Sewage Sludge Bottom Ash','Sewage Sludge Fly Ash']
#['MSWI Bottom Ash','MSWI Fly Ash', 'Paper Bottom Ash', 'Paper Fly Ash',"Biomass Bottom Ash", "Biomass fly Ash", "Animal_waste bottom Ash", "Vegetal_waste bottom Ash", "Animal_waste fly Ash", "Vegetal_waste fly Ash"]
#['Coal Bottom Ash','Coal Fly Ash']
#['Red mud from alumina production','Primary Al production slags', 'End-slag from Pb metallurgy',"End-slag from Cu metallurgy",'Sludges from Zn metallurgy']
#['Sewage Sludge Bottom Ash','Sewage Sludge Fly Ash']#['Coal Bottom Ash','Coal Fly Ash','MSWI Bottom Ash','MSWI Fly Ash', 'Paper Bottom Ash', 'Paper Fly Ash']#["Biomass Bottom Ash", "Biomass fly Ash", "Animal_waste bottom Ash", "Vegetal_waste bottom Ash", "Animal_waste fly Ash", "Vegetal_waste fly Ash"] #


#Provide the waste stream name (ws_name) to include in the output sheet
ws_name = "SLASH"

#List any elements to be excluded from the analysis. Provide in list format
to_drop = [] #['otherOrUndefinedElements']

#Countries to use of the aggregation (note, Malta missing from the example analysis)
#country_list = ['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE']
country_list = ['EUROPE','AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR']
#['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR']
#country_list = ['DEU']
column_names = ['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification', 'Stock/Flow ID', 'Layer 1','Layer 2', 'Layer 3', 'Layer 4', 'Value', 'Unit']

#*********************End Change parameters******************************************#
data_sf = pd.read_excel(filename_sf, sheet_name=sheetname_sf, header=0)
data_sf = data_sf[data_sf['Substance_main_parent'].isin(code_sf_list)]
data_sf['Value']=data_sf['Value'].astype(float) 

data_cons = pd.read_csv(filename_comp, usecols=cons_cols)#pd.read_excel(filename_comp, sheet_name=sheetname_cons, header=0, usecols=cons_cols)

data_sf_eu27 = data_sf[data_sf['Location'].isin(country_list)]


#print('data data data')
#print(data_sf_eu27)

fi = True
for i in range(0, len(code_list)):
    code = code_list[i]
    code_sf = code_sf_list[i]
    process = process_list[i]
    if method_single == True:
        for nation in country_list:
            data_sf_eu27 = data_sf.loc[data_sf['Location']==nation]
            if fi == True:
                result_df = element_for_code_single(years, code, code_sf, data_cons, data_sf_eu27, to_drop, ws_name, process, sum_EU, nation, column_names)
                fi = False
            else:
                result_df = pd.concat([result_df,element_for_code_single(years, code, code_sf, data_cons, data_sf_eu27, to_drop, ws_name, process, sum_EU, nation, column_names)])
    
    else:
        if fi == True:
            result_df = element_for_code_EU(years, code, code_sf, data_cons, data_sf_eu27, to_drop, ws_name, process, sum_EU, country_list, column_names)
            fi = False
        else:
            result_df = pd.concat([result_df,element_for_code_EU(years, code, code_sf, data_cons, data_sf_eu27, to_drop, ws_name, process, sum_EU, country_list, column_names)])

result_df = result_df[~result_df["Value"].isna()]

#result_check = result_df[result_df["Stock/Flow ID"]=='SLASH_historicalStockBottomAndFlyAshEnergyProd']
#result_df = result_df[~(result_df["Stock/Flow ID"]=='SLASH_historicalStockBottomAndFlyAshEnergyProd')]

#result_df['Layer 1']=result_df['additionalSpecification']
#result_df['additionalSpecification'] = ''

#result_df['Stock/Flow ID']='SLASH_ashesEnergyProductionGenerated'
#result_df.loc[result_df['Year'].isin(np.arange(2010, 2022)),'Scenario'] = 'OBS'


#result_df.to_excel(filename_write, index=False)


#%%Adding SS SF data
#sf_data = pd.read_excel("MS18 _SLASH dataset_FINAL.xlsx", sheet_name ="Sheet1")

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification','Substance_main_parent', 'Stock/Flow ID', 'Value', 'Unit']]

#sf_data.rename(columns = {'Substance_main_parent':'Layer 1'}, inplace=True)

#sf_data['Layer 2']=''

#sf_data['Layer 3']=''

#sf_data['Layer 4']=''
#sf_data['additionalSpecification']=''

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification', 'Stock/Flow ID', 'Layer 1','Layer 2', 'Layer 3', 'Layer 4', 'Value', 'Unit']]

#sf_data=sf_data[sf_data['Layer 1'].isin(['19 01 12','19 01 14'])]


#sf_data=sf_data.loc[sf_data['Value'].notna()]

#sf_data=sf_data[sf_data['Location'].isin(['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR'])]

#sf_data['Unit']='Mg'

#result_df = pd.concat([result_df,sf_data],ignore_index=True)
#result_df.to_excel(filename_write, index=False)


#%%Adding sf_data

#sf_data = pd.read_excel("MS18 _SLASH dataset_FINAL.xlsx", sheet_name ="Sheet1")

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification','Substance_main_parent', 'Stock/Flow ID', 'Value', 'Unit']]

#sf_data.rename(columns = {'Substance_main_parent':'Layer 1'}, inplace=True)

#sf_data['Layer 2']=''

#sf_data['Layer 3']=''

#sf_data['Layer 4']=''
#sf_data['additionalSpecification']=''

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification', 'Stock/Flow ID', 'Layer 1','Layer 2', 'Layer 3', 'Layer 4', 'Value', 'Unit']]

#sf_data=sf_data[sf_data['Layer 1'].isin(['10 01 01_C','10 01 02_C','19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P',"10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B", '01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*'])]

#sf_data.loc[(sf_data['Layer 1']=='10 01 02_C'), 'Layer 1']= '10 01 02'

#sf_data.loc[(sf_data['Location']=='SVK')&(sf_data['Year']==2028)&(sf_data['Layer 1'].isin(['19 01 11*','19 01 13*']))&(sf_data['Scenario']=='BAU'),'Value']=0

#sf_data=sf_data.loc[sf_data['Value'].notna()]

#sf_data=sf_data[sf_data['Location'].isin(['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR'])]

#sf_data['Unit']='Mg'

#plugin=pd.read_csv('SLASH_SF_cmp_version4.csv')

#new = pd.concat([plugin,sf_data],ignore_index=True)

#new.to_csv('SLASH_SF_cmp_version6.csv',index=False)
#%% Adding sewage sludge data
#sf_data = pd.read_excel("MS18 _SLASH dataset_FINAL.xlsx", sheet_name ="Sheet1")
#sf_data = sf_data[sf_data['Substance_main_parent'].isin(['19 01 12','19 01 14'])]
#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification','Substance_main_parent', 'Stock/Flow ID', 'Value', 'Unit']]

#sf_data.rename(columns = {'Substance_main_parent':'Layer 1'}, inplace=True)

#sf_data['Layer 2']=''

#sf_data['Layer 3']=''

#sf_data['Layer 4']=''
#sf_data['additionalSpecification']=''

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification', 'Stock/Flow ID', 'Layer 1','Layer 2', 'Layer 3', 'Layer 4', 'Value', 'Unit']]

#sf_data=sf_data[sf_data['Layer 1'].isin(['10 01 01_C','10 01 02_C','19 01 11*','19 01 13*', '10 01 15_P', '10 01 17_P',"10 01 01_B", "10 01 03", "10 01 15_A", "10 01 15_B", "10 01 17_A", "10 01 17_B", '01 03 09', '10 03 04*', '10 04 01*', '10 06 01', '11 02 02*'])]

#sf_data.loc[(sf_data['Layer 1']=='10 01 02_C'), 'Layer 1']= '10 01 02'

#sf_data.loc[(sf_data['Location']=='SVK')&(sf_data['Year']==2028)&(sf_data['Layer 1'].isin(['19 01 11*','19 01 13*']))&(sf_data['Scenario']=='BAU'),'Value']=0

#sf_data=sf_data.loc[sf_data['Value'].notna()]

#sf_data=sf_data[sf_data['Location'].isin(['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR'])]

#sf_data['Unit']='Mg'

#plugin=pd.read_csv('SLASH_SF_cmp_version6.csv')
#ss_plugin = pd.read_excel('plugin_ss.xlsx')
#new = pd.concat([plugin,sf_data],ignore_index=True)
#new_2=pd.concat([new,result_df], ignore_index=True)

#new.to_csv('SLASH_SF_cmp_version7.csv',index=False)

#%% Adding SF data coal + slag data + coal plug-in + missing countries


#sf_data = pd.read_excel("Data_Structure_Task4.1_Task4.2_WP6_SLASH_v3-coal_only.xlsx", sheet_name ="Sheet1")

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification','Substance_main_parent', 'Stock/Flow ID', 'Value', 'Unit']]

#sf_data.rename(columns = {'Substance_main_parent':'Layer 1'}, inplace=True)

#sf_data['Layer 2']=''

#sf_data['Layer 3']=''

#sf_data['Layer 4']=''
#sf_data['additionalSpecification']=''

#sf_data = sf_data[['Waste Stream', 'Location', 'Year', 'Scenario', 'additionalSpecification', 'Stock/Flow ID', 'Layer 1','Layer 2', 'Layer 3', 'Layer 4', 'Value', 'Unit']]

#sf_data=sf_data[sf_data['Layer 1'].isin(['10 01 01_C','10 01 02_C'])]

#sf_data.loc[(sf_data['Layer 1']=='10 01 02_C'), 'Layer 1']= '10 01 02'

#sf_data=sf_data.loc[sf_data['Value'].notna()]

#sf_data=sf_data[sf_data['Location'].isin(['AUT', 'BEL', 'BGR', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GRC', 'HRV', 'HUN', 'IRL', 'ITA', 'LTU', 'LUX', 'LVA', 'MLT','NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'SWE','CHE','ISL', 'GBR','NOR'])]

#sf_data['Unit']='Mg'


#df_1 = pd.read_csv("SLASH_SF_cmp_version8.csv")
#df_1 = df_1.loc[~df_1["Layer 1"].isin(['10 01 01_C', '10 01 02'])]
#df_2 = pd.read_excel("plugin_coal.xlsx", sheet_name='Sheet1')
#df_3 = pd.read_excel("plugin_slags.xlsx", sheet_name='Sheet1')
#df_4 = pd.read_excel("plugin_missing_countries.xlsx", sheet_name='Sheet1')

#new = pd.concat([df_1,df_2,df_3,df_4,sf_data],ignore_index=True)
#new['Layer 2']=''
#new['Layer 3']=''
#new.to_excel('SLASH_SF_cmp_version9.xlsx',index=False)



