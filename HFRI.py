import pandas as pd
import os
import datetime
from datetime import date

def check_cash(s):
    if str(s).lower() == 'cash':
        return True
    else:
        return False
def check_index(s):
    if str(s).lower() == 'index adjustment':
        return True
    else:
        return False

def HFRI(folder_path, output_folder, asof):
    df_list = list()
    df_ID_list = list()
    df_DAV_list = list()
    e=f"Data_Template_for_Aberdeen_and_Vidrio_{asof}.xlsx"
    df_HFRI = pd.read_excel(f"{folder_path}/HFRI_ACCT_MAPPING.xlsx",sheet_name = 'Mgr Accounts')
    path = folder_path + "/" + f"{e}"
    template_date = e[-15:-11] + "/" + e[-10:-8] + "/" + e[-7:-5]
    print("PATH" , path, "Template Date: ", template_date )
    df_data_template = pd.read_excel(path,sheet_name = 'Constituent and Candidate Data')
    df_data_template_2 = pd.read_excel(path,sheet_name = 'HFRI-I Index Data')
    df_data_template = df_data_template.rename(columns = {'ISIN ':'Counterparty ID'})
    df_data_template_2.columns = ['Date', 'ISIN', 'Index Name', 'Total AUM', 'End AUM',
         'AUM Change from Previous Day', 'NAV', 'Previous Day NAV',
         'NAV Change $', 'NAV Change %']
    df_merge = pd.merge(df_data_template, df_HFRI, on='Counterparty ID', how='left')
    df_2 = df_data_template_2[df_data_template_2['Index Name'] == 'HFRI-I Liquid Alt UCITS Index\n(Net)']

    def get_NAV(s):
        return df_2[df_2['Date'] == s]['NAV']
    def get_previous_NAV(s):
        return df_2[df_2['Date'] == s]['Previous Day NAV']

    df = pd.DataFrame()
        
    df['Reference Day']  = df_merge['Date']
    df['Reference Day'] = df['Reference Day'].apply(lambda x: str(x.date()))
    df['Periodicity'] = 'Daily' 
    try:
        df['Attribution Gross'] = df_merge['Net Contribution to Index']
        df['Attribution Net'] = df_merge['Net Contribution to Index']
    except KeyError:
        df['Attribution Gross'] = ""
        df['Attribution Net'] = ""
    df['Investment Account Long Name'] =df_merge['Name']
    df['Opening Allocation'] = df_merge['Beginning Weight %']
    df['Closing Allocation'] = df_merge['End Weight %']
    df['Investment Performance'] = df_merge['% Price Change']
    df['Opening Equity'] = df_merge['Date'].apply(get_previous_NAV)
    df['Closing Equity'] = df_merge['Date'].apply(get_NAV)
    df['Investment Adj Opening Balance'] = df['Opening Allocation'] * df['Opening Equity']
    df['Investment Closing Balance'] = df['Closing Allocation'] * df['Closing Equity']
    df['Portfolio Opening Balance'] = df['Investment Adj Opening Balance'] 
    df['Portfolio Closing Balance'] = df['Investment Closing Balance']
    df['Investment Account UID'] = ''
    df['Investor Account UID'] = 'HFRIILAU'
    df['Investor Account Long Name'] = 'HFRI-I Liquid Alt UCITS Index - Account'
    df['Counterparty ID'] = df_merge['Counterparty ID']
    df['Management Company'] = df_merge['FUND_COMP_NAME']
    df['LONG_COMP_NAME']=df_merge['LONG_COMP_NAME']
   
    # for cash and index adjustment 
    #df['cash'] = df['LONG_COMP_NAME'].apply(check_cash)
    df['cash'] = df['Counterparty ID'].apply(check_cash)
    df.loc[df['cash'],'Investment Account Long Name']  = "HFRI Fund Adjustments - Account"
    df['index adjustment'] = df['LONG_COMP_NAME'].apply(check_index)
        
    try:
        df.loc[df['cash'],'Attribution Gross'] = float(df.loc[df['index adjustment'],'Attribution Gross'])
        df.loc[df['cash'],'Attribution Net'] = float(df.loc[df['index adjustment'],'Attribution Net'])
        df = df.drop(index = df[df['index adjustment']].index)
    except TypeError:
        df.loc[df['cash'],'Attribution Gross'] = ""
        df.loc[df['cash'],'Attribution Net'] = ""


    df_P = df[['Reference Day', 'Periodicity', 'Investor Account UID',
         'Investor Account Long Name', 'Investment Account UID',
         'Investment Account Long Name', 'Attribution Gross',
         'Attribution Net', 'Opening Allocation', 'Closing Allocation',
         'Investment Performance', 'Investment Adj Opening Balance',
         'Investment Closing Balance', 'Portfolio Opening Balance',
         'Portfolio Closing Balance', 'Opening Equity', 'Closing Equity']]
    df_list.append(df_P)
    df_ID = df[['Reference Day', 'Periodicity', 'Investor Account UID',
           'Investor Account Long Name', 'Investment Account UID',
           'Investment Account Long Name', 'Attribution Gross',
           'Attribution Net', 'Opening Allocation', 'Closing Allocation',
           'Investment Performance', 'Investment Adj Opening Balance',
           'Investment Closing Balance', 'Portfolio Opening Balance',
           'Portfolio Closing Balance', 'Opening Equity', 'Closing Equity','Counterparty ID', 'Management Company','LONG_COMP_NAME']]
    df_ID_list.append(df_ID)
    df_DAV = pd.DataFrame()
    df_2 = df_data_template_2[(df_data_template_2['Date']==template_date) & (df_data_template_2['ISIN']=='HFRIILAU')]
    df_DAV['Portfolio Account UID'] = df_2['ISIN']
    df_DAV['Account Long Name'] = df_2['Index Name'].apply(lambda s: s.replace("(Net)", "").strip() + " - Account")
    df_DAV['Date'] = df_2['Date'].apply(lambda x: str(x.date()))
    df_DAV['NAV/Share'] = df_2['NAV']
    df_DAV["Final"] = "True"
    df_DAV_list.append(df_DAV)
    print(f"{e} is done")
        
    df_DAV_all = pd.concat(df_DAV_list)
    df_Proforma = pd.concat(df_list)
    df_Proforma_ID = pd.concat(df_ID_list)

    df_DAV_all = df_DAV_all.reset_index()
    df_Proforma = df_Proforma.reset_index()
    df_Proforma_ID = df_Proforma_ID.reset_index()

    df_DAV_all = df_DAV_all.drop(columns = ['index'])
    df_Proforma = df_Proforma.drop(columns = ['index'])
    df_Proforma_ID = df_Proforma_ID.drop(columns = ['index'])
    
    #date_ = str(date.today())

    df_DAV_all.to_excel(output_folder + '/'+ f'Portfolio Account NAV {asof}.xlsx', index = False)
    df_Proforma.to_excel(output_folder + '/'+f'DAV Proforma Acc Analy {asof}.xlsx', index = False)  
    df_Proforma_ID.to_excel(output_folder + '/'+ f'DAV Proforma Acc Analy with ID {asof}.xlsx', index = False)  
