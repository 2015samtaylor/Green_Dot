#!/usr/bin/env python
# coding: utf-8

# In[24]:


import pandas as pd
import time
import pyodbc
from calpads.client import CALPADSClient
import pandas as pd

cp_user = input(str('Input Calpads Username/Email: ')).strip()
cp_pass = input(str('Input Calpads Password: ')).strip()

start_time = time.time()

# CALPADS_PASS = os.environ.get('CALPADS_PASS')
cc = CALPADSClient(username = cp_user,
                  password = cp_pass)

# file straight from SQL database, must be connected to VPN
# coming from Calpads_8_1, Filtered for 2021-2022 school year. 

# df = pd.read_csv(r'C:\Users\samuel.taylor\OneDrive - Green Dot Public Schools\Desktop\Downloaded_CSVS\Calpads_2021_2022.csv')

def SQL_query(query):
    odbc_name = 'GD_DW'
    conn = pyodbc.connect(f'DSN={odbc_name};')
    df_SQL = pd.read_sql_query(query, con = conn)
    return(df_SQL)

df = SQL_query('select * from [SingleSource].[dbo].[Calpads_8_1]')
df['Start_Date'] = pd.to_datetime(df['Start_Date'])
df = df.loc[df['Start_Date'] >= '2021-07-30']
df.reset_index(inplace = True, drop = True)

# provides SSIDs for all students that were accounted for (Calpads_8_1) in Fall 2021. 
# 10820 unique SSIDs. 



# good for running from start
list_len = len(df['SSID'][:10])

# list_len = 10820
i = 0


output = []

while i < list_len:
    student = cc.get_enrollment_history(df['SSID'][i])
    
    i += 1
    
    query_len = len(student['Data'])-1
    
    n = 0
    while n < query_len:
        
        SchoolOfAttendance = student['Data'][n]['EnrollmentSummary']['SchoolOfAttendance']
        SSID = student['Data'][n]['SSID']
        GradeLevel = student['Data'][n]['EnrollmentSummary']['GradeLevel']
        EnrollmentStartDate = student['Data'][n]['EnrollmentSummary']['EnrollmentStartDate']

        info_list = [SSID, GradeLevel, SchoolOfAttendance, EnrollmentStartDate]

        output.append(info_list)

        n += 1
        
df_2 = pd.DataFrame(output, columns = ['SSID', 'GradeLevel', 'SchoolOfAttendance', 'EnrollmentStartDate'])  
df_2 = df_2.drop_duplicates() 
df_2['SSID'] = df_2['SSID'].astype(str)

df_2 = df_2[(df_2['GradeLevel'] != 'KN') & (df_2['GradeLevel'] != 'PS') & df_2['GradeLevel'] != 'IN']
df_2['SchoolOfAttendance'] = df_2['SchoolOfAttendance'].astype(str)
df_2['SchoolID'] = df_2['SchoolOfAttendance'].str[-7:]


df_2['EnrollmentStartDate'] = df_2['EnrollmentStartDate'].astype(str)
df_2['EnrollmentStartDate'] = df_2['EnrollmentStartDate'].str[:10]
df_2['EnrollmentStartDate'] = pd.to_datetime(df_2['EnrollmentStartDate'])

df_2 = df_2[(df_2['EnrollmentStartDate'] > '2020-08-01') & (df_2['EnrollmentStartDate'] <= '2021-07-30')]

print('--- %s seconds ----' % (time.time() - start_time))
    
df_2.head(40)    

# Filters CALPADS API for student enrollment between 2020-08-01 : 2021-07-30 
# effectively getting prior enrollments before 2021-2022 school year. 

