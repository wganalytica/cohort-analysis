#!/usr/bin/env python
# coding: utf-8

# ## Introduction
# 
# This notebook automatically runs cohort analysis. Namely, a dataframe is created where users are grouped based on week (starting from 6 weeks ago), and each week's number of users, average session duration, number of completed modules per user, and number of referrals executed per user is listed.
# 
# ### Requirements
# 
# You will need your own SQL username and password, as well as a service account key to the Google Analytics server.

# ## Imports & API Keys

# In[1]:


import os
import pandas as pd
import mysql.connector
import json
import requests
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import pygsheets

usr = os.getenv('sql_user')
pwd = os.getenv('sql_pwd')
host = os.getenv('sql_host')
db = os.getenv('sql_db')
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client_secrets.json'
VIEW_ID = os.getenv('ga_viewid')


# ## Functions

# ### Extracting Data from SQL Database
# 
# The following function creates the corresponding Pandas dataframe when a SQL query is passed as the argument.

# In[2]:


def query(sql):
    
    cnx = mysql.connector.connect(user=usr, 
                              password=pwd,
                              host=host,
                              database=db,
                                 port=3306)
    
    df = pd.read_sql(sql, cnx)
    cnx.close()
    return df


# ### Extracting Data from Google Analytics
# 
# The following functions extracts data from Google Analytics database and creates a table. The metrics that are extracted are `avgSessionDuration` and `users` (number of users), and the time period is weeks.

# In[3]:


def initialize_analyticsreporting():
    #Initializes an Analytics Reporting API V4 service object.

    #Returns: An authorized Analytics Reporting API V4 service object.

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def get_report(analytics, group):
  # Queries the Analytics Reporting API V4.

  # Args: analytics: An authorized Analytics Reporting API V4 service object.
  # Returns: The Analytics Reporting API V4 response.
    if group == "all":
        return analytics.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': VIEW_ID,
              'dateRanges': [{'startDate': '49daysAgo', 'endDate': 'today'}],
              'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:avgSessionDuration'}],
              'dimensions': [{'name': 'ga:week'}]
            }]
          }
        ).execute()
    if group == "cu":
        return analytics.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': VIEW_ID,
              'dateRanges': [{'startDate': '49daysAgo', 'endDate': 'today'}],
              'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:avgSessionDuration'}],
              'dimensions': [{'name': 'ga:segment'}, {'name': 'ga:week'}],
              'segments': [{'segmentId': 'gaid::xYwcqslQTFOYNgonvtoKzA'}]
            }]
          }
        ).execute()
    if group == "zogo":
        return analytics.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': VIEW_ID,
              'dateRanges': [{'startDate': '49daysAgo', 'endDate': 'today'}],
              'metrics': [{'expression': 'ga:users'}, {'expression': 'ga:avgSessionDuration'}],
              'dimensions': [{'name': 'ga:segment'}, {'name': 'ga:week'}],
              'segments': [{'segmentId': 'gaid::llm7xc8IQv2Flkit2ofZFA'}]
            }]
          }
        ).execute()

def table_response(response, group):
    #Parses and creates a table from the Analytics Reporting API V4 response.

    #Args: response: An Analytics Reporting API V4 response.

    dim = []
    val = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        for dimension in dimensions:
            dim.append(dimension)

        for i, values in enumerate(dateRangeValues):
            for value in values.get('values'):
                val.append(float(value))
                
    user = [] 
    sessDur = [] 
    for i in range(0, len(val)): 
        if i % 2: 
            sessDur.append((str(int(val[i] // 60))) + " minutes " + str(int(val[i] % 60)) + " seconds")
        else : 
            user.append(int(val[i])) 
    
    if group == "all":
        week = []
        for i in dim:
            week.append(int(i))
        
        df_ga = pd.DataFrame()
        df_ga['week'] = week
        df_ga['num_users'] = user
        df_ga['avg_session_duration'] = sessDur
        
    else:
        segment = [] 
        week = [] 
        for i in range(0, len(dim)): 
            if i % 2: 
                week.append(int(dim[i]))
            else : 
                segment.append(dim[i])

        df_ga = pd.DataFrame()
        df_ga['week'] = week
        df_ga['num_users'] = user
        df_ga['avg_session_duration'] = sessDur

    return df_ga


# ### Cohort Analysis Function
# 
# The following function uses the GA data extracted above as well as data from the SQL database to complete a cohort analysis.

# In[4]:


# for the parameter group, pass "all" for all users, "zogo" for ZOGO123 users, and "cu" for CU users
def cohort(group):
    
    #Google Analytics Data: number of users and average session duration
    analytics = initialize_analyticsreporting()
    if group == "all":
        response = get_report(analytics, "all")
        df_ga = table_response(response, "all")
    if group == "cu":
        response = get_report(analytics, "cu")
        df_ga = table_response(response, "cu")
    if group == "zogo":
        response = get_report(analytics, "zogo")
        df_ga = table_response(response, "zogo")
        

    
    #SQL tables: average number of days used,
    #number of modules and number of referrals
    
    if group == "all":
        num_days_query = '''
        SELECT week, sum(num_days) / count(*) as avg_num_days
        FROM
        (
        SELECT u.id, week(ecm.date_created) + 1 as week, count(DISTINCT date(ecm.date_created)) as num_days
        FROM user u
        LEFT JOIN education_completed_module ecm
        ON u.id = ecm.user_id
        WHERE (ecm.date_created IS NOT NULL OR ecm.date_created IS NULL)
        AND ecm.date_created >= now() - interval 49 day
        GROUP BY u.id, week(ecm.date_created)
        ORDER BY u.id
        ) AS num_days_tab
        GROUP BY week
        '''
        num_modules_query = '''
        SELECT week(ecm.date_created) + 1 as week, count(ecm.id) as num_modules
        FROM education_completed_module ecm
        WHERE ecm.date_created >= now() - interval 49 day
        GROUP BY week(ecm.date_created)
        '''
        num_referrals_query = '''
        SELECT week(rr.date_executed) + 1 as week, count(rr.referred_id) as num_refer
        FROM referral_relationship rr
        WHERE rr.date_executed >= now() - interval 49 day
        GROUP BY week(rr.date_executed)
        '''
    
    if group == "cu":
        num_days_query = '''
        SELECT week, sum(num_days) / count(*) as avg_num_days
        FROM
        (
        SELECT u.id, week(ecm.date_created) + 1 as week, count(DISTINCT date(ecm.date_created)) as num_days
        FROM user u
        LEFT JOIN education_completed_module ecm
        ON u.id = ecm.user_id
        WHERE (ecm.date_created IS NOT NULL OR ecm.date_created IS NULL)
        AND ecm.date_created >= now() - interval 49 day
        AND u.institution_id <> 90
        GROUP BY u.id, week(ecm.date_created)
        ORDER BY u.id
        ) AS num_days_tab
        GROUP BY week
        '''
        num_modules_query = '''
        SELECT week(ecm.date_created) + 1 as week, count(ecm.id) as num_modules
        FROM education_completed_module ecm
        LEFT JOIN user u
        ON ecm.user_id = u.id
        WHERE ecm.date_created >= now() - interval 49 day
        AND u.institution_id <> 90
        GROUP BY week(ecm.date_created)
        '''
        num_referrals_query = '''
        SELECT week(rr.date_executed) + 1 as week, count(rr.referred_id) as num_refer
        FROM referral_relationship rr
        LEFT JOIN user u
        ON rr.referred_id = u.id
        WHERE rr.date_executed >= now() - interval 49 day
        AND u.institution_id <> 90
        GROUP BY week(rr.date_executed)
        '''
        
    if group == "zogo":
        num_days_query = '''
        SELECT week, sum(num_days) / count(*) as avg_num_days
        FROM
        (
        SELECT u.id, week(ecm.date_created) + 1 as week, count(DISTINCT date(ecm.date_created)) as num_days
        FROM user u
        LEFT JOIN education_completed_module ecm
        ON u.id = ecm.user_id
        WHERE (ecm.date_created IS NOT NULL OR ecm.date_created IS NULL)
        AND ecm.date_created >= now() - interval 49 day
        AND u.institution_id = 90
        GROUP BY u.id, week(ecm.date_created)
        ORDER BY u.id
        ) AS num_days_tab
        GROUP BY week
        '''
        num_modules_query = '''
        SELECT week(ecm.date_created) + 1 as week, count(ecm.id) as num_modules
        FROM education_completed_module ecm
        LEFT JOIN user u
        ON ecm.user_id = u.id
        WHERE ecm.date_created >= now() - interval 49 day
        AND u.institution_id = 90
        GROUP BY week(ecm.date_created)
        '''
        num_referrals_query = '''
        SELECT week(rr.date_executed) + 1 as week, count(rr.referred_id) as num_refer
        FROM referral_relationship rr
        LEFT JOIN user u
        ON rr.referred_id = u.id
        WHERE rr.date_executed >= now() - interval 49 day
        AND u.institution_id = 90
        GROUP BY week(rr.date_executed)
        '''
    
    day_tab = query(num_days_query)
    mod_tab = query(num_modules_query)
    ref_tab = query(num_referrals_query)
    
    # Merge tables
    df = pd.merge(df_ga, day_tab, on='week')
    df = pd.merge(df, mod_tab, on='week')
    df = pd.merge(df, ref_tab, on='week')
    
    # Drop the first week
    df = df.iloc[1:]
    
    #Metric calculation
    df['modules_per_user'] = df['num_modules'] / df['num_users']
    df['referrals_per_user'] = df['num_refer'] / df['num_users']
    df['week'] = df['week'] - df['week'].iloc[-1]
    df = df.drop(columns = ['num_modules', 'num_refer'])
    
    return df
    


# ## Results

# In[5]:


df_all = cohort("all")
df_zogo = cohort("zogo")
df_cu = cohort("cu")


# ### Upload to Google Sheets

# In[6]:


gc = pygsheets.authorize(service_file='client_secrets.json')
sh = gc.open('Zogo Cohort Analysis')
wks = sh[0]
wks.set_dataframe(df_all,(1,1))
wks = sh[1]
wks.set_dataframe(df_zogo,(1,1))
wks = sh[2]
wks.set_dataframe(df_cu,(1,1))


# In[ ]:




