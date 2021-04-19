import pandas as pd
import datetime
import pytz
import numpy as np


def getLatestOperator(start,end):

    #start.astimezone(tz=None)
    #end.astimezone(tz=None)
    timezone = pytz.timezone("UTC")
    df = pd.read_excel('NN_Final_All_Data.xlsx')
    df['sampled_at'] = pd.to_datetime(df['sampled_at'], utc=True)
    df = df[(df['sampled_at'] > start) & (df['sampled_at'] < end)]
    recentDate = df['sampled_at'].max()
    row = df[df['sampled_at']== recentDate]
    #res = df.iloc[row]['Operator']
    res = row['Operator'].item()
    print(res)    
    return res 

def getLatestJob(start,end):


    df = pd.read_excel('NN_Final_All_Data.xlsx')
    df['sampled_at'] = pd.to_datetime(df['sampled_at'], utc=True)
    df = df[(df['sampled_at'] > start) & (df['sampled_at'] < end)]
    recentDate = df['sampled_at'].max()
    row = df[df['sampled_at']== recentDate]
    
    res = row['Job No'].item()

    return res

def getRunningPeriods(start,end):

    df = pd.read_excel('NN_Final_All_Data.xlsx')
    df['sampled_at'] = pd.to_datetime(df['sampled_at'], utc=True)
    df = df[(df['sampled_at'] > start) & (df['sampled_at'] < end)]

    if (len(df)==0):
        result = pd.Series({}).to_fatem('value')
    else:
        df['state'] = np.where(df['opto_input_1'] == 1, 'running', 'loading')
        df.loc[(df['opto_input_1'] == 0) & (df['opto_input_2']== 0), 'state'] = 'idle'
        ts = pd.to_datetime(df['sampled_at'], utc=True)
        result = pd.Series(df['state'].tolist(), index=ts).to_frame('value')

    return result
