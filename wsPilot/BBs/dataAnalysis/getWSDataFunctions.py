# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 15:24:53 2020

@author: ezzgm
"""

import requests
import datetime
import pandas as pd
import numpy as np
#from socketIO_client import SocketIO, LoggingNamespace
import socketio
from eventlet import event
from eventlet.timeout import Timeout
import eventlet
eventlet.monkey_patch()

import uuid

htmlAddress = "https://admin.moninamo.com/api/v1/devices/3C71BF104D00/stats?since="

startDate = "2019-11-15 00:00:00"
endDate = "2019-11-15 23:59:59"
wsData = {}

#socketIO = SocketIO('localhost', 8000, LoggingNamespace)




def getWSData(htmlAddress,startDate,endDate):
    
    #startTimeStamp = datetime.datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S").timestamp()
    #endTimeStamp = datetime.datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S").timestamp()
    startTimeStamp = startDate.timestamp()
    endTimeStamp = endDate.timestamp()

    endDateReached = 0
    hA = htmlAddress + str(int(startTimeStamp))
    data = pd.DataFrame()
    print(hA)
    
    while hA is not None and endDateReached == 0:
        resp = requests.get(url=hA)
        results = resp.json() 
        
        temp = pd.io.json.json_normalize(results['results'])
        
        
        
        if (temp.size > 0):
            
            finalDate = temp['sampled_at'].iloc[-1]
            print(finalDate)
            
            if (endTimeStamp > datetime.datetime.strptime(finalDate, "%Y-%m-%d %H:%M:%S").timestamp()):
                endDateReached = 0
            else:
                endDateReached = 1
            data = data.append(temp, ignore_index=True)
        
        #print(data)
        hA = results['links']['next']
        print("next: ",hA)

        data = data[pd.to_datetime(data['sampled_at'], utc=True) < pd.to_datetime(endDate)]
    
    print(data)
    return data

def calculateEventsDuration(df):

    i=0
    resultPos=0
    res = pd.DataFrame(columns=['event', 'startTime', 'endTime', 'duration'])

    while i < len(df)-2:

        if df['opto_input_1'].iloc[i] == 1:
            j = i+1
            startTime = pd.to_datetime(df['sampled_at'].iloc[i])
            #print(startTime, "running")
            while j< len(df)-1 & df['opto_input_1'].iloc[j] == 1:
                j = j+1

            endTime = pd.to_datetime(df['sampled_at'].iloc[j])

            totalTime = (endTime-startTime).seconds #difference in seconds
            
            res.loc[resultPos] = ['running', startTime,endTime,totalTime]
            
            i = j
            resultPos = resultPos + 1
        else: # case the machine is not running
            if df['opto_input_2'].iloc[i] == 1:  #loading
                
                j = i+1
                startTime = pd.to_datetime(df['sampled_at'].iloc[i])
                #print(startTime, "loading")
                while j < len(df)-1 & df['opto_input_2'].iloc[j] == 1 & df['opto_input_1'].iloc[j] == 0:
                    j = j+1

                endTime = pd.to_datetime(df['sampled_at'].iloc[j])

                totalTime = (endTime-startTime).seconds  #difference in sec

                res.loc[resultPos] = ['loading', startTime,endTime,totalTime]
                i = j
                resultPos = resultPos + 1
            else:  # case the machine is idle

                j = i+1
                startTime = pd.to_datetime(df['sampled_at'].iloc[i])
                #print(startTime, "idle")
                
                while j < len(df)-1 & df['opto_input_1'].iloc[j] == 0 & df['opto_input_2'].iloc[j] == 0:
                    j = j+1
                    #print(j)

                endTime = pd.to_datetime(df['sampled_at'].iloc[j])

                totalTime = (endTime-startTime).seconds #difference in secs

                res.loc[resultPos] = ['idle', startTime,endTime,totalTime]
                i = j
                resultPos = resultPos + 1
    return(res)


def getHistoricalEvents(e, ts_range):

    startDate = ts_range['$gt']
    endDate = ts_range['$lte']
    df = getWSData(htmlAddress,startDate,endDate)
    e = int(e)
    eventToFilter = np.where(e==1, 'running',np.where(e==2, 'loading', 'idle'))
    events = calculateEventsDuration(df)

    

    noDays = pd.to_datetime(endDate) - pd.to_datetime(startDate)

    freq = np.where(noDays> pd.Timedelta(1,'D'), 'days', 'hours')


    #df_Runs = df[df['opto_input_1'] == 1]
    events = events[events['event'] == eventToFilter]


    if freq == 'hours':
        events['timeStamp'] = pd.to_datetime(events['startTime'])
        events['timeStamp'] = events['timeStamp'].dt.floor('1H')


        #df.set_index(df['timeStamp'])
        result = events.groupby([pd.to_datetime(events['timeStamp'])])['duration'].sum().reset_index()
        result['duration'] = result['duration']/60 # convert to minutes
        #dat = result.index.get_level_values(0)
        #stamp = result.index.get_level_values(1)
        #result.index = [datetime.datetime.strptime(str(dat)+" "+str(stamp),"%Y-%m-%d %H")]



        ts = result['timeStamp']
    else:
        result = events.groupby([pd.to_datetime(df['sampled_at']).dt.date])['duration'].sum().reset_index()

        ts = pd.to_datetime(result['sampled_at'])



    #ts = pd.to_datetime(result['sampled_at'], utc=True)

    return(pd.Series(result['duration'].tolist(), index=ts).to_frame('value'))


def getHistoricalRuns(freq, ts_range, columns, data):

    startDate = ts_range['$gt']
    endDate = ts_range['$lte']
    df = pd.DataFrame(data)
    df.columns = columns

    events = calculateEventsDuration(df)

    #print(events[events['event']=='running'])

    noDays = pd.to_datetime(endDate) - pd.to_datetime(startDate)

    freq = np.where(noDays> pd.Timedelta(1,'D'), 'days', 'hours')


    #df_Runs = df[df['opto_input_1'] == 1]
    events = events[events['event'] == 'running']


    if freq == 'hours':
        events['timeStamp'] = pd.to_datetime(events['startTime'])
        events['timeStamp'] = events['timeStamp'].dt.floor('1H')
    

        #df.set_index(df['timeStamp'])
        result = events.groupby([pd.to_datetime(events['timeStamp'])])['duration'].sum().reset_index()
        result['duration'] = result['duration']/60 # convert to minutes
        #dat = result.index.get_level_values(0)
        #stamp = result.index.get_level_values(1)
        #result.index = [datetime.datetime.strptime(str(dat)+" "+str(stamp),"%Y-%m-%d %H")]

        
        
        ts = result['timeStamp']
    else:
        result = events.groupby([pd.to_datetime(df['sampled_at']).dt.date])['duration'].sum().reset_index()
        
        ts = pd.to_datetime(result['sampled_at'])

    

    #ts = pd.to_datetime(result['sampled_at'], utc=True)

    return(pd.Series(result['duration'].tolist(), index=ts).to_frame('value'))

def getRunningPeriods(freq,ts_range, columns, data):


    startDate = ts_range['$gt']
    endDate = ts_range['$lte']
    result = pd.DataFrame(columns=['event', 'startTime','endTime','duration','averageTemp'])
    df = pd.DataFrame(data)
    

    if(len(df)==0):
        result = pd.Series({}).to_frame('value')
    else:
        df.columns = columns
    
        df['state'] = np.where(df['opto_input_1'] == 1, 'running','loading')
        df.loc[(df['opto_input_1'] == 0) & (df['opto_input_2']== 0), 'state'] = 'idle'

    
        ts = pd.to_datetime(df['sampled_at'], utc=True)
        result = pd.Series(df['state'].tolist(), index=ts).to_frame('value')
    print(result)
    return(result)


def getCurrentCabinetTemperature(freq,ts_range, columns, data):

    
    startDate = ts_range['$gt']
    endDate = ts_range['$lte']

    df = pd.DataFrame(data)
    #df.columns = columns

    #ts = pd.to_datetime(df['sampled_at'], utc=True)

    
    if (len(df)==0):
        result = pd.Series({}).to_frame('value')
    else:
        df.columns = columns
        ts = pd.to_datetime(df['sampled_at'], utc=True)
        result = pd.Series(df['temperature_a_celsius'].tolist(), index=ts).to_frame('value')
    
    
    return(result)
    #return(pd.Series(df['temperature_a_celsius'].tolist(), index=ts).to_frame('value'))

def getCurrentAmbientTemperature(freq,ts_range, columns, data):

    startDate = ts_range['$gt']
    endDate = ts_range['$lte']
    df = pd.DataFrame(data)
    #df.columns = columns

    #ts = pd.to_datetime(df['sampled_at'], utc=True)
    
    if (len(df)==0):
        result = pd.Series({}).to_frame('value')
    else:
        df.columns = columns
        ts = pd.to_datetime(df['sampled_at'], utc=True)
        result = pd.Series(df['temperature_b_celsius'].tolist(), index=ts).to_frame('value')
        print(result)

    return(result)



