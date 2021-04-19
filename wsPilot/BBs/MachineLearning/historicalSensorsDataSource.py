# -*- coding: utf-8 -*-outer
"""
Created on Mon Oct  5 12:38:15 2020

@author: ezzgm
"""

import pandas as pd
from datetime import datetime

def getInstance(selectedDate):

    df1 = pd.read_excel ('NN_Final_All_Data.xlsx')
    #print(df1.columns)
    df1 = df1[['opto_input_1','opto_input_2','sampled_at','Billet Nunber', 'Run Time (s) ', 'Load Time (s)', 'Idle Time (s)', 'Program Held (s)']]
    df2 = pd.read_excel ('NN_Final_All_Data.xlsx')
    df2 = df2[['sampled_at','Job No', 'IRN', 'Total Quantity', 'Quantity In Shift', 'Shift', 'ShiftAvergeCycleTime (s)', 'ShiftAvergeLoadTime (s)', 'ShiftAvergeIdleTime (s)', 'ShiftAverageProgramHeld (s)', 'Operator', 'Tools used', 'Vices used', 'Fixture used', 'New Predicted Total Time (s)', 'NewPredictedBillitTime (s)', 'ActualBillitTime (s)']]
     
    
    #selectedDate = str(selectedDate[0:19])
    selectedDate = '2020-05-01 21:58:54'
    selectedDate = datetime.strptime(selectedDate, '%Y-%m-%d %H:%M:%S')
    print("after striping")
    print(selectedDate)
    print(type(selectedDate))
    print(df1["sampled_at"])
    differences = df1["sampled_at"] - selectedDate
    print(differences)
    smallest = min(differences, key=abs)
    print(smallest)
    smallestIndex = differences.tolist().index(smallest)
    
    #### Getting sensor data###
    merge = pd.merge(left = df1, right = df2, how='outer', left_on='sampled_at', right_on='sampled_at')
    merge = merge.drop('sampled_at', 1)
    instance = merge.iloc[smallestIndex]
    print("the instance is")
    print(instance)
    return(instance)

#sd= '2020-04-02T08:30:33Z'
#i = getInstance(sd)
#print(i)
