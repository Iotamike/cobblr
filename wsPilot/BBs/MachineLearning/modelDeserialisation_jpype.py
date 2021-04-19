# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 11:45:55 2020

@author: ezzgm
"""
import jpype as jp
from historicalSensorsDataSource import getInstance
from jpype.types import *


def predictJobDuration(date):

    #path = "/home/ec2-user/weka-3-8-5/weka.jar"
    #jp.startJVM(jp.getDefaultJVMPath(), "-Djava.class.path=" + path)

    #if not jp.isThreadAttachedToJVM():
    #    jp.attachThreadToJVM()
    
    #BufferedReader = jp.JClass("java.io.BufferedReader")
    #FileReader = jp.JClass("java.io.FileReader")
    Instances = jp.JClass("weka.core.Instances")
    DenseInstance = jp.JClass("weka.core.DenseInstance")
    Atribute = jp.JClass("weka.core.Attribute")
    ArrayList = jp.JClass("java.util.ArrayList")


    nfeatures = 23
    jattributes = ArrayList()
    attributes = []
    values = []
    tempList = ArrayList() 

    #reader = BufferedReader(FileReader("TestSet_final_50_updates copy.arff"))
    #data = Instances(reader)
    #reader.close()
    #data.setClassIndex(data.numAttributes() - 1) # setting class attribute
    



    sd= date
    i = getInstance(sd)



    #collecting attributes

    for index, val in i.iteritems():
        if index == 'Shift' or index == 'Operator':
            #print(index)
            tempList.add(JString(val))
            jattributes.add(Atribute(index, tempList))
            tempList.clear()
        else:
            jattributes.add(Atribute(index))
        #attributes.append(index)
        #values.append(val)



    dataRaw = Instances("TestInstances", jattributes, 0)
    inst = DenseInstance(nfeatures)
    inst.setDataset(dataRaw)


    x=0
    for index, val in i.iteritems():
    
        if index == 'Shift' or index == 'Operator':
            #print(val)
            inst.setValue(x,JString(val))
        elif index != 'ActualBillitTime (s)':
            #print(val)
            inst.setValue(x,JDouble(round(val)))
    
        x = x+1
    #print(inst)
    inst.setDataset(dataRaw)

    dataRaw.add(inst)
    dataRaw.setClassIndex(dataRaw.numAttributes() - 1)
    #print(dataRaw)

    #print(data)

    mapped_classifier = jp.JClass('weka.classifiers.misc.InputMappedClassifier')()
    mapped_classifier.setIgnoreCaseForNames(True)
    mapped_classifier.setTrim(True)
    mapped_classifier.setModelPath("/home/ec2-user/wrapped_WS_Pilot/BBs/MachineLearning/Neural_network_test_model.model")

    for i in range(dataRaw.numInstances()):
        pred = mapped_classifier.classifyInstance(dataRaw.instance(i))
        print(pred)
  

    #print("then weka ones")
    #for i in range(data.numInstances()):
    #    pred = mapped_classifier.classifyInstance(data.instance(i))
    #    print(pred)
    
    #jp.shutdownJVM()

    return pred
