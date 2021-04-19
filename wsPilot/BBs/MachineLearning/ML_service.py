from flask import Flask, Response, abort, request
from flask import jsonify, json
from flask_cors import CORS, cross_origin
import datetime
import requests
import sys
import pandas as pd
import jpype as jp
from modelDeserialisation_jpype import predictJobDuration
from historicalSensorsDataSource import getInstance
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS']='Content-Type'

methods = ('GET', 'POST')

port = sys.argv[1]
metric_readers={}

path = "/home/ec2-user/weka-3-8-5/weka.jar"
jp.startJVM(jp.getDefaultJVMPath(), "-Djava.class.path=" + path)

if not jp.isThreadAttachedToJVM():
    jp.attachThreadToJVM()



def add_reader(name, reader):
    metric_readers[name] = reader




@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    return jsonify(list(metric_readers))


@app.route('/query',methods=methods)
@cross_origin()
def getPrediction():

    req = request.get_json()
    startDate = pd.Timestamp(req['range']['from']).to_pydatetime()
    rawendDate = pd.Timestamp(req['range']['to']).to_pydatetime()
    endTimeStamp = rawendDate.timestamp()
    target = req['targets']
    print(rawendDate)

    instance = getInstance(str(rawendDate))
    prediction = predictJobDuration(instance)

    response = {'target': 'getPredictedJobTime', 'datapoints':[[prediction,endTimeStamp]]}

    return jsonify([response])

if __name__ == '__main__':
    
    add_reader('getPredictedJobTime', getPrediction)
    app.run(host='0.0.0.0', port= port, debug=True)
