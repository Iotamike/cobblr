from flask import Flask, request, jsonify, json, request
from flask_cors import CORS, cross_origin
import sys
import pandas as pd

from helperfunctions import getLatestOperator
from helperfunctions import getLatestJob
from helperfunctions import getRunningPeriods

app = Flask(__name__)

port = sys.argv[1]

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

methods = ('GET', 'POST')

metric_readers = {}

def add_reader(name,reader):
    metric_readers[name] = reader

def _series_to_response(df, target):
    if df.empty:
        return {'target': '%s' % (target),
                'datapoints': []}

    sorted_df = df.dropna().sort_index()

    try:
        timestamps = (sorted_df.index.astype(pd.np.int64) // 10 ** 6).values.tolist() # New pandas version
    except:
        timestamps = (sorted_df.index.astype(pd.np.int64) // 10 ** 6).tolist()

    values = sorted_df.values.tolist()

    return {'target': '%s' % (target),
            'datapoints': list(zip(values, timestamps))}


@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    return jsonify(list(metric_readers))

@app.route('/query', methods=methods)
@cross_origin()
def getHistoricalData():

    req = request.get_json()

    targets = req['targets']
    target = targets[0].get('target','')
    print(target)
    startDate = pd.Timestamp(req['range']['from']).to_pydatetime()
    rawendDate = pd.Timestamp(req['range']['to']).to_pydatetime()
    endTimeStamp = rawendDate.timestamp()
 
    if target == 'Operator':
        r = getLatestOperator(startDate,rawendDate)
        resp = {'target': 'Operator', 'datapoints':[[r,endTimeStamp]]}
    
    elif target == 'CurrentJob':
        r = getLatestJob(startDate,rawendDate)
        resp = {'target': 'Operator', 'datapoints':[[r,endTimeStamp]]}
    elif target == 'runningPeriods':
        r = getRunningPeriods(startDate,rawendDate)
        resp = _series_to_response(r,'getRunningPeriods')
    else:
        print('target is not operator')
        resp = {}
    return jsonify([resp])

if __name__ == '__main__':

    add_reader('Operator',getLatestOperator)
    add_reader('CurrentJob', getLatestJob)
    add_reader('runningPeriods', getRunningPeriods)
    app.run(host='0.0.0.0', port=port, debug=True)
