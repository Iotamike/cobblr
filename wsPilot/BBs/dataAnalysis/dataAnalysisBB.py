from eventlet import event
import eventlet
eventlet.monkey_patch()
from eventlet.timeout import Timeout
from flask import Flask, Response, abort, request
#from socketIO_client import SocketIO, LoggingNamespace
from flask import request, jsonify, json
from flask_cors import CORS, cross_origin
import socketio
import pandas as pd
import numpy as np
import zmq
import sys
import uuid
from ast import literal_eval

import getWSDataFunctions

from getWSDataFunctions import  getCurrentCabinetTemperature
from getWSDataFunctions import getCurrentAmbientTemperature
from getWSDataFunctions import getRunningPeriods
app = Flask(__name__)

port = sys.argv[1]
newport = sys.argv[2]
#socketio = socketio.Client()
#socketio.connect('http://localhost:8000')
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:%s" % newport)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

methods = ('GET', 'POST')

metric_finders= {}
metric_readers = {}
annotation_readers = {}
panel_readers = {}
events = {}
def add_reader(name, reader):
    metric_readers[name] = reader
    print(metric_readers)


def add_finder(name, finder):
    metric_finders[name] = finder


def add_annotation_reader(name, reader):
    annotation_readers[name] = reader


def add_panel_reader(name, reader):
    panel_readers[name] = reader

def on_wsData_Response(args):
    print("Data received")
  #  print(args)
   # print(args['uuid'])
    wsData = args['data']
    try:
    #    print(events)
        e= events[uuid.UUID(args['uuid'])]
     #   print("econtre event eviando datos")
      #  print(args['data'])
        e.send(args)
    except KeyError:
        pass
    #print("sali de callback")

#@socketio.on('response')
#def response_handler(args):
#    on_wsData_Response(args)



@app.route('/', methods=methods)
@cross_origin()
def hello_world():
    print (request.headers, request.get_json())
    return 'Jether\'s python Grafana datasource, used for rendering HTML panels and timeseries data.'

@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    print (request.headers, request.get_json())
    req = request.get_json()

    target = req.get('target', '*')
    print("target: ", target)
    if ':' in target:
        finder, target = target.split(':', 1)
    else:
        finder = target

    if not target or finder not in metric_finders:
        metrics = []
        if target == '*' or target=='':
            print(metric_finders.keys())
            metrics += list(metric_finders.keys()) + list(metric_readers.keys())
            print(metrics)
        else:
            metrics.append(target)
        print("en if: ", metrics)
        return jsonify(metrics)
    else:
        print("en else")
        return jsonify(list(metric_finders[finder](target)))


def dataframe_to_response(target, df, freq=None):
    response = []

    if df.empty:
        return response

    if freq is not None:
        orig_tz = df.index.tz
        #df = df.tz_convert('UTC').resample(rule=freq, label='right', closed='right').mean()
    print(df.__class__.__name__)
    if isinstance(df, pd.Series):
        response.append(_series_to_response(df, target))
    elif isinstance(df, pd.DataFrame):
        for col in df:
            response.append(_series_to_response(df[col], target))
    else:
        abort(404, Exception('Received object is not a dataframe or series.'))

    return response

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

    return {'target': '%s' % (df.name),
            'datapoints': list(zip(values, timestamps))}



@app.route('/query', methods=methods)
@cross_origin(max_age=600)
def query_metrics():
    print(request)
    print (request.headers, request.get_json())
    req = request.get_json()

    results = []

    ts_range = {'$gt': pd.Timestamp(req['range']['from']).to_pydatetime(),
                '$lte': pd.Timestamp(req['range']['to']).to_pydatetime()}

    startDate = ts_range['$gt']
    endDate = ts_range['$lte']

    u = uuid.uuid4()
    print("issuing uuid")
    print(u)
    #socketio.on('response', on_wsData_Response)
    #socketio.emit('aaa', {'uuid':str(u)})
    #socketio.emit('getWSData',{'uuid':str(u),'rawEndDate':str(endDate),'startDate':str(int(startDate.timestamp())), 'endDate':str(int(endDate.timestamp())),'floatEndDate':str(endDate.timestamp())})

    #timeout = Timeout(10)
    #try:
    #    e = events[u] = event.Event()
        #print(e)
        #print('waiting')
    #    resp=e.wait()
    #except Timeout:
        #print('hizo timeout')
    #    abort(504)
    #finally:
    #    events.pop(u,None)
        #print(resp)
        #print("sali del evento")
    #    timeout.cancel()
   # temp = '2020-09-02 14:33:35.874000+00:00'
    message ='/getWSData, {\"uuid\":\"'+str(u)+'\",\"rawEndDate\":\"'+str(endDate)+'\",\"startDate\":\"'+str(int(startDate.timestamp()))+'\",\"endDate\":\"'+str(int(endDate.timestamp()))+'\",\"floatEndDate\":\"'+str(endDate.timestamp())+'\"}'
    socket.send(message.encode())
    resp = socket.recv().decode()
    resp = literal_eval(resp)
    #print(resp)
    #print(type(resp))
    #resp = literal_eval(resp)
    #print("call target")
    
    if 'intervalMs' in req:
        freq = str(req.get('intervalMs')) + 'ms'
    elif 'by' in req:
        freq = str(req.get('by'))
    else:
        freq = None

    for target in req['targets']:
        print(target.get('target',''))
        if ':' not in target.get('target', ''):
            abort(404, Exception('Target must be of type: <finder>:<metric_query>, got instead: ' + target['target']))

        req_type = target.get('type', 'timeserie')

        finder, t = target['target'].split(':', 1)

        if (len(resp) > 0):
            query_results = metric_readers[target['target']](t, ts_range,resp['columns'],resp['data'])
     
            if req_type == 'table':
                results.extend(dataframe_to_json_table(t, query_results))
            else:
                results.extend(dataframe_to_response(t, query_results, freq=freq))
        else:
            results = "[]"
    return jsonify(results)




events = {}

@app.route('/', defaults={'path': '/'})
@app.route('/<path:path>')
def proxy(path):

    u = uuid.uuid4()

    req = {
        'uuid': u,
        'method': request.method,
        # etc.
    }

    socketio.emit('getWSData', req, callback=response)

    timeout = Timeout(10)
    try:
        e = events[u] = event.Event()
        resp = e.wait()
    except Timeout:
        abort(504)
    finally:
        events.pop(u, None)
        timeout.cancel()

    response = list(resp)
    return response


if __name__ == '__main__':
    #socketio.run(app, debug=True)
    
    #add_reader('sine_wave:24', get_sine)
    add_reader('currentCabinetTemperature:24', getCurrentCabinetTemperature)
    add_reader('currentAmbientTemperature:24', getCurrentAmbientTemperature)
    add_reader('getRunningPeriods:24', getRunningPeriods)
    #add_reader('getHistoricalRuns:24', getHistoricalRuns)
    #add_reader('getHistoricalEvents:1', getHistoricalEvents)
    #add_reader('getHistoricalEvents:2', getHistoricalEvents)
    #add_reader('getHistoricalEvents:3', getHistoricalEvents)


    app.run(host='0.0.0.0', port=port, debug=True)
