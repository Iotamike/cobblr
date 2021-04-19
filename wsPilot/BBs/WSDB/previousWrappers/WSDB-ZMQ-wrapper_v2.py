import zmq
import time
import sys

import socketio
import uuid
from eventlet import event
import eventlet
eventlet.monkey_patch()

import json
from ast import literal_eval
from eventlet.timeout import Timeout
from flask import abort
import numpy
events = {}

if len(sys.argv) > 1 :
    port = sys.argv[1] # port to talk to wrapped service
    newport = sys.argv[2] # port to talk to another ZMQ wrapper

context = zmq.Context()
socket = context.socket(zmq.REP)
#socket.setsockopt(zmq.LINGER, 0)
socket.bind("tcp://127.0.0.1:%s" % newport)


socketio = socketio.Client()
socketio.connect("http://localhost:%s" % port)


@socketio.on('response')
def response_handler(args):
    on_wsData_Response(args)        

def on_wsData_Response(data):
    print('data received')
    
    try:
        e = events[uuid.UUID(data['uuid'])]
    
        data = json.dumps(data)
        #socket.send(data.encode()) # data sent back to requester
        e.send(data)
    except KeyError:
        pass
while True:

    #wait for next request from another ZMQ wrapper
    print("listening")
    message = socket.recv().decode()
    print("request received")
    print(message)
    res = message.split(", ")
    #res[1] is the json request 
    
    
    jrequest = literal_eval(res[1])
    

    u = uuid.UUID(jrequest['uuid'])
    #socketio = scoketio.Client()
    #socketio.connect("http://localhost:%s" % port)
    socketio.emit(res[0],res[1])

    timeout = Timeout(60)
    try:
        e = events[u] = event.Event()
        print('waiting')
        
        resp = e.wait()
    except Timeout:
        print("timeout")
        abort(504)
    finally:
        events.pop(u,None)
        timeout.cancel()
    data =json.loads(resp)
    data = data['data']
    #socketio.disconnect()
    print(data)
    socket.send(resp.encode())
