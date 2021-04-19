
import sys

sys.path.append('/home/ec2-user/wrapped_WS_Pilot/BBs/cobblr')
from flask import Flask, request, jsonify, Response
from flask_cors import CORS, cross_origin
#from flask_socketio import SocketIO, emit
import json
import zmq
from cobblr import zpipe, AppType, QueueHandler
import time
from queue import Queue
from threading import Thread

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
methods = ('GET', 'POST')

#socketio = SocketIO(app)
q = Queue()
qOut = Queue()
#NAME = "client_app_two"

if len(sys.argv) > 1 :
    myport = sys.argv[1] # port to talk to wrapped service
    name= sys.argv[2] # client app name
@app.route('/query', methods=methods)
@cross_origin()
def makeRequest():
    print(request)
    json = request.get_json()
    
    print(["/query",json])
    q.put(["/query",json])
    res = qOut.get()
    data = res[4]
    data = data.decode("UTF-8")
    
    print("got back for thread")
    print(data)
    #return Response(data, mimetype='application/json')
    return Response(data)
@app.route('/search', methods=methods)
@cross_origin()
def requestMetrics():
    json = request.get_json()

    print(json)

    q.put(["/search",json])
    res = qOut.get()
    data = res[4]
    data = data.decode("UTF-8")
    print(data)
    return Response(data, mimetype='application/json')

@app.route('/', methods=methods)
@cross_origin()
def requestRoot():
    print(json)

@app.route('/annotations', methods=methods)
@cross_origin()
def requestAnnotations():
    print(annotations)

def client_main(in_q, out_q):

    # create ZMQ context
    context = zmq.Context()

    # creates a list of two connected zmq 'pair' sockets
    # address with api_pipe[0], api_pipe[1]
    #
    # this will be used to pass commands to and from the wrapper API
    api_pipe = zpipe(context)

    # create a wrapper queue handler
    # QueueHandler:
    #   context -> a zeroMQ context
    #   "service_app" -> this is the address of this endpoint
    #   AppType.SERVICE_APP ->  can be either .SERVICE_APP or .CLIENT_APP
    #                           there can be only one .SERVICE_APPs but several .CLIENT_APPs
    #   api_pipe[0] -> this is one end of the pipe to pass commands to and from the wrapper
    queue_thread = QueueHandler(context, name, AppType.CLIENT_APP, api_pipe[0])
    # start the queue handler thread
    queue_thread.start()

    poller = zmq.Poller()
    poller.register(api_pipe[1], zmq.POLLIN)

    msg_print_queue = []
    alive = True
    more_data = True

    # required to initialise the client - service communication
    api_pipe[1].send(b"REGISTER")

    while more_data:
        #print("requesting messages:")
           
        api_pipe[1].send(b"GET_MSG")
        event = dict(poller.poll(100))
        if api_pipe[1] in event:
            message = api_pipe[1].recv_multipart()
         #   print(message)
            if message[0] != b"NO_MSG":
                more_data = False
         #   else:
         #       print(message)


    # if you break this loop, the code will exit (messily)
    # all sockets will be closed
    # I will implement proper shutdown soon
    while True:
        #print("iterando")
        if not alive:
            print("not alive")
            break
        
        # create a dictionary to store any polled events from the poller (timeout 100 msecs)
        #print("antes de hacer poll")
        event = dict(poller.poll(100))
        #print("antes de checar evento")
        if api_pipe[1] in event:
            message = api_pipe[1].recv_multipart()
            #print("from %s recv: %s" % (NAME, message))
         #   print("Ya me llego algo!")
         #   print(message)
            if message[0] != b"NO_MSG":
                more_data = False
          #      print("diferente a no message")
                if len(message) > 3 and message[3] == b"REPLY":
                    print("sending back")
                    print(message)
                    out_q.put(message)

    
       # print("antes de checar cola")
        
        if (in_q.qsize() > 0):
            print("la cola ya tiene algo")
            grafanaReq = in_q.get()
            print("got request")
            print(grafanaReq)
            reqType = grafanaReq[0]
            grafanaString = json.dumps(grafanaReq[1])
            api_pipe[1].send_multipart([b"SEND_TO", b"service", b"REQUEST", bytes(reqType,"ascii"),bytes(grafanaString,encoding="ascii")])
            in_q.task_done()
            more_data =True


        time.sleep(0.05)
        
        if more_data:
            #print("did a get messg")
            api_pipe[1].send(b"GET_MSG")

            #print("sending back")
            #out_q.put(grafanaReq)
        else:
            time.sleep(2)
        #    api_pipe[1].send_multipart([b"SEND_TO", b"service", b"REQUEST", b"DATA_PLEASE"])
            more_data = True

    context.term()


if __name__ == '__main__':

    p = Thread(target=client_main, args=(q,qOut,))
    p.start()
    print(myport)
    app.run(host='0.0.0.0', port=myport, debug=True)
    q.join()
    qOut.join()


