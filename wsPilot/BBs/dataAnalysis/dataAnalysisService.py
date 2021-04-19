import sys

sys.path.append('/home/ec2-user/wrapped_WS_Pilot/BBs/cobblr')
import uuid
import zmq
from cobblr import zpipe, AppType, QueueHandler
from flask import abort
import time
import requests
import json
#import socketio
#import eventlet
#eventlet.monkey_patch()
# this end is always called 'service'
NAME = "service"


"""
Start the service_test first 
"""
#events={}
if len(sys.argv) > 1 :
    port = sys.argv[1] # port to talk to wrapped service
url = 'http://0.0.0.0:8002'
#socketio = socketio.Client()
#socketio.connect("http://localhost:%s" % port)


#@socketio.on('response')
#def response_handler(args):
#    on_wsData_Response(args)

#def on_wsData_Response(data):
#    print('data received')

#    try:
#        e = events[uuid.UUID(data['uuid'])]

#        data = json.dumps(data)
        #socket.send(data.encode()) # data sent back to requester
#        e.send(data)
#    except KeyError:
#        pass





def service_main():

    # create ZMQ context
    context = zmq.Context()

    # creates a list of two connected zmq 'pair' sockets
    # address with api_pipe[0], api_pipe[1]
    #
    # this will be used to pass commands to and from the wrapper API
    api_pipe = zpipe(context)
    client_api_pipe = zpipe(context)
    # create a wrapper queue handler
    # QueueHandler:
    #   context -> a zeroMQ context
    #   "service_app" -> this is the address of this endpoint
    #   AppType.SERVICE_APP ->  can be either .SERVICE_APP or .CLIENT_APP
    #                           there can be only one .SERVICE_APPs but several .CLIENT_APPs
    #   api_pipe[0] -> this is one end of the pipe to pass commands to and from the wrapper
    queue_thread = QueueHandler(context, NAME, AppType.SERVICE_APP, api_pipe[0])
    # start the queue handler thread
    queue_thread.start()

    #client_thread = QueueHandler(context, "dataAnalysis_app", AppType.CLIENT_APP, client_api_pipe[0])

    #client_thread.start()


    poller = zmq.Poller()
    poller.register(api_pipe[1], zmq.POLLIN)
    #poller.register(client_api_pipe[1], zmq.POLLIN)


    #client_api_pipe[1].send(b"REGISTER")
    msg_print_queue = []
    alive = True
    more_data = True

    # if you break this loop, the code will exit (messily)
    # all sockets will be closed
    # I will implement proper shutdown soon
    while True:

        if not alive:
            break

        # create a dictionary to store any polled events from the poller (timeout 100 msecs)
        event = dict(poller.poll(100))
        if api_pipe[1] in event:
            message = api_pipe[1].recv_multipart()
            print("%s recv: %s" % (NAME, message))
            if message[0] == b"MSG":
                print("recv: %s from %s" % (message[3:], message[2]))
                if message[3] == b"REQUEST":
                    print("sending to data analysis bb")
                    reqType = message[4].decode("ascii")
                    #params = message[5]
                    params = json.loads(message[5].decode("UTF-8"))
                    print(params)
                    response = requests.post(url+reqType,json=params, headers={'Content-type': 'application/json', 'Accept': 'text/plain'})
                    data = response.json()
                    data = json.dumps(data)
                    print("the response is")
                    print(data)
                    #u = uuid.UUID(jrequest['uuid'])

                    #timeout = Timeout(60)
                    #try:
                    #    e = events[u] = event.Event()
                    #    print('waiting')

                    #    resp = e.wait()
                    #except Timeout:
                    #    print("timeout")
                    #    abort(504)
                    #finally:
                    #    events.pop(u,None)
                    #    timeout.cancel()
                    #data =json.loads(resp)
                    #data = data['data']
                    #print(data)

                    api_pipe[1].send_multipart([b"SEND_TO", message[2], b"REPLY", bytes(data, 'ascii'), message[2]])
            if message[0] == b"NO_MSG":
                more_data = False

        time.sleep(0.05)
        if more_data:
            # continuously pull data while there is more
            api_pipe[1].send(b"GET_MSG")
        else:
            # wait for a while for more data
            time.sleep(2)
            more_data = True

    context.term()


service_main()
