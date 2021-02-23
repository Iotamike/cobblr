import zmq
from cobblr import zpipe, AppType, QueueHandler
import time

NAME = "client_app_two"


def client_main():

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
    queue_thread = QueueHandler(context, NAME, AppType.CLIENT_APP, api_pipe[0])
    # start the queue handler thread
    queue_thread.start()

    poller = zmq.Poller()
    poller.register(api_pipe[1], zmq.POLLIN)

    msg_print_queue = []
    alive = True
    more_data = True

    # required to initialise the client - service communication
    api_pipe[1].send(b"REGISTER")

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
            print("from %s recv: %s" % (NAME, message))
            if message[0] == b"NO_MSG":
                more_data = False

        time.sleep(0.05)

        if more_data:
            api_pipe[1].send(b"GET_MSG")
        else:
            time.sleep(10)
            # api_pipe[1].send_multipart([b"SEND_TO", b"service", b"REQUEST", b"DATA_PLEASE"])
            more_data = True

    context.term()

client_main()