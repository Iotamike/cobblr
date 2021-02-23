
import zmq

from HandlerThreads.QueueHandler import QueueHandler
from cobblr_static_functions import zpipe
from cobblr_debug import db_print

from config import AppType
"""
To Do:

Need XPUB & XSUB sockets:
                        methods for creating & destroying handler threads
                        method for requesting new XSUB handler for particular channel

Need a 'connection manager' - this should store:
                                                name
                                                information
                                                latest status
                                                time since last 'heartbeat'
                                                port numbers
port pipes should be stored locally (internal to appropriate threads) and addressed by name
                        

                                        
Method for creating requesting and making connections between clients
                                                                server side connection requests
                                                                client side connection requests
                                                                client side connection 
                                                                
Implement logging
Need methods for logging transactions, errors, (use logging package)
                        write to file
                        method for reading and sending data

Overhead 'Manager' thread class to manage higher level functionality

Simple API for passing data to and from:
                                        REST API
                                        MQTT
                                        Internal address book

                                Global Address book

                                General 'message' class

"""


class AddressBook:
    def __init__(self, app_type):
        self.type = app_type
        self.dealers = {}
        self.dealer_threads = {}
        self.router_thread = None
        self.ports = {}

    def router_exists(self):
        if self.router_thread is None:
            return False
        else:
            return True

    def get_dealers(self):
        return [*self.dealer_threads]

    def get_dealer_pipe(self, dealer_name):
        try:
            thread = self.dealers[dealer_name]
        except IndexError as e:
            db_print(e)
            return 1
        return thread

    def get_dealer_thread(self, dealer_name):
        try:
            thread = self.dealer_threads[dealer_name]
        except IndexError as e:
            db_print(e)
            return 1
        return thread

    def check_dealers(self):
        if [*self.dealers] == [*self.dealer_threads]:
            return True
        else:
            return False

#    def get_next_port(self):
        # need to crack this one


class CobblrBroker:
    def __init__(self):
        self.context = zmq.Context()
        self.pipe = zpipe(self.context)
        self.handler = QueueHandler(self.context, "service", AppType.SERVICE_APP, self.pipe[0])
        self.poller = zmq.Poller()

        self.handler.start()

        self.poller.register(self.pipe[1], zmq.POLLIN)

    def get_connected_clients(self):
        connections = []
        self.pipe[1].send(b"GET_CONNS")
        message = self.pipe[1].recv_multipart()
        for word in message:
            connections.append(word.decode())
        return connections

    def shutdown(self):
        self.pipe[1].send(b"SHUTDOWN")
        self.pipe[1].close()
        self.pipe[0].close()
        self.context.term()

    def get_messages(self):
        messages = []
        while True:

            # create a dictionary to store any polled events from the poller (timeout 100 msecs)
            event = dict(self.poller.poll(100))
            # check if pipe[1] has a message
            if self.pipe[1] in event:
                # receive any message
                message = self.pipe[1].recv_multipart()
                # stop running if there are no handled messages
                if message[0] == b"NO_MSG":
                    break
                else:
                    for word in message[2:]:
                        messages.append(word.decode())

                    messages.append("\t")

            self.pipe[1].send(b"GET_MSG")

        if len(messages) > 0:
            return messages
        else:
            return ["no_msg"]

    def send_message(self, to, message):
        send_message = [b"SEND_TO", str.encode("%s" % to)]
        for word in message:
            send_message.append(str.encode("%s" % word))


class CobblrClient:
    def __init__(self, name):
        self.name = name
        self.context = zmq.Context()
        self.pipe = zpipe(self.context)
        self.handler = QueueHandler(self.context, self.name, AppType.CLIENT_APP, self.pipe[0])
        self.poller = zmq.Poller()

        self.handler.start()

        self.poller.register(self.pipe[1], zmq.POLLIN)

    def get_connected(self):
        connections = []
        self.pipe[1].send(b"GET_CONNS")
        message = self.pipe[1].recv_multipart()
        for word in message:
            connections.append(word.decode())
        return connections

    def shutdown(self):
        self.pipe[1].send(b"SHUTDOWN")
        self.pipe[1].close()
        self.pipe[0].close()
        self.context.term()

    def register(self):
        self.pipe[1].send(b"REGISTER")

    def request_connection(self, to):
        self.pipe[1].send_multipart([b"RQ_ROUTE", str.encode("%s" % to)])

    def get_messages(self):
        messages = []
        while True:

            # create a dictionary to store any polled events from the poller (timeout 100 msecs)
            event = dict(self.poller.poll(100))
            # check if pipe[1] has a message
            if self.pipe[1] in event:
                # receive any message
                message = self.pipe[1].recv_multipart()
                # stop running if there are no handled messages
                if message[0] == b"NO_MSG":
                    break
                else:
                    for word in message[2:]:
                        messages.append(word.decode())

                    messages.append("\t")

            self.pipe[1].send(b"GET_MSG")

        if len(messages) > 0:
            return messages
        else:
            return ["no_msg"]

    def send_message(self, to, message):
        send_message = [b"SEND_TO", str.encode("%s" % to)]
        for word in message:
            send_message.append(str.encode("%s" % word))

        self.pipe[1].send_multipart(send_message)



