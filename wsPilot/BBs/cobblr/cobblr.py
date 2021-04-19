"""
    Digital Manufacturing on a Shoestring
    ZeroMQ - In-Service Broker

    Binds zmq xpub-xsub proxy on fixed ports

    Binds zmq router on fixed port

    Listens for shoes_app heartbeats on pub-sub

    Dynamically connects zmq dealer sockets to advertised routers

    ---- Interface to Greg's REST API

    ---- Interface to MQTT client and API

"""

import zmq
import threading
import binascii
import os
import enum

DEBUG = False
DEFAULT_PORT = 53045


def db_print(message):
    if DEBUG:
        print(message)


# Enum to hold application type
# CLIENT_APP = connects to service, standard building block
# SERVICE_APP = opens a connection and routes traffic (exactly how to be determined!)
class AppType(enum.Enum):
    CLIENT_APP = 1
    SERVICE_APP = 2


def zpipe(ctx):
    """
    builds inproc pipe for talking to threads - lifted from zhelpers.py

    :param ctx: context
    :return: two 'PAIR' sockets using OS sockets
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


# relay helper function for proxy
# lifted from monitored_queue in pyzmq
# ins = input socket
# outs = output socket
# sides = side channel socket
# prefix = optional prefix for side channel
# swap_ids = boolean, only True for ROUTER -> ROUTER
def _relay(ins, outs, sides, prefix, swap_ids=False):
    msg = ins.recv_multipart()
    if swap_ids:
        msg[:2] = msg[:2][::-1]
    outs.send_multipart(msg)
    sides.send_multipart([prefix] + msg)


# This class overrides threading.Thread in order to allow threading off sockets easily
# Router sockets will always bind in our scheme
# ctx = context
# port = integer port number
# N.B. Protected Names
# DO NOT USE: self.name
class RouterThread(threading.Thread):
    def __init__(self, ctx, port, pipe):
        super(RouterThread, self).__init__()
        self.ctx = ctx
        self.port = port
        self.pipe = pipe
        self.router = self.ctx.socket(zmq.ROUTER)
        self.alive = True
        self.in_queue = []
        self.out_queue = []
        self.address_book = {}

    # --------------------- #
    # Run Method            #
    # --------------------- #
    def run(self):

        db_print("router started\n")

        self.router.bind("tcp://*:%s" % self.port)

        poller = zmq.Poller()
        poller.register(self.router, zmq.POLLIN)
        poller.register(self.pipe, zmq.POLLIN)

        while self.alive:
            event = dict(poller.poll(100))
            if self.router in event:
                message = self.router.recv_multipart()
                db_print("router recv: %s" % message)
                address = message[0]
                name = message[2]
                self.address_book[name] = address
                self.in_queue.append(message[1:])
            if self.pipe in event:
                message = self.pipe.recv_multipart()
                if message[0] == b"ROUTER_PORT":
                    dlr_name = message[1]
                    port_num = message[2]
                    self.send_out(dlr_name, [b"ROUTER_PORT", port_num])

            # handle outbound queue
            if len(self.out_queue) > 0:
                # for each message in the out_queue
                for response in self.out_queue:

                    # try and get the address by looking up the name
                    try:
                        address = self.address_book[response[0]]
                    except KeyError as e:
                        db_print("Error: no address for that name %s" % e)
                    except IndexError as e:
                        db_print("problem with queue response formation \n"
                                 "response should be [name, msg1, msg2, ... msgN] \n"
                                 " %s" % e)

                    # try and get the message
                    try:
                        message = response[1:]
                    except IndexError as e:
                        db_print("problem with queue response formation \n"
                              "response should be [name, msg1, msg2, ... msgN] \n"
                              " %s" % e)

                    # send the message to the address
                    self.router.send_multipart([address] + message)

                self.out_queue = []

            # handle inbound queue
            if len(self.in_queue) > 0:
                for inbound in self.in_queue:
                    try:
                        name = inbound[0]
                        message = inbound[1:]
                    except IndexError as e:
                        db_print("problem with inbound message form")
                    db_print(message)
                    self.pipe.send_multipart(inbound)

                self.in_queue = []

        self.router.close()

    # --------------------- #
    # Send Method           #
    # --------------------- #
    def send_out(self, name, message):
        if type(message) == list:
            self.out_queue.append([name] + message)
        else:
            self.out_queue.append([name] + [message])

    # --------------------- #
    # Shutdown Method       #
    # --------------------- #
    def shutdown(self):
        self.alive = False


# This class overrides threading.Thread in order to allow threading off sockets easily
# Dealer sockets will always connect in our scheme
# ctx = context
# port = integer port number
# N.B. Protected Names
# DO NOT USE: self.name
class DealerThread(threading.Thread):
    def __init__(self, ctx, port, dlr_name, pipe):
        super(DealerThread, self).__init__()
        self.ctx = ctx
        self.port = port
        self.pipe = pipe
        self.dealer = self.ctx.socket(zmq.DEALER)
        self.alive = True
        self.dlr_name = str.encode(dlr_name)
        self.in_queue = []
        self.out_queue = []

    # --------------------- #
    # Run Method            #
    # --------------------- #
    def run(self):

        self.dealer.connect("tcp://localhost:%s" % self.port)

        db_print("dealer started\n")

        poller = zmq.Poller()
        poller.register(self.dealer, zmq.POLLIN)
        poller.register(self.pipe, zmq.POLLIN)

        while self.alive:
            event = dict(poller.poll(100))
            if self.dealer in event:
                message = self.dealer.recv_multipart()
                db_print("dealer recv: %s" % message)
                self.in_queue.append(message)
            if self.pipe in event:
                message = self.pipe.recv_multipart()
                if message[0] == b"REGISTER":
                    db_print("send register message")
                    self.out_queue.append([b"REGISTER", self.dlr_name])
                elif message[0] == b"SEND":
                    db_print("send message")
                    self.out_queue.append([b"MSG_FROM", self.dlr_name] + message[1:])
                elif message[0] == b"CONNECT":
                    db_print("send connect message")
                    port = message[2]
                    self.out_queue.append([b"CONNECT", self.dlr_name, port])

            # handle outbound queue
            if len(self.out_queue) > 0:
                # for each message in the out_queue
                for out_msg in self.out_queue:

                    # send the message to the address
                    self.dealer.send_multipart(out_msg)

                self.out_queue = []

            # handle inbound queue
            if len(self.in_queue) > 0:
                for inbound in self.in_queue:
                    db_print(inbound)
                    self.pipe.send_multipart(inbound)

                self.in_queue = []

        self.dealer.close()

    # --------------------- #
    # Send Method           #
    # --------------------- #
    def send_out(self, message):
        if type(message) == list:
            self.out_queue.append([self.dlr_name] + message)
        else:
            self.out_queue.append([self.dlr_name] + [message])

    # --------------------- #
    # Shutdown Method       #
    # --------------------- #
    def shutdown(self):
        self.alive = False


# This class overrides threading.Thread to neatly handle communications
# Dealer sockets will always connect in our scheme
# ctx = context
# app_name = application name (string)
# app_type = AppType.CLIENT_APP or AppType.SERVICE_APP Enum
# N.B. Protected Names
# DO NOT USE: self.name
class QueueHandler(threading.Thread):
    def __init__(self, ctx, app_name, app_type, api_pipe):
        super(QueueHandler, self).__init__()
        self.ctx = ctx
        self.app_name = app_name
        self.app_type = app_type
        self.outbound_queue = []
        self.inbound_queue = []
        self.dealers = {}  # stores a table of dealer pipe sockets; key is the dealer name string
        self.dealer_threads = {}  # stores a table of dealer_threads; key is the dealer name string
        self.connected_dealers = []  # stores a list of connected dealers
        self.router_pipe = zpipe(self.ctx)
        self.api_pipe = api_pipe
        self.ports = []  # stores a list of occupied ports - should only be populated by a SERVICE_APP
        self.router = False
        self.poller = zmq.Poller()
        self.poller.register(self.api_pipe, zmq.POLLIN)
        self.message_queue = []
        self.alive = True

    # --------------------- #
    # Run Method            #
    # --------------------- #
    def run(self):

        # check - is this a SERVICE_APP
        if self.app_type == AppType.SERVICE_APP:
            # start the router thread to receive incoming communications
            self.create_router(DEFAULT_PORT)

        # check - is this a CLIENT_APP
        elif self.app_type == AppType.CLIENT_APP:
            self.create_dealer("service", DEFAULT_PORT)
            self.ports.append(DEFAULT_PORT)

        # check - invalid app_type
        else:
            raise Exception("Invalid App Type")

        while True:

            if not self.alive:
                break

            event = dict(self.poller.poll(100))

            if self.router_pipe[1] in event:
                message = self.router_pipe[1].recv_multipart()
                db_print("router pipe recv: %s" % message)
                if message[0] == b"REGISTER":
                    db_print("registering?")
                    dlr_name = message[1]
                    new_port = self.next_port()
                    self.router_pipe[1].send_multipart([b"ROUTER_PORT", dlr_name, str.encode("%s" % new_port)])
                if message[0] == b"CONNECT":
                    db_print("connect: %s" % message)
                    dlr_name = message[1].decode()
                    port_num = int(message[2].decode())
                    self.create_dealer(dlr_name, port_num)
                    db_print("creating deaer with name")
                    db_print(dlr_name)
                    self.send_to(dlr_name, b"CONN_ACK")
                if message[0] == b"ROUTE":
                    dlr_name = message[1]
                    request = message[2].decode
                    port_num = message[3]
                    db_print("Routing request: app %s request com from %s on port %s" % (dlr_name, request, port_num))
                if message[0] == b"CONN_ACK":
                    db_print("connected")
                if message[0] == b"MSG_FROM":
                    self.message_queue.append(message[1:])

            for key, dealer in list(self.dealers.items()):
                if dealer in event:
                    message = dealer.recv_multipart()
                    db_print("dealer %s pipe recv: %s" % (key, message))
                    if message[0] == b"ROUTER_PORT":
                        port_num = int(message[1].decode())
                        self.create_router(port_num)
                        db_print("creating router")
                        dealer.send_multipart([b"CONNECT", str.encode(self.app_name), str.encode("%s" % port_num)])

            if self.api_pipe in event:
                message = self.api_pipe.recv_multipart()
                if message[0] == b"SEND_TO":
                    to = message[1].decode()
                    self.send_to(to, message[2:])
                if message[0] == b"GET_CONNS":
                    con_dealer_list = []
                    for dlr_name in list(self.dealers.keys()):
                        con_dealer_list.append(str.encode(dlr_name))
                    self.api_pipe.send_multipart([b"CONNS"] + con_dealer_list)
                if message[0] == b"GET_MSG":
                    number = len(self.message_queue)
                    if number == 0:
                        self.api_pipe.send(b"NO_MSG")
                    else:
                        message = self.message_queue.pop()
                        self.api_pipe.send_multipart([b"MSG"]+[str.encode("%s" % number)] + message)
                if message[0] == b"REGISTER":
                    db_print("sent register message to dealer")
                    self.dealers["service"].send_multipart([b"REGISTER"])
                if message[0] == b"SHUTDOWN":
                    self.alive = False

    # --------------------- #
    # Create router         #
    # --------------------- #
    def create_router(self, port_num):
        if self.router:
            db_print("router already exists")
            return 1
        else:
            router_thread = RouterThread(self.ctx, port_num, self.router_pipe[0])
            router_thread.start()
            self.ports.append(port_num)
            self.router = True
            self.poller.register(self.router_pipe[1], zmq.POLLIN)

    # --------------------- #
    # Send via dealer       #
    # --------------------- #
    def send_to(self, to: str, message):
        try:
            dealer = self.dealers[to]
        except IndexError as e:
            db_print("invalid destination name \n")
            return 1

        if type(message) == list:
            out_list = []
            for word in message:
                if type(word) == str:
                    out_list.append(str.encode(word))
                elif type(word) == bytes:
                    out_list.append(word)
                else:
                    raise Exception("message must consist of string or byte objects")
            out_msg = [b"SEND"] + out_list
        elif type(message) == str:
            out_msg = [b"SEND"] + [str.encode(message)]
        elif type(message) == bytes:
            out_msg = [b"SEND"] + [message]
        else:
            raise Exception("Input should be a string, byte object\n or a list of byte or string objects")

        try:
            db_print("sending the message though dealer")
            dealer.send_multipart(out_msg)
        except zmq.ZMQError as e:
            db_print("zmq error: %s \n" % e)
            return 1

    # --------------------- #
    # Dealer thread creator #
    # --------------------- #
    def create_dealer(self, dlr_name, port_num):
        db_print("creating dealer %s in app %s on port %s" % (dlr_name, self.app_name, port_num))
        dealer_pipe = zpipe(self.ctx)
        self.dealers[dlr_name] = dealer_pipe[1]
        self.dealer_threads[dlr_name] = DealerThread(self.ctx, port_num, self.app_name, dealer_pipe[0])
        self.dealer_threads[dlr_name].start()
        self.poller.register(dealer_pipe[1], zmq.POLLIN)

    # --------------------- #
    # next port finder      #
    # --------------------- #
    def next_port(self):
        port_num = DEFAULT_PORT
        while True:
            if self.ports.count(port_num) != 0:
                port_num += 1
            else:
                self.ports.append(port_num)
                return port_num

