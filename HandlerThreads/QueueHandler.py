
import zmq
import threading
from config import AppType, DEFAULT_PORT
from cobblr_debug import db_print
from cobblr_static_functions import zpipe
from CommThreads.DealerThread import DealerThread
from CommThreads.RouterThread import RouterThread


# This class overrides threading.Thread to neatly handle request-response communications
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
        self.dealers = {}  # stores a table of dealer pipe sockets; key is the dealer name string
        self.dealer_threads = {}  # stores a table of dealer_threads; key is the dealer name string
        self.router_thread = "NULL"
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

            # _______________________________ #
            # Handle Router Messages
            # Parses messages and handles any that include keywords
            # N.B. can't use python objects instead of keywords
            # _______________________________ #
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
                    self.send_to(dlr_name, b"CONN_ACK")
                if message[0] == b"RQ_ROUTE":
                    dlr_name = message[1]
                    request = message[2]
                    port_num = message[3]
                    db_print("recv RQ_ROUTE: %s, %s, %s" % (dlr_name.decode(), request.decode(), port_num.decode()))
                    if self.app_type == AppType.CLIENT_APP:
                        db_print("RQ_ROUTE: client app")
                        if self.app_name == request.decode():
                            self.create_dealer(dlr_name.decode(), port_num.decode())
                            self.dealers[dlr_name.decode()].send_multipart([b"ACK_ROUTE",
                                                                           str.encode("%s" % self.app_name),
                                                                           str.encode("%s" % self.router_thread.port)])
                        else:
                            db_print("Routing request: Error - cannot route via CLIENT_APP")
                    elif self.app_type == AppType.SERVICE_APP:
                        db_print("RQ_ROUTE: service app")
                        try:
                            self.dealers[request.decode()].send_multipart(message)
                        except IndexError as e:
                            db_print("Error, dealer %s not found: %s" % (request, e))
                if message[0] == b"ACK_ROUTE":
                    db_print("router recv ACK ROUTE")
                    dlr_name = message[1].decode()
                    port_num = message[2].decode()
                    if dlr_name in self.dealers:
                        db_print("route connection acknowledged")
                    else:
                        self.create_dealer(dlr_name, port_num)
                        self.dealers[dlr_name].send_multipart([b"ACK_ROUTE",
                                                              str.encode("%s" % self.app_name),
                                                              str.encode("NULL")])
                if message[0] == b"CONN_ACK":
                    db_print("connected")
                if message[0] == b"MSG_FROM":
                    self.message_queue.append(message[1:])

            # _______________________________ #
            # Handle Dealer Messages
            # Parses messages and handles any that include keywords
            # N.B. can't use python objects instead of keywords
            # _______________________________ #
            for key, dealer in list(self.dealers.items()):
                if dealer in event:
                    message = dealer.recv_multipart()
                    db_print("dealer %s pipe recv: %s" % (key, message))
                    if message[0] == b"ROUTER_PORT":
                        port_num = int(message[1].decode())
                        self.create_router(port_num)
                        dealer.send_multipart([b"CONNECT", str.encode(self.app_name), str.encode("%s" % port_num)])

            # _______________________________ #
            # Handle API Messages
            # Parses messages and handles any that include keywords
            # N.B. can't use python objects instead of keywords
            # _______________________________ #
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
                    self.dealers["service"].send_multipart([b"REGISTER"])
                if message[0] == b"RQ_ROUTE":
                    request = message[1]
                    rq_message = [b"RQ_ROUTE",
                                  str.encode("%s" % self.app_name),
                                  request,
                                  str.encode("%s" % self.router_thread.port)]
                    if request.decode() in self.dealers:
                        db_print("api RQ dealer recv")
                        self.dealers[request.decode()].send_multipart(rq_message)
                    elif self.app_type == AppType.SERVICE_APP:
                        db_print("Requested dealer is not known")
                    else:
                        db_print("api RQ service recv")
                        self.dealers["service"].send_multipart(rq_message)
                if message[0] == b"SHUTDOWN":
                    self.alive = False

            if not self.alive:
                for key, dealer in list(self.dealer_threads.items()):
                    dealer.shutdown()
                if self.router:
                    self.router_thread.shutdown()

    # --------------------- #
    # Create router         #
    # --------------------- #
    def create_router(self, port_num):
        if self.router:
            db_print("router already exists")
            return 1
        else:
            self.router_thread = RouterThread(self.ctx, port_num, self.router_pipe[0])
            self.router_thread.start()
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
            db_print("invalid destination name %s \n" % e)
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
