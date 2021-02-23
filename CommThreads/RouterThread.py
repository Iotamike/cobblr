
import zmq

import threading
from cobblr_debug import db_print


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
                        db_print("problem with inbound message %s" % e)
                    db_print(message)
                    self.pipe.send_multipart(inbound)

                self.in_queue = []

            # shutdown
            if not self.alive:
                break

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
