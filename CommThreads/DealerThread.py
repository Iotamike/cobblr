
import zmq
import threading
from cobblr_debug import db_print


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
                    db_print("send message %s" % message[1:])
                    self.out_queue.append([b"MSG_FROM", self.dlr_name] + message[1:])
                elif message[0] == b"CONNECT":
                    db_print("send connect message")
                    port = message[2]
                    self.out_queue.append([b"CONNECT", self.dlr_name, port])
                elif message[0] == b"RQ_ROUTE":
                    db_print("send rq")
                    self.out_queue.append(message)
                elif message[0] == b"ACK_ROUTE":
                    db_print("send ack_route")
                    self.out_queue.append(message)

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