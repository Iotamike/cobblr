import time
import zmq
from zmq.asyncio import Poller

from ..cobblr_debug import db_print


# This class contains all the functionality for the zeroMQ dealer
class CobblrDealer:
    def __init__(self, queue, context, dlr_name, port, pipe):

        # register input objects
        self.context = context
        self.port = port
        self.dlr_name = str.encode(dlr_name)
        self.pipe = pipe

        # register CobblrRoutine objects
        self.method = self.run_loop
        self.queue = queue
        self.ongoing = True

        # create 0MQ dealer object
        self.dealer = self.context.socket(zmq.DEALER)
        while True:
            try:
                self.dealer.connect("tcp://localhost:%s" % self.port)
            except zmq.ZMQError as e:
                db_print("Can't connect to dealer: %s" % e)
            break

        # create and register poller
        self.poller = Poller()
        self.poller.register(self.dealer, zmq.POLLIN)
        self.poller.register(self.pipe, zmq.POLLIN)

        # create housekeeping objects
        self.in_queue = []
        self.out_queue = []

    async def start(self):
        await self.queue.put(self.run_method())

    async def run_method(self):
        if not self.ongoing:
            return 0
        await self.method()
        if self.ongoing:
            await self.queue.put(self.run_method())

    def end(self):
        if self.ongoing:
            self.ongoing = False
        else:
            db_print("task is already ending")

    async def run_loop(self):
        poll_in = await(self.poller.poll())
        event = dict(poll_in)

        if self.dealer in event:
            message = await self.dealer.recv_multipart()
            db_print("dealer recv: %s" % message)
            self.in_queue.append(message)
        if self.pipe in event:
            message = await self.pipe.recv_multipart()
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
            elif message[0] == b"CONN_ACK":
                db_print("send Conn_Ack")
                self.out_queue.append([b"CONN_ACK", self.dlr_name])

        # handle outbound queue
        if len(self.out_queue) > 0:
            # for each message in the out_queue
            for out_msg in self.out_queue:
                # send the message to the address
                await self.dealer.send_multipart(out_msg)

            self.out_queue = []

        # handle inbound queue
        if len(self.in_queue) > 0:
            for inbound in self.in_queue:
                db_print(inbound)
                await self.pipe.send_multipart(inbound)

            self.in_queue = []

    # --------------------- #
    # Send Method           #
    # --------------------- #
    def send_out(self, message):
        if type(message) == list:
            self.out_queue.append([self.dlr_name] + message)
        else:
            self.out_queue.append([self.dlr_name] + [message])