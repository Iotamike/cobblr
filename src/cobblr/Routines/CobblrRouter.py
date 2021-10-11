
import zmq
from zmq.asyncio import Poller

from ..cobblr_debug import db_print


# This class contains all the functionality for the zeroMQ router
class CobblrRouter:
    def __init__(self, queue, context, name, port, pipe):

        # register input objects
        self.context = context
        self.name = name
        self.port = port
        self.pipe = pipe

        # register CobblrRoutine objects
        self.method = self.first_run
        self.queue = queue
        self.ongoing = True

        # create housekeeping objects
        self.router = None
        self.poller = None
        self.in_queue = []
        self.out_queue = []
        self.address_book = {}

    async def start(self):
        await self.queue.put(self.run_method())

    async def run_method(self):
        await self.method()
        if self.ongoing:
            await self.queue.put(self.run_method())

    def end(self):
        if self.ongoing:
            self.ongoing = False
        else:
            db_print("task is already ending")

    async def first_run(self):
        # create 0MQ router
        self.router = self.context.socket(zmq.ROUTER)
        self.router.bind("tcp://*:%s" % self.port)

        # create async poller
        self.poller = Poller()
        self.poller.register(self.router, zmq.POLLIN)
        self.poller.register(self.pipe, zmq.POLLIN)

        self.method = self.run_loop

    async def run_loop(self):
        poll_in = await self.poller.poll()
        event = dict(poll_in)

        if self.router in event:
            message = await self.router.recv_multipart()
            db_print("router recv: %s" % message)
            address = message[0]
            name = message[2]

            """
            NEEDS:
                self.check_address_book
                self.update_address_book
                self.raise_address_error
            """
            self.address_book[name] = address
            self.in_queue.append(message[1:])

        if self.pipe in event:
            message = await self.pipe.recv_multipart()
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
                    continue
                except IndexError as e:
                    db_print("problem with queue response formation \n"
                             "response should be [name, msg1, msg2, ... msgN] \n"
                             " %s" % e)
                    continue

                # try and get the message
                try:
                    message = response[1:]
                except IndexError as e:
                    db_print("problem with queue response formation \n"
                             "response should be [name, msg1, msg2, ... msgN] \n"
                             " %s" % e)
                    continue

                # send the message to the address
                await self.router.send_multipart([address] + message)

            self.out_queue = []

        # handle inbound queue
        if len(self.in_queue) > 0:
            for inbound in self.in_queue:
                try:
                    name = inbound[0]
                    message = inbound[1:]
                except IndexError as e:
                    db_print("problem with inbound message %s" % e)
                    continue
                db_print(message)
                await self.pipe.send_multipart(inbound)

            self.in_queue = []

    def send_out(self, name, message):
        if type(message) == list:
            self.out_queue.append([name] + message)
        else:
            self.out_queue.append([name] + [message])