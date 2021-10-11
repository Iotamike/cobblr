
import zmq

from ..config import DEFAULT_XPUB_PORT
from ..cobblr_debug import db_print
from ..cobblr_static_functions import list_or_string_encode

from zmq.asyncio import Poller


class CobblrSubscribe:
    def __init__(self, context, queue, pipe):
        self.context = context
        self.queue = queue
        self.method = self.run_first
        self.ongoing = True
        self.pipe = pipe

        self.subscriber = None
        self.poller = None

    # starts the routine by adding the run_method to the queue
    async def start(self):
        await self.queue.put(self.run_method())

    # ensures the routine continues to run by re-adding itself to the queue
    async def run_method(self):
        await self.method()
        if self.ongoing:
            await self.queue.put(self.run_method())

    async def run_first(self):
        # create 0MQ router
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.connect("tcp://localhost:%s" % DEFAULT_XPUB_PORT)

        # create async poller
        self.poller = Poller()
        self.poller.register(self.subscriber, zmq.POLLIN)

        self.method = self.run_loop

    async def run_loop(self):
        poll_in = await self.poller.poll()
        event = dict(poll_in)

        if self.subscriber in event:
            message = await self.subscriber.recv_multipart()
            db_print("subscriber recv: %s" % message)
            await self.pipe.send_multipart(message)

    def sub(self, topic):
        self.subscriber.subscribe(topic)

    def end(self):
        if self.ongoing:
            self.ongoing = False
            self.subscriber.close(linger=1)
        else:
            db_print("task is already ending")

    def __del__(self):
        del self.subscriber, self.poller

