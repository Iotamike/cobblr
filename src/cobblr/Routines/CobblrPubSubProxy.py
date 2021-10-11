
import zmq
from zmq.asyncio import Poller

from ..cobblr_debug import db_print
from ..config import DEFAULT_XPUB_PORT, DEFAULT_XSUB_PORT


class CobblrPubSubProxy:

    def __init__(self, context, queue, pipe=None):

        self.context = context
        self.pipe = pipe
        self.queue = queue

        self.method = self.run_first

        self.xsub = None
        self.xpub = None

        self.poller = None

        self.ongoing = True

    async def start(self):
        await self.queue.put(self.run_method())

    async def run_method(self):
        if not self.ongoing:
            return 0
        await self.method()
        if self.ongoing:
            await self.queue.put(self.run_method())

    async def run_first(self):

        # create 0MQ XSUB
        self.xsub = self.context.socket(zmq.XSUB)
        self.xsub.bind("tcp://*:%s" % DEFAULT_XSUB_PORT)

        # create 0MQ XPUB
        self.xpub = self.context.socket(zmq.XPUB)
        self.xpub.bind("tcp://*:%s" % DEFAULT_XPUB_PORT)

        self.poller = Poller()
        self.poller.register(self.xsub, zmq.POLLIN)
        self.poller.register(self.xpub, zmq.POLLIN)

        self.method = self.run_loop

    async def run_loop(self):
        poll_in = await self.poller.poll()
        event = dict(poll_in)

        message = [b"no_msg"]

        if self.xsub in event:
            message = await self.xsub.recv_multipart()
            await self.xpub.send_multipart(message)
            db_print("xsub.recv : %s" % message)

        if self.xpub in event:
            message = await self.xpub.recv_multipart()
            await self.xsub.send_multipart(message)
            db_print("xpub.recv : %s" % message)

        if self.pipe:
            try:
                self.pipe.send_multipart(message)
            except zmq.ZMQError as e:
                db_print("problem sending on proxy side pipe: %s" % e)

    def __del__(self):
        self.xsub.unbind()
        self.xsub.unbind()
        self.xsub.close()
        self.xpub.close()
        del self.poller, self.xsub, self.xpub




