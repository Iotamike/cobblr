
import zmq
from zmq.asyncio import Poller

from ..cobblr_debug import db_print
from ..config import DEFAULT_XPUB_PORT, DEFAULT_XSUB_PORT


class CobblrPubSubProxy:

    def __init__(self, context, pipe):

        self.context = context
        self.pipe = pipe

        # create 0MQ XSUB
        self.xsub = self.context.socket(zmq.XSUB)
        self.xsub.connect("tcp://localhost:%s" % DEFAULT_XSUB_PORT)

        # create 0MQ XPUB
        self.xpub = self.context.socket(zmq.XPUB)
        self.xpub.bind("tcp://*:%s" % DEFAULT_XPUB_PORT)

        self.proxy = None

    def run(self):

        # create 0MQ proxy
        self.proxy = zmq.proxy_steerable(self.xsub, self.xpub, self.pipe, control=None)

    def __del__(self):
        del self.proxy, self.xsub, self.xpub




