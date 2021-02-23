
import zmq
import threading
from cobblr_debug import db_print


# This class overrides threading.Thread in order to allow threading off sockets easily
# XSUB socket bound to accept connections for channel communications
# ctx = context
# port = integer port number
# N.B. Protected Names
# DO NOT USE: self.name
class PubChannelThread(threading.Thread):
    def __init__(self, ctx, port, pipe):
        super(PubChannelThread, self).__init__()
        self.ctx = ctx
        self.port = port
        self.pub = ctx.socket(zmq.PUB)
        self.pipe = pipe
        self.alive = True

    def run(self):

        self.pub.connect("tcp://localhost:%s" % self.port)

        db_print("sub_started")

        poller = zmq.poller()
        poller.register(self.pub, zmq.POLLIN)

        while self.alive:
            event = dict(poller.poll(100))
            if self.pub in event:
                # handle inboard messages here
                # should only see 'sub' messages here "0x01XX"
                continue

            if self.pipe in event:
                # handle pipe api messages here
                # basically just publish messages
                continue

        self.sub.close()
