
import zmq
import threading
from cobblr_debug import db_print


# This class overrides threading.Thread in order to allow threading off sockets easily
# XSUB socket bound to accept connections for channel communications
# ctx = context
# port = integer port number
# N.B. Protected Names
# DO NOT USE: self.name
class XsubChannelThread(threading.Thread):
    def __init__(self, ctx, port, pipe):
        super(XsubChannelThread, self).__init__()
        self.ctx = ctx
        self.port = port
        self.xsub = ctx.socket(zmq.XSUB)
        self.pipe = pipe
        self.alive = True

    def run(self):

        self.xsub.bind("tcp://*:%s" % self.port)

        db_print("xsub_started")

        poller = zmq.poller()
        poller.register(self.xsub, zmq.POLLIN)

        while self.alive:
            event = dict(poller.poll(100))
            if self.xsub in event:
                # handle inboard messages here
                continue

            if self.pipe in event:
                # handle pipe api messages here
                continue

        self.xsub.close()