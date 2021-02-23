
import zmq
import threading
from cobblr_debug import db_print


# This class overrides threading.Thread in order to allow threading off sockets easily
# XSUB socket bound to accept connections for channel communications
# ctx = context
# port = integer port number
# N.B. Protected Names
# DO NOT USE: self.name
class SubChannelThread(threading.Thread):
    def __init__(self, ctx, port, pipe):
        super(SubChannelThread, self).__init__()
        self.ctx = ctx
        self.port = port
        self.sub = ctx.socket(zmq.XSUB)
        self.pipe = pipe
        self.alive = True

    def run(self):

        self.sub.connect("tcp://localhost:%s" % self.port)

        db_print("sub_started")

        poller = zmq.poller()
        poller.register(self.sub, zmq.POLLIN)

        while self.alive:
            event = dict(poller.poll(100))
            if self.sub in event:
                # handle inboard messages here
                continue

            if self.pipe in event:
                # handle pipe api messages here
                continue

        self.sub.close()

    def subscribe(self, topic):
        if type(topic) == list:
            for item in topic:
                self.sub.setsockopt(zmq.SUBSCRIBE, str.encode("%s" % item))
        elif type(topic) == str:
            self.sub.setsockopt(zmq.SUBSCRIBE, str.encode("%s" % topic))
        else:
            db_print("Error, need to pass a string or list of strings")
