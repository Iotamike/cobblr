import zmq

from ..cobblr_debug import db_print
from ..cobblr_static_functions import list_or_string_encode
from ..config import DEFAULT_XSUB_PORT


class CobblrPublish:

    def __init__(self, context):

        self.context = context

        self.ongoing = True

        self.publish = None

    async def start(self):
        # create 0MQ publish socket
        self.publish = self.context.socket(zmq.PUB)
        self.publish.connect("tcp://localhost:%s" % DEFAULT_XSUB_PORT)
        db_print("publish socket connected")

    async def pub(self, topic, message):
        out_msg = list_or_string_encode(message)
        out_topic = list_or_string_encode(topic)
        # N.B. list_or_string_encode always returns a list
        await self.publish.send_multipart(out_topic + out_msg)




