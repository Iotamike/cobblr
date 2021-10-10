import zmq

from ..cobblr_debug import db_print
from ..cobblr_static_functions import list_or_string_encode
from ..config import DEFAULT_XSUB_PORT


class CobblrPublish:

    def __init__(self, context):

        self.context = context

        self.ongoing = True

        # create 0MQ publish socket
        self.publish = self.context.socket(zmq.PUB)
        self.publish.bind("tcp://*:%s" % DEFAULT_XSUB_PORT)

    def pub(self, topic, message):
        out_msg = list_or_string_encode(message)
        out_topic = list_or_string_encode(topic)
        # N.B. list_or_string_encode always returns a list
        self.publish.send_multipart(out_topic + out_msg)




