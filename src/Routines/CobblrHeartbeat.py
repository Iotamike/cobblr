import zmq
import time
from json import dumps

from ..cobblr_static_functions import list_or_string_encode


class CobblrHeartbeat:
    def __init__(self, handler):
        self.handler = handler
        self.publisher = handler.publisher

        self.topic = b"HB"

        self.ongoing = True

    def start(self):
        while self.ongoing:
            time.sleep(5)
            msg_len = len(self.handler.message_queue)
            if msg_len > 0:
                most_recent_msg = self.handler.message_queue[msg_len-1]
            else:
                most_recent_msg = "None"
            out_msg = {"num_dealers": len(self.handler.dealers),
                       "num_msgs": msg_len,
                       "name": self.handler.app_name,
                       "app_type": self.handler.app_type.name,
                       "app_state": self.handler.handler_state.name,
                       "most_recent_msg": most_recent_msg
                       }

            hb_msg = list_or_string_encode(dumps(out_msg))

            self.publisher.pub(self.topic, hb_msg)

    def end(self):
        self.ongoing = False
