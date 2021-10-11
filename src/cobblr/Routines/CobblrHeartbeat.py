import time
from json import dumps

import zmq

from ..cobblr_debug import db_print
from ..cobblr_static_functions import list_or_string_encode


class CobblrHeartbeat:
    def __init__(self, handler):
        self.handler = handler
        self.publisher = handler.publisher
        self.queue = handler.queue

        self.topic = b"HB"

        self.ongoing = True

        self.loop_time = time.time()
        self.hb_period = 5.000

    async def start(self):
        await self.queue.put(self.run_method(self.method))

    async def run_method(self, method):
        if not self.ongoing:
            return 0
        await method()
        if self.ongoing:
            await self.queue.put(self.run_method(method))

    async def method(self):
        if (time.time() - self.loop_time) > self.hb_period:
            msg_len = len(self.handler.message_queue)

            # temporarily got rid of most recent message as I need to decode it, handling for list or single
            # just wait until I've built a list_or_string_decode function. Not a priority

            # if msg_len > 0:
            #    most_recent_msg = self.handler.message_queue[msg_len-1]
            # else:
            #     most_recent_msg = "None"
            out_msg = {"num_dealers": len(self.handler.dealers),
                       "num_msgs": msg_len,
                       "name": self.handler.app_name,
                       "app_type": self.handler.app_type.name,
                       "app_state": self.handler.handler_state.name,
                       # "most_recent_msg": most_recent_msg
                       }

            hb_msg = list_or_string_encode(dumps(out_msg))

            try:
                await self.publisher.pub(self.topic, hb_msg)
            except zmq.ZMQError as e:
                print("error in publisher: %s" % e)

            self.loop_time = time.time()

    def end(self):
        self.ongoing = False
