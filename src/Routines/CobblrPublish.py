from typing import Any, Coroutine

import zmq
from zmq.asyncio import Poller

from cobblr.cobblr_debug import db_print


class CobblrPublish:
    """
    Do not use or _super this class.
    It is left here as a template for what needs to go into a routine that can re-run itself.
    Inherited async functions aren't recognised as valid coroutines in:
            asyncio.create_task(coroutine)
            queue.put(coroutine)
    Hence _super'ing this class won't work to create a self re-running task as self.method won't be
    recognised as a valid coroutine.
    """

    def __init__(self, context, queue, port, pipe):

        self.context = context

        self.ongoing = True

        self.publish = None
        self.poller = None

        self.queue = queue
        self.port = port
        self.pipe = pipe
        self.method = self.first_run

    # starts the routine by adding the run_method to the queue
    async def start(self):
        await self.queue.put(self.run_method())

    # ensures the routine continues to run by re-adding itself to the queue
    async def run_method(self):
        await self.method()
        if self.ongoing:
            await self.queue.put(self.run_method)

    def end(self):

        if self.ongoing:
            self.ongoing = False
        else:
            db_print("task is already ending")

    async def first_run(self):
        # create 0MQ publish socket
        self.publish = self.context.socket(zmq.PUB)
        self.publish.bind("tcp://*:%s" % self.port)

        # create 0MQ poller
        self.poller = Poller()
        self.poller.register(self.pipe, zmq.POLLIN)

        self.method = self.run_loop()

    async def run_loop(self):
        poll_in = await self.poller.poll()
        event = dict(poll_in)

        if self.pipe in event:
            message = await self.pipe.recv_multipart()
            if message[0] == b"PUBLISH":
                self.publish.send_multipart([message[1:]])




