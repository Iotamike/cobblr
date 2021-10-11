
import os

import asyncio

import zmq
from zmq.asyncio import Context, Poller

from .config import AppType
from .cobblr_debug import db_print
from .cobblr_static_functions import pipe_end, zpipe

from .Routines.CobblrHandler import CobblrHandler

import threading

# needed for using a custom event_loop in windows
if os.name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# Most of the loop function happens in CobblrBroker and CobblrClient below
# The main principle of operations of the code:
# An event_loop is created
#   An async queue is created, attached to this event_loop
#   The 'startup' async operations on each routine (e.g. router, dealer, etc) are performed
#   These routines are attached to the event_loop, and functions are appended to the queue
#       The event_loop_manager schedules the top queue item to be run
#       then schedules itself to be run
#       After each run, a function will add itself back onto the queue
class CobblrBroker:
    def __init__(self):

        # initialise class  for main thread
        self.local_context = zmq.Context()
        # create a pipe endpoint which is bound to context in thread1 (i.e. main thread)
        # and grab it's address
        # have to use tcp pipes as inproc aren't supported between contexts
        self.local_pipe, self.local_address = pipe_end(context=self.local_context)
        self.poller = Poller()
        self.poller.register(self.local_pipe, zmq.POLLIN)

        # initialise empty class objects for event_loop thread
        self.thread_context = None
        self.queue = None
        self.handler = None
        self.loop = None
        self.loop_counter = 1

        self.handler_thread = None

    def start(self):

        # thread off the async operations into a separate thread
        self.handler_thread = threading.Thread(target=self.event_loop)
        self.handler_thread.start()

    def event_loop(self):

        # as we are threading this off, we need to create a new event loop here
        self.loop = asyncio.new_event_loop()
        # the default event loop will always be thread1 - if you don't explicitly set it here
        # any event_loop based operations will try and attach to, or create an event_loop in thread1
        asyncio.set_event_loop(self.loop)
        db_print(self.loop)
        # Now create the queue when you can be sure it will attach to this threads event_loop
        self.queue = asyncio.Queue()
        # Now create a new async zmq context for this event loop as well
        self.thread_context = zmq.asyncio.Context()
        # create a pipe endpoint which is anchored in this thread
        # and use the thread1 address to connect it
        thread_pipe = pipe_end(context=self.thread_context, address=self.local_address)
        db_print("event_loop created")
        self.handler = CobblrHandler(self.queue, self.thread_context, "service", AppType.SERVICE_APP, thread_pipe)
        self.loop.create_task(self.handler.start())
        db_print("new handler")
        self.loop.create_task(self.event_loop_manager())
        self.loop.run_forever()
        db_print("event_loop running")

    async def event_loop_manager(self):
        next_cobblr_routine = await self.queue.get()
        # db_print("loop number %s" % self.loop_counter) # seriously keep this commented out
        self.loop_counter += 1
        # db_print(next_cobblr_routine) # ^^ ditto ^^
        self.loop.create_task(next_cobblr_routine)
        self.loop.create_task(self.event_loop_manager())

    def get_connected_clients(self):
        connections = []
        self.local_pipe.send(b"GET_CONNS")
        message = self.local_pipe.recv_multipart()
        for word in message:
            connections.append(word.decode())
        return connections

    def get_messages(self):
        messages = []

        while True:

            self.local_pipe.send(b"GET_MSG")

            message = self.local_pipe.recv_multipart()
            # stop running if there are no handled messages
            if message[0] == b"NO_MSG":
                break
            else:
                temp_msg = []
                for word in message[1:]:
                    temp_msg.append(word)

                messages.append(temp_msg)

        if len(messages) > 0:
            return messages
        else:
            return ["no_msg"]

    def get_subs(self):
        sub_messages = []

        while True:

            self.local_pipe.send(b"GET_SUB")

            message = self.local_pipe.recv_multipart()
            # stop running if there are no handled messages
            if message[0] == b"NO_SUB":
                break
            else:
                temp_msg = []
                for word in message[1:]:
                    temp_msg.append(word)

                sub_messages.append(temp_msg)

        if len(sub_messages) > 0:
            return sub_messages
        else:
            return ["no_sub"]

    def send_message(self, to, message):
        send_message = [b"SEND_TO", str.encode("%s" % to)]
        for word in message:
            send_message.append(str.encode("%s" % word))
        self.local_pipe.send_multipart(send_message)

    def end(self):
        try:
            self.loop.stop()
            self.local_pipe.close(linger=1)
            del self.poller, self.local_pipe
            self.local_context.destroy(linger=1)
            self.thread_context.destroy(linger=1)
            del self.local_context, self.thread_context
        except AttributeError as e:
            print("Closing down fuss: %s" % e)
        except zmq.ZMQError as e:
            print("Closing down fuss: %s" % e)

    def __del__(self):
        try:
            self.loop.stop()
            self.local_pipe.close(linger=1)
            del self.poller, self.local_pipe, self.loop
            self.local_context.destroy(linger=1)
            self.thread_context.destroy(linger=1)
            del self.local_context, self.thread_context
        except AttributeError as e:
            print("Closing down fuss: %s" % e)
        except zmq.ZMQError as e:
            print("Closing down fuss: %s" % e)

class CobblrClient:
    def __init__(self, name):
        self.name = name
        # initialise class  for main thread
        self.local_context = zmq.Context()
        # create a pipe endpoint which is bound to context in thread1 (i.e. main thread)
        # and grab it's address
        self.local_pipe, self.local_address = pipe_end(context=self.local_context)
        self.poller = Poller()
        self.poller.register(self.local_pipe, zmq.POLLIN)

        # initialise empty class objects for event_loop thread
        self.thread_context = None
        self.queue = None
        self.handler = None
        self.loop = None
        self.loop_counter = 0

        self.handler_thread = None

    def start(self):
        # thread off the async operations into a separate thread
        self.handler_thread = threading.Thread(target=self.event_loop)
        self.handler_thread.start()

    def event_loop(self):

        # as we are threading this off, we need to create a new event loop here
        self.loop = asyncio.new_event_loop()
        # the default event loop will always be thread1 - if you don't explicitly set it here
        # any event_loop based operations will try and attach to, or create an event_loop in thread1
        asyncio.set_event_loop(self.loop)
        db_print(self.loop)
        # Now create the queue when you can be sure it will attach to this threads event_loop
        self.queue = asyncio.Queue()
        # ditto the zmq async context
        self.thread_context = zmq.asyncio.Context()
        # create a pipe endpoint which is anchored in this thread
        # and use the thread1 address to connect it
        thread_pipe = pipe_end(context=self.thread_context, address=self.local_address)
        db_print("event_loop created")
        self.handler = CobblrHandler(self.queue, self.thread_context, self.name, AppType.CLIENT_APP, thread_pipe)
        self.loop.create_task(self.handler.start())
        db_print("new handler")
        self.loop.create_task(self.event_loop_manager())
        self.loop.run_forever()
        db_print("event_loop running")

    async def event_loop_manager(self):
        next_cobblr_routine = await self.queue.get()
        # db_print("loop number %s" % self.loop_counter) # seriously keep this commented out
        self.loop_counter += 1
        # db_print(next_cobblr_routine) # ^^ ditto ^^
        self.loop.create_task(next_cobblr_routine)
        self.loop.create_task(self.event_loop_manager())

    def get_connected_clients(self):
        connections = []
        self.local_pipe.send(b"GET_CONNS")
        message = self.local_pipe.recv_multipart()
        for word in message:
            connections.append(word.decode())
        return connections

    def get_messages(self):
        messages = []

        while True:

            self.local_pipe.send(b"GET_MSG")

            message = self.local_pipe.recv_multipart()
            # stop running if there are no handled messages
            if message[0] == b"NO_MSG":
                break
            else:
                temp_msg = []
                for word in message[1:]:
                    temp_msg.append(word)

                messages.append(temp_msg)

        if len(messages) > 0:
            return messages
        else:
            return ["no_msg"]

    def get_subs(self):
        sub_messages = []

        while True:

            self.local_pipe.send(b"GET_SUB")

            message = self.local_pipe.recv_multipart()
            # stop running if there are no handled messages
            if message[0] == b"NO_SUB":
                break
            else:
                temp_msg = []
                for word in message[1:]:
                    temp_msg.append(word)

                sub_messages.append(temp_msg)

        if len(sub_messages) > 0:
            return sub_messages
        else:
            return ["no_sub"]

    def subscribe(self, sub):
        send_message = [b"SUBSCRIBE", str.encode("%s" % sub)]
        self.local_pipe.send_multipart(send_message)

    def publish(self, pub, message):
        send_message = [b"PUB_MSG", str.encode("%s" % pub)]
        for word in message:
            send_message.append(str.encode("%s" % word))
        self.local_pipe.send_multipart(send_message)

    def send_message(self, to, message):
        send_message = [b"SEND_TO", str.encode("%s" % to)]
        for word in message:
            send_message.append(str.encode("%s" % word))
        self.local_pipe.send_multipart(send_message)

    def request_connection(self, to):
        send_message = [b"RQ_ROUTE", str.encode("%s" % to)]
        self.local_pipe.send_multipart(send_message)

    def register(self):
        db_print("registering")
        self.local_pipe.send(b"REGISTER")
        db_print("sent")

    def get_connected(self):
        connections = []
        self.local_pipe.send(b"GET_CONNS")
        message = self.local_pipe.recv_multipart()
        for word in message:
            connections.append(word.decode())
        return connections

    def end(self):
        try:
            self.loop.stop()
            self.local_pipe.close(linger=1)
            del self.poller, self.local_pipe
            self.local_context.destroy(linger=1)
            self.thread_context.destroy(linger=1)
            del self.local_context, self.thread_context
        except AttributeError as e:
            print("Closing down fuss: %s" % e)
        except zmq.ZMQError as e:
            print("Closing down fuss: %s" % e)

    def __del__(self):
        try:
            self.loop.stop()
            self.local_pipe.close(linger=1)
            del self.poller, self.local_pipe, self.loop
            self.local_context.destroy(linger=1)
            self.thread_context.destroy(linger=1)
            del self.local_context, self.thread_context
        except AttributeError as e:
            print("Closing down fuss: %s" % e)
        except zmq.ZMQError as e:
            print("Closing down fuss: %s" % e)