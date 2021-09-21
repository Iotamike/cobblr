"""
Static functions for cobblr library
"""

import zmq
import binascii
import os
from random import randint
from .cobblr_debug import db_print


def zpipe(ctx):
    """
    builds inproc pipe for talking to threads - lifted from zhelpers.py

    :param ctx: context
    :return: two 'PAIR' sockets using OS sockets
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b


def pipe_end(context, address=None):
    """
        starts or finishes an inproc pipe

        :param context: zmq.Context() or zmq.asyncio.Context()
        :param address: port string, leave blank to generate and return an address
        :return: zmq.PAIR socket, or [zmq.PAIR socket, bound address string]
    """
    if address:
        db_print("close pipe")
        sock = context.socket(zmq.PAIR)
        sock.linger = 0
        sock.hwm = 1
        sock.connect(address)
        return sock
    else:
        while True:
            db_print("open pipe")
            iface = "tcp://127.0.0.101:%s" % (53173 + randint(0, 16384))
            db_print(iface)
            sock = context.socket(zmq.PAIR)
            sock.linger = 0
            sock.hwm = 1
            try:
                sock.bind(iface)
            except zmq.ZMQError as e:
                db_print("Address in use: %s" % e)
                continue
            break
        return sock, iface


# relay helper function for proxy
# lifted from monitored_queue in pyzmq
# ins = input socket
# outs = output socket
# sides = side channel socket
# prefix = optional prefix for side channel
# swap_ids = boolean, only True for ROUTER -> ROUTER
def _relay(ins, outs, sides, prefix, swap_ids=False):
    msg = ins.recv_multipart()
    if swap_ids:
        msg[:2] = msg[:2][::-1]
    outs.send_multipart(msg)
    sides.send_multipart([prefix] + msg)

