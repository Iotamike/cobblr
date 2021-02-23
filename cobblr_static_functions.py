"""
Static functions for cobblr library
"""

import zmq
import binascii
import os


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

