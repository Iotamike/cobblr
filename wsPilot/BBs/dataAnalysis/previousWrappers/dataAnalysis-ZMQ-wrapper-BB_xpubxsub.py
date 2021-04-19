import time
import binascii
import os
import threading
import zmw

def listener_thread(pipes):
    poller = zmq.Poller()

    for pipe in pipes:
        poller.register(pipe, zmq.POLLIN)

    while True:
        try:
            events = dict(poller.poll(1000))
            for pipe in pipes:
                if pipe in events:
                    try:
                        message = pipe.recv_multipart()
                        if message[1] == b"TERMINATE":
                            break
                        else:
                            print(message)
                    except zmq.ZMQError as e:
                        if e.errno == zmq.ETERM:
                            break
                else:
                    try:
                        if message[1] == b"TERMINATE":
                            break
                    except NameError:
                        continue
        except zmq.ZMQError:
            break

# relay helper function for proxy
# lifted from monitored_queue in pyzmq
# ins = input socket
# outs = output socket
# sides = side channel socket
# prefix = optional prefix for side channel
# swap_ids = boolean, only True for ROUTER -> ROUTER
def _relay(ins, outs, sides, prefix, swap_ids = False):
    msg = ins.recv_multipart()
    if swap_ids:
        msg[:2] = msg[:2][::-1]
    outs.send_multipart(msg)
    sides.send_multipart([prefix] + msg)

# xpub / xsub broker
# proxy for pub / sub
# self written as SteerableProxy from pyzmq is opaque
def pub_sub_broker(ctx, mon, ctl):

    # bind xsub socket
    # connect to this for internal BB pub/sub
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind("tcp://*:53045")

    # bind xpub socket
    # connect to this for internal BB pub/sub
    xpub = ctx.socket(zmq.XPUB)
    xpub.bind("tcp://*:53046")

    # start the thread running a proxy
    # which forwards from frontend to backend
    # send messages down mon and listen for commands on ctl

    poller = zmq.Poller()

    poller.register(xsub, zmq.POLLIN)
    poller.register(xpub, zmq.POLLIN)
    poller.register(ctl, zmq.POLLIN)

    while True:
        events = dict(poller.poll(1000))
        if xsub in events:
            _relay(xsub, xpub, mon, b"in")
        if xpub in events:
            _relay(xpub, xsub, mon, b"out")
        if ctl in events:
            message = ctl.recv_multipart()
            if message[1] == b"TERMINATE":
                break

    xsub.close()
    xpub.close()


def main():
    # create context, share this between threads
    context = zmq.Context()

    # bind pub socket for controlling app
    ctl_pub = context.socket(zmq.PUB)
    ctl_iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    ctl_pub.bind(ctl_iface)

    # bind sub socket for controlling pub-sub broker
    ps_ctl_sub = context.socket(zmq.SUB)
    ps_ctl_sub.connect(ctl_iface)
    ps_ctl_sub.subscribe(b"PS_CTL")

    # bind sub socket for controlling response-reply broker
    rr_ctl_sub = context.socket(zmq.SUB)
    rr_ctl_sub.connect(ctl_iface)
    rr_ctl_sub.subscribe(b"RR_CTL")

    # bind sub socket for listener
    ll_ctl_sub = context.socket(zmq.SUB)
    ll_ctl_sub.connect(ctl_iface)
    ll_ctl_sub.subscribe(b"LL_CTL")

    # bind pair of sockets for monitoring pub-sub proxy
    pipe_ps = zpipe(context)

    # bind pair of sockets for monitoring response/reply
    pipe_rr = zpipe(context)

    # open a listener thread - make sure to put all the pipe 'bind' ends in here
    # before starting the rest - unsure of safe ways to pass sockets to running threads
    #xpub
    # args=(expects a list) - hence the lots of [] to wrap the list of pipes
    l_thread = threading.Thread(target=listener_thread, args=([[pipe_ps[1], pipe_rr[1], ll_ctl_sub]]))
    l_thread.start()

    # open a thread for pub-sub
    ps_thread = threading.Thread(target=pub_sub_broker, args=([context, pipe_ps[0], ps_ctl_sub]))
    ps_thread.start()

    # open a thread for request-response
    rr_thread = threading.Thread(target=router_dealer, args=([context, pipe_rr[0], rr_ctl_sub]))
    rr_thread.start()


    input("Press enter to end ...")
    ctl_pub.send_multipart([b"PS_CTL", b"TERMINATE"])
    ctl_pub.send_multipart([b"RR_CTL", b"TERMINATE"])
    ctl_pub.send_multipart([b"LL_CTL", b"TERMINATE"])
    print("Interrupted")

    ps_thread.join()
    print("ps_thread ended")
    l_thread.join()
    print("ll_thread ended")
    rr_thread.join()
    print("rr_thread ended")

    # close up all the pipes
    for pair in [pipe_ps, pipe_rr]:
        for pipe in pair:
            pipe.close()
    print("pairs closed")

    ll_ctl_sub.close()
    rr_ctl_sub.close()
    ps_ctl_sub.close()
    print("thread subs closed")

    ctl_pub.close()
    print("control closed")

    context.term()
    print("context terminated")


if __name__ == "__main__":
    main()
