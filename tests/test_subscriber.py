from src.cobblr.cobblr import CobblrClient
from time import sleep
import sys

from threading import Thread


def test_subscriber(name=None, timer=None, sub=None):
    """
    start up a listener
    :param name: name of the subscriber, use this for name based addressing - defaults to "test_subscriber"
    :param timer: time in seconds until closing the broker - defaults to None = run forever
    :param sub: list of topics to subscribe. Defaults to ["test_topic"]
    """
    if name:
        cl = CobblrClient(name)
    else:
        cl = CobblrClient("test_subscriber")

    if sub:
        if type(sub) == list:
            sub_topics = sub
        else:
            sub_topics = ["test_topic"]
    else:
        sub_topics = ["test_topic"]

    cl.start()

    sleep(1)

    cl.register()

    sleep(1)

    for t in sub_topics:
        cl.subscribe(t)
        sleep(0.1)

    if timer:
        killer = Thread(target=kill_timer(cl, timer))
        killer.start()

    while True:
        msg = ["no_sub"]
        while msg[0] == "no_sub":
            msg = cl.get_subs()
            sleep(0.1)
        print(msg)


def kill_timer(client, secs):
    sleep(secs)
    client.end()
    sys.exit()


if __name__ == "__main__":
    test_subscriber(name=None, timer=None, sub=None)



