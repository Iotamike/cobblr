from src.cobblr.cobblr import CobblrClient
from time import sleep
import sys

from threading import Thread


def test_listener(name=None, timer=None):
    """
    start up a listener
    :param name: name of the listener, use this for name based addressing - defaults to "test_listener"
    :param timer: time in seconds until closing the broker - defaults to None = run forever
    """
    if name:
        cl = CobblrClient(name)
    else:
        cl = CobblrClient("test_listener")

    if timer:
        killer = Thread(target=kill_timer(cl, timer))
        killer.start()

    cl.start()

    sleep(1)

    cl.register()

    sleep(1)

    while True:
        msg = ["no_msg"]
        while msg[0] == "no_msg":
            msg = cl.get_messages()
            sleep(0.1)
        print(msg)


def kill_timer(client, secs):
    sleep(secs)
    client.end()
    sys.exit()


if __name__ == "__main__":
    test_listener(name=None, timer=None)

