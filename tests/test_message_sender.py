from src.cobblr.cobblr import CobblrClient
from time import sleep
import sys

from threading import Thread


def test_message_sender(name=None, timer=None, target=None, message=None):
    """
    start up a sender
    :param name:    name of the sender, use this for name based addressing
                    - defaults to "test_sender"
    :param timer:   time in seconds until closing the broker
                    - defaults to None = run until all messages sent to all listeners
    :param target:  list of names of the target application to send the message(s) to
                    - defaults to ["test_listener"]
    :param message: list of strings to send as messages
                    - defaults to ["testing", "1", "2", "3", "end"]
    """
    if name:
        cl = CobblrClient(name)
    else:
        cl = CobblrClient("test_sender")

    if target:
        if type(target) == list:
            send_target = target
        else:
            send_target = ["test_listener"]
    else:
        send_target = ["test_listener"]

    if message:
        if type(message) == list:
            send_message = message
        else:
            send_message = ["testing", "1", "2", "3", "end"]
    else:
        send_message = ["testing", "1", "2", "3", "end"]

    cl.start()

    sleep(1)

    cl.register()

    sleep(1)

    for t in send_target:
        cl.request_connection(t)
        sleep(1)

    if timer:
        killer = Thread(target=kill_timer(cl, timer))
        killer.start()

    for m in send_message:
        for t in send_target:
            sleep(0.1)
            cl.send_message(t, [m])

    sleep(1)

    cl.end()


def kill_timer(client, secs):
    sleep(secs)
    client.end()
    sys.exit()


if __name__ == "__main__":
    test_message_sender(name=None, timer=None, target=None, message=None)