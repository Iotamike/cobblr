import zmq

from src.cobblr import CobblrClient
from time import sleep
from datetime import datetime
from random import randint
import sys

from threading import Thread


def test_publisher(name=None, timer=None, pub=None):
    """
    start up a listener
    :param name:    name of the publisher, use this for name based addressing
                    - defaults to "test_publisher"
    :param timer:   time in seconds until closing the broker
                    - defaults to None = run forever
    :param pub:     list of tuples (topic, message)
                    - defaults to [("test_topic", <date & time string> : <Random string>)]
    """
    if name:
        cl = CobblrClient(name)
    else:
        cl = CobblrClient("test_subscriber")

    cl.start()

    sleep(1)

    cl.register()

    sleep(1)

    if timer:
        killer = Thread(target=kill_timer(cl, timer))
        killer.start()

    while True:
        sleep(2)
        if pub:
            if type(pub) == list:
                for p in pub:
                    if type(p) == tuple:
                        try:
                            cl.publish(p[0], [p[1]])
                        except IndexError as e:
                            print("whoops, not enough items in the tuple: %s" % e)
                        except zmq.ZMQError as e:
                            print("whoops, zmq had a boo-boo: %s" % e)
        else:
            try:
                curr_time = datetime.now()
                formatted_time = curr_time.strftime('%d/%m/%Y %H:%M:%S.%f')
                rand_str = ''
                for i in range(8):
                    rand_str += chr(randint(48, 122))
                pub_string = formatted_time + " - " + rand_str
                cl.publish("test_topic", [pub_string])
            except zmq.ZMQError as e:
                print("whoops, zmq had a boo-boo: %s" % e)


def kill_timer(client, secs):
    sleep(secs)
    client.end()
    sys.exit()


if __name__ == "__main__":
    test_publisher(name=None, timer=None, pub=None)



