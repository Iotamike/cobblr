from src.cobblr import CobblrBroker
from time import sleep

from threading import Thread


def test_broker(timer=None):
    """
    start up a broker
    :param timer: time in seconds until closing the broker - defaults to None = run forever
    """
    br = CobblrBroker()

    br.start()

    if timer:
        sleep(10)
        br.end()


if __name__ == "__main__":
    test_broker(timer=None)

