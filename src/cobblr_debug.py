"""
debug functions for cobblr

Currently just limited to printing
"""

import inspect
import os
from datetime import datetime

DEBUG = True

TIMEOUT = 10.0


def db_print(message):
    if DEBUG:
        curr_time = datetime.now()
        formatted_time = curr_time.strftime('%d/%m/%Y %H:%M:%S.%f')
        pid = os.getpid()
        caller = inspect.currentframe().f_back.f_code.co_name
        print("%s  pid: %s caller: %s   %s" % (formatted_time, pid, caller, message))

