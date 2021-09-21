"""
debug functions for cobblr

Currently just limited to printing
"""


DEBUG = False

TIMEOUT = 10.0

def db_print(message):
    if DEBUG:
        print(message)