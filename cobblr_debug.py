"""
debug functions for cobblr

Currently just limited to printing
"""


DEBUG = False


def db_print(message):
    if DEBUG:
        print(message)