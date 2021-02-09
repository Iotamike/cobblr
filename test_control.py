
from time import sleep
from cobblr import CobblrClient
import subprocess

def control_main():

    control = CobblrClient("control")

    # wait while all the other apps are registering
    # I'll remove the need for this delay in future versions
    sleep(2)

    control.register()

    # registered = False

    # system_started = False

    print("================\n"
          "Control Program:\n"
          "================\n"
          "\n"
          "Type SHUTDOWN to close the running cobblr system\n"
          "Type ADD to add a connection to output block")

    while True:
        command = input(">>>")
        if command == "SHUTDOWN":
            control.send_message("service", ["SHUTDOWN"])
            sleep(5)
            control.shutdown()
            break
        if command == "ADD":
            name1 = input("Type first block name")
            control.request_connection(name1)
            name2 = input ("Type second block name")
            control.send_message(name1, ["CONNECT", name2])
        else:
            continue


control_main()