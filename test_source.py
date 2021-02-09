
from time import sleep
from random import random
from cobblr import CobblrClient


def source_main():

    # create a new CobblrClient with the passed name
    # you can create multiple CobblrClients in a single python script
    source = CobblrClient("source")

    sleep(2)

    # You need to register your CobblrClient with the broker
    source.register()

    # wait while all the other apps are registering
    # I'll remove the need for this delay in future versions
    sleep(2)

    base_temp = 16.0

    shutdown = False

    while not shutdown:

        if random() > 0.85:
            base_temp += 1.0 + random()
        elif random() < 0.05:
            base_temp -= 1.0 + random()

        temperature = base_temp + 0.5 * random()

        # print(temperature)

        # I've put a long delay on the loop for debugging
        # Response times are in ms for the program
        sleep(0.5)

        # Get all connected clients and print
        # get_connected_clients() returns a list
        # clients = source.get_connected()
        # print("Connected clients:")
        # try:
        #    for client in clients[1:]:
        #        print(client)
        # except IndexError:
        #    print("Error: no connected clients")

        # Get all messages in the 'inbox' and print
        # get_messages returns a tab "\t" separated list
        # each message is in the form:
        # "sender_name", "message_string1", "message_string2", ... , "\t",
        messages = source.get_messages()
        # print(messages)

        # loop through messages
        i = 0
        while i < len(messages):
            if not messages == "no_msg":
                temp_message = []
                for word in messages:
                    if not word == "\t":
                        # add any actual message strings to the temporary message
                        # the first string will be the message sender
                        temp_message.append(word)
                    else:
                        # if you hit a \t = tab character, handle the message
                        print(temp_message)
                        # in this case, if we have data requested, we send back a random number
                        if temp_message[1] == "TEMP":
                            source.send_message(temp_message[0], ["TEMP", "%s" % temperature])
                        # clear the temp message after handling it
                        temp_message = []
                    i += 1


source_main()
