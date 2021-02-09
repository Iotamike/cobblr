
from time import sleep
from cobblr import CobblrClient


def output_main():

    # create a new CobblrClient with the passed name
    # you can create multiple CobblrClients in a single python script
    output = CobblrClient("output")

    sleep(2)

    # You need to register your CobblrClient with the broker
    output.register()

    # wait while all the other apps are registering
    # I'll remove the need for this delay in future versions
    sleep(2)

    # request a connection to the named CobblrClient
    # once ONE client has requested the connection -
    # BOTH clients can communicate with each other
    # by name addressing
    output.request_connection("analysis")

    connections = ["analysis"]

    shutdown = False

    # This is the main program loop which is up to the user to define
    while not shutdown:

        # I've put a long delay on the loop for debugging
        # Response times are in ms for the program
        sleep(4)

        # Get all connected clients and print
        # get_connected_clients() returns a list
        # clients = output.get_connected()
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
        messages = output.get_messages()
        # print(messages)

        for connection in connections:
            if not connection == "service":
                output.send_message(connection, ["RESULT"])

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
                        # in this case, we have received data, we assign it appropriately
                        if temp_message[1] == "RESULT":
                            print("Result = %s" % temp_message[2])
                        if temp_message[1] == "CONNECT":
                            output.request_connection(temp_message[2])
                            connections.append(temp_message[2])
                        # clear the temp message after handling it
                        temp_message = []
                    i += 1




output_main()
