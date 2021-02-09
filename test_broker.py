
from time import sleep
from cobblr import CobblrBroker


def broker_main():

    broker = CobblrBroker()

    shutdown = False

    sleep(2)

    # This is the main program loop which is up to the user to define
    # At the moment, get_connected_clients is the only user function
    while not shutdown:

        # I've put a long delay on the loop for debugging
        # Response times are in ms for the program
        sleep(4)

        # Get all connected clients and print
        # get_connected_clients() returns a list
        clients = broker.get_connected_clients()
        print("Connected clients:")
        try:
            for client in clients[1:]:
                print(client)
        except IndexError:
            print("Error: no connected clients")

        messages = broker.get_messages()

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
                        # clear the temp message after handling it
                        temp_message = []
                    i += 1


broker_main()




