import zmq
from cobblr import zpipe, AppType, QueueHandler


def main():

    # create context
    context = zmq.Context()

    svc_pipe = zpipe(context)
    cli_pipe = zpipe(context)

    service_thread = QueueHandler(context, "service", AppType.SERVICE_APP, svc_pipe[0])
    service_thread.start()

    client_thread = QueueHandler(context, "test_app", AppType.CLIENT_APP, cli_pipe[0])
    client_thread.start()

    poller = zmq.Poller()
    poller.register(svc_pipe[1], zmq.POLLIN)
    poller.register(cli_pipe[1], zmq.POLLIN)

    msg_print_queue = []
    client_command = False
    service_command = False
    shutdown = False

    while True:

        if shutdown:
            break

        event = dict(poller.poll(100))
        if svc_pipe[1] in event:
            message = svc_pipe[1].recv_multipart()
            msg_print_queue.append("Service Message:\n %s \n" % message)
        if cli_pipe[1] in event:
            message = cli_pipe[1].recv_multipart()
            msg_print_queue.append("Client Message:\n %s \n" % message)

        for message in msg_print_queue:
            print(message)
        msg_print_queue = []

        command = input("service or client? type S or C\n")
        if command == "C":
            client_command = True
        elif command == "S":
            service_command = True
        else:
            print("-invalid entry-\n")

        if client_command or service_command:
            command = input("Type your command:\n"
                            "1 = Send message to address\n"
                            "2 = Get connected addresses\n"
                            "3 = Get Message (returns number of messages in queue followed by message)\n"
                            "4 = Register with service\n"
                            "5 = Shutdown (at the moment this will crash out!)\n"
                            "Hit Enter with no command to see message log\n")
            if command == "1":
                address = str.encode(input("Type address:\n"))
                message = str.encode(input("Type message:\n"))
                if client_command:
                    cli_pipe[1].send_multipart([b"SEND_TO", address, message])
                else:
                    svc_pipe[1].send_multipart([b"SEND_TO", address, message])
            if command == "2":
                if client_command:
                    cli_pipe[1].send_multipart([b"GET_CONNS"])
                else:
                    svc_pipe[1].send_multipart([b"GET_CONNS"])
            if command == "3":
                if client_command:
                    cli_pipe[1].send_multipart([b"GET_MSG"])
                else:
                    svc_pipe[1].send_multipart([b"GET_MSG"])
            if command == "4":
                if client_command:
                    cli_pipe[1].send_multipart([b"REGISTER"])
                else:
                    print("SERVICE_APP does not register!")
            if command == "5":
                cli_pipe[1].send(b"SHUTDOWN")
                svc_pipe[1].send(b"SHUTDOWN")
                shutdown = True
            client_command = False
            service_command = False

    svc_pipe[0].close()
    svc_pipe[1].close()
    cli_pipe[0].close()
    cli_pipe[1].close()

    context.term()


if __name__ == "__main__":
    main()