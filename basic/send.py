#!/usr/bin/env python

import demo.utils

class Sender(object):
    QUEUE_NAME = 'hello'
    ROUTING_KEY = 'hello'

    def __init__(self):
        print "Sender: Constructing instance"
        self.initialized = False
        self.connection = None

    def __del__(self):
        print "Sender: Deleting instance"
        self.destroy()

    def initialize(self):
        print "Sender: initialize"

        self.initialized = True
        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.QUEUE_NAME)

    def destroy(self):
        print "Sender: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def send(self, message):
        print "Sender: send"

        if not self.initialized:
            self.initialize()

        self.channel.basic_publish(exchange="", routing_key=self.ROUTING_KEY, body=message)
        print "Sender: Sent %r" % (message,)

def main(args):
    sender = None
    try:
        message = args[0] if len(args) > 0 else "Hello World!"
        sender = Sender()
        print "Sending %r" % (message,)
        sender.send(message)
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del sender

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
