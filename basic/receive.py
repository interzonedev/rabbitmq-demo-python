#!/usr/bin/env python

import demo.utils

class Receiver(object):
    QUEUE_NAME = 'hello'

    def __init__(self):
        print "Receiver: Constructing instance"
        self.connection = None

    def __del__(self):
        print "Receiver: Deleting instance"
        self.destroy()

    def destroy(self):
        print "Receiver: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def listen(self):
        print "Receiver: listen"

        self.connection = demo.utils.get_connection()

        channel = self.connection.channel()
        channel.queue_declare(queue=self.QUEUE_NAME)
        channel.basic_consume(self.received_message, queue=self.QUEUE_NAME, no_ack=True)

        print "Receiver: Waiting for messages. To exit press CTRL+C"

        try:
            channel.start_consuming()
        finally:
            self.destroy()

    def received_message(self, ch, method, properties, body):
        print "Receiver: Received %r" % (body,)

def main(args):
    receiver = None
    try:
        receiver = Receiver()
        receiver.listen()
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del receiver

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
