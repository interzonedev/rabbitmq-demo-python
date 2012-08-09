#!/usr/bin/env python

import demo.utils

class ReceiveLog(object):
    EXCHANGE_NAME = 'logs'

    def __init__(self):
        print "ReceiveLog: Constructing instance"
        self.connection = None

    def __del__(self):
        print "ReceiveLog: Deleting instance"
        self.destroy()

    def destroy(self):
        print "ReceiveLog: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def listen(self):
        print "ReceiveLog: listen"

        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()
        declare_ok = self.channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='fanout')
        print "ReceiveLog: declare_ok = %r" % (declare_ok,)

        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        print "ReceiveLog: queue_name = %r" % (queue_name,)

        bind_ok = self.channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name)
        print "ReceiveLog: bind_ok = %r" % (bind_ok,)

        consumer_tag = self.channel.basic_consume(self.received_message, queue=queue_name, no_ack=True)
        print "ReceiveLog: consumer_tag = %r" % (consumer_tag,)

        print "ReceiveLog: Waiting for messages. To exit press CTRL+C"

        try:
            self.channel.start_consuming()
        finally:
            self.destroy()

    def received_message(self, ch, method, properties, body):
        print "ReceiveLog: Received %r" % (body,)

def main(args):
    receiveLog = None
    try:
        receiveLog = ReceiveLog()
        receiveLog.listen()
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del receiveLog

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
