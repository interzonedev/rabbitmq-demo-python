#!/usr/bin/env python

import demo.utils

class ReceiveLogsDirect(object):
    EXCHANGE_NAME = 'direct_logs'

    def __init__(self, severities):
        print "ReceiveLogsDirect: Constructing instance"
        self.severities = severities
        self.connection = None

    def __del__(self):
        print "ReceiveLogsDirect: Deleting instance"
        self.destroy()

    def destroy(self):
        print "ReceiveLogsDirect: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def listen(self):
        print "ReceiveLogsDirect: listen"

        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='direct')

        result = self.channel.queue_declare(exclusive=True)
        print "ReceiveLogsDirect: result = %r" % (result,)
        queue_name = result.method.queue
        print "ReceiveLogsDirect: queue_name = %r" % (queue_name,)
        
        for severity in self.severities:
            bind_ok = self.channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name, routing_key=severity)
            print "ReceiveLogsDirect: bind_ok = %r" % (bind_ok,)

        self.channel.basic_consume(self.received_message, queue=queue_name, no_ack=True)

        print "ReceiveLogsDirect: Waiting for messages. To exit press CTRL+C"

        try:
            self.channel.start_consuming()
        finally:
            self.destroy()

    def received_message(self, ch, method, properties, body):
        print "ReceiveLogsDirect: Received %r" % (body,)

def main(args):
    receive_logs_direct = None
    try:
        severities = args[0:]
        if not severities:
            print "Usage: %s [info] [warning] [error]" % (args[0],)
            return 2

        receive_logs_direct = ReceiveLogsDirect(severities)
        receive_logs_direct.listen()
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del receive_logs_direct

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
