#!/usr/bin/env python

import demo.utils

class ReceiveLogsTopic(object):
    EXCHANGE_NAME = 'topic_logs'

    def __init__(self, binding_keys):
        print "ReceiveLogsTopic: Constructing instance"
        self.connection = None
        self.binding_keys = binding_keys

    def __del__(self):
        print "ReceiveLogsTopic: Deleting instance"
        self.destroy()

    def destroy(self):
        print "ReceiveLogsTopic: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def listen(self):
        print "ReceiveLogsTopic: listen"

        self.connection = demo.utils.get_connection()
        channel = self.connection.channel()

        channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='topic')

        result = channel.queue_declare(exclusive=True)
        print "ReceiveLogsTopic: result = %r" % (result,)
        queue_name = result.method.queue
        print "ReceiveLogsTopic: queue_name = %r" % (queue_name,)

        for binding_key in self.binding_keys:
            bind_ok = channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=queue_name, routing_key=binding_key)
            print "ReceiveLogsTopic: bind_ok = %r" % (bind_ok,)

        channel.basic_consume(self.received_message, queue=queue_name, no_ack=True)

        print "ReceiveLogsTopic: Waiting for messages. To exit press CTRL+C"

        try:
            channel.start_consuming()
        finally:
            self.destroy()

    def received_message(self, ch, method, properties, body):
        print "ReceiveLogsTopic: Received %r" % (body,)

def main(args):
    receive_logs_topic = None
    try:
        binding_keys = args[0:]
        if not binding_keys:
            print "Usage: %s [binding_key]..." % (args[0],)
            return 2

        receive_logs_topic = ReceiveLogsTopic(binding_keys)
        receive_logs_topic.listen()
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del receive_logs_topic

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
