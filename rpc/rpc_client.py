#!/usr/bin/env python

import pika
import uuid

import demo.utils

class RpcClient(object):
    ROUTING_KEY = 'rpc_queue'

    def __init__(self):
        print "RpcClient: Constructing instance"
        self.initialized = False
        self.connection = None

    def __del__(self):
        print "RpcClient: Deleting instance"
        self.destroy()

    def initialize(self):
        print "RpcClient: initialize"

        self.initialized = True
        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        print "RpcClient: result = %r" % (result,)
        self.callback_queue = result.method.queue
        print "RpcClient: self.callback_queue = %r" % (self.callback_queue,)

        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

    def destroy(self):
        print "RpcClient: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def call(self, n):
        print "RpcClient: call"

        if not self.initialized:
            self.initialize()

        self.response = None

        self.corr_id = str(uuid.uuid4())
        print "RpcClient: corr_id = %r" % (self.corr_id,)

        self.channel.basic_publish(exchange='',
                                   routing_key=self.ROUTING_KEY,
                                   properties=pika.BasicProperties(
                                             reply_to = self.callback_queue,
                                             correlation_id = self.corr_id,
                                         ),
                                   body=str(n))

        while self.response is None:
            self.connection.process_data_events()

        return int(self.response)

    def on_response(self, ch, method, props, body):
        print "RpcClient: on_response"

        if self.corr_id == props.correlation_id:
            self.response = body

def main(args):
    rpc_client = None
    try:
        fib_start = args[0] if len(args) > 0 else 30
        print "Requesting fib(%r)" % (fib_start,)
        
        rpc_client = RpcClient()
        response = rpc_client.call(fib_start)
        print "Got %r" % (response,)

        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del rpc_client

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
