#!/usr/bin/env python

import pika

import demo.utils

class RpcServer(object):
    QUEUE_NAME = 'rpc_queue'

    def __init__(self):
        print "RpcServer: Constructing instance"
        self.connection = None

    def __del__(self):
        print "RpcServer: Deleting instance"
        self.destroy()

    def destroy(self):
        print "RpcServer: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def listen(self):
        print "RpcServer: listen"

        self.connection = demo.utils.get_connection()

        channel = self.connection.channel()

        result = channel.queue_declare(queue=self.QUEUE_NAME)
        print "RpcServer: result = %r" % (result,)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.on_request, queue=self.QUEUE_NAME)

        print "RpcServer: Waiting for messages. To exit press CTRL+C"

        try:
            channel.start_consuming()
        finally:
            self.destroy()

    def fib(self, n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return self.fib(n-1) + self.fib(n-2)

    def on_request(self, ch, method, props, body):
        n = int(body)

        print "RpcServer: fib(%s)"  % (n,)
        response = self.fib(n)
        print "RpcServer: response = %r" % (response,)

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id = props.correlation_id),
                         body=str(response))
        ch.basic_ack(delivery_tag = method.delivery_tag)

def main(args):
    rpc_server = None
    try:
        rpc_server = RpcServer()
        rpc_server.listen()
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del rpc_server

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
