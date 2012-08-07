#!/usr/bin/env python

import time

import demo.utils

class Worker(object):
    QUEUE_NAME = 'task_queue'

    def __init__(self):
        print "Worker: Constructing instance"
        self.connection = None

    def __del__(self):
        print "Worker: Deleting instance"
        self.destroy()

    def destroy(self):
        print "Worker: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def listen(self):
        print "Worker: listen"

        self.connection = demo.utils.get_connection()

        channel = self.connection.channel()
        channel.queue_declare(queue=self.QUEUE_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)
        consumer_tag = channel.basic_consume(self.received_message, queue='task_queue')
        print "Worker: consumer_tag %r" % (consumer_tag,)

        print "Worker: Waiting for messages. To exit press CTRL+C"

        try:
            channel.start_consuming()
        finally:
            self.destroy()

    def received_message(self, ch, method, properties, body):
        print "Worker: Received %r at %s" % (body, demo.utils.get_current_time_formatted())
        time.sleep(body.count('.'))
        print "Worker: Done at %s" % (demo.utils.get_current_time_formatted(),)
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main(args):
    worker = None
    try:
        worker = Worker()
        worker.listen()
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del worker

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
