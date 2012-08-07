#!/usr/bin/env python

import pika

import demo.utils

class Task(object):
    QUEUE_NAME = 'task_queue'
    ROUTING_KEY = 'task_queue'

    def __init__(self):
        print "Task: Constructing instance"
        self.initialized = False
        self.connection = None

    def __del__(self):
        print "Task: Deleting instance"
        self.destroy()

    def initialize(self):
        print "Task: initialize"

        self.initialized = True
        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.QUEUE_NAME, durable=True)

    def destroy(self):
        print "Task: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def send(self, message):
        print "Task: send"

        if not self.initialized:
            self.initialize()

        self.channel.basic_publish(exchange='',
                          routing_key=self.ROUTING_KEY,
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode = 2, # make message persistent
                          ))

        print "Task: Sent %r" % (message,)

def main(args):
    task = None
    try:
        message = args[0] if len(args) > 0 else "Hello World!"
        task = Task()
        print "Sending %r" % (message,)
        task.send(message)
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del task

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
