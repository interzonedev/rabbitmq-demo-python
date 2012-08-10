#!/usr/bin/env python

import demo.utils

class EmitLogTopic(object):
    EXCHANGE_NAME = 'topic_logs'

    def __init__(self):
        print "EmitLogTopic: Constructing instance"
        self.initialized = False
        self.connection = None

    def __del__(self):
        print "EmitLogTopic: Deleting instance"
        self.destroy()

    def initialize(self):
        print "EmitLogTopic: initialize"

        self.initialized = True
        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='topic')

    def destroy(self):
        print "EmitLogTopic: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def send(self, routing_key, message):
        print "EmitLogTopic: send"

        if not self.initialized:
            self.initialize()

        self.channel.basic_publish(exchange=self.EXCHANGE_NAME, routing_key=routing_key, body=message)
        print "EmitLogTopic: Sent %r:%r" % (routing_key, message)

def main(args):
    emit_log_topic = None
    try:
        routing_key = args[0] if len(args) > 0 else 'anonymous.info'
        message = ' '.join(args[1:]) or 'Hello World!'

        emit_log_topic = EmitLogTopic()
        print "Sending %r %r" % (routing_key, message,)
        emit_log_topic.send(routing_key, message)
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del emit_log_topic

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
