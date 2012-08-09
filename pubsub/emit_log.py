#!/usr/bin/env python

import demo.utils

class EmitLog(object):
    EXCHANGE_NAME = 'logs'

    def __init__(self):
        print "EmitLog: Constructing instance"
        self.initialized = False
        self.connection = None

    def __del__(self):
        print "EmitLog: Deleting instance"
        self.destroy()

    def initialize(self):
        print "EmitLog: initialize"

        self.initialized = True
        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='fanout')

    def destroy(self):
        print "EmitLog: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def emit(self, message):
        print "EmitLog: send"

        if not self.initialized:
            self.initialize()

        self.channel.basic_publish(exchange=self.EXCHANGE_NAME, routing_key='', body=message)
        
def main(args):
    emitLog = None
    try:
        message = ' '.join(args) or "info: Hello World!"
        emitLog = EmitLog()
        print "Emitting %r" % (message,)
        emitLog.emit(message)
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del emitLog

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
