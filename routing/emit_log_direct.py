#!/usr/bin/env python

import demo.utils

class EmitLogDirect(object):
    EXCHANGE_NAME = 'direct_logs'

    def __init__(self):
        print "EmitLogDirect: Constructing instance"
        self.initialized = False
        self.connection = None

    def __del__(self):
        print "EmitLogDirect: Deleting instance"
        self.destroy()

    def initialize(self):
        print "EmitLogDirect: initialize"

        self.initialized = True
        self.connection = demo.utils.get_connection()

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='direct')

    def destroy(self):
        print "EmitLogDirect: destroy"
        if (self.connection is not None) and (not self.connection.closed):
            self.connection.close()

    def emit(self, severity, message):
        print "EmitLogDirect: emit"

        if not self.initialized:
            self.initialize()

        self.channel.basic_publish(exchange=self.EXCHANGE_NAME, routing_key=severity, body=message)
        print "EmitLogDirect: Sent %r:%r" % (severity, message)

def main(args):
    emit_log_direct = None
    try:
        severity = args[0] if len(args) > 0 else 'info'
        message = ' '.join(args[1:]) or 'Hello World!'
        
        emit_log_direct = EmitLogDirect()
        print "Sending %r %r" % (severity, message)
        emit_log_direct.emit(severity, message)
        return 0
    except KeyboardInterrupt:
        print "Exiting"
        return 0
    except Exception, e:
        print "Exception"
        print str(e)
        return 1
    finally:
        del emit_log_direct

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv[1:]))
