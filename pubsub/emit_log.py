import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

declare_ok = channel.exchange_declare(exchange='logs',
                         type='fanout')
print " [*] declare_ok = %r" % (declare_ok,)

message = ' '.join(sys.argv[1:]) or "info: Hello World!"
channel.basic_publish(exchange='logs',
                      routing_key='',
                      body=message)
print " [x] Sent %r" % (message,)

connection.close()
