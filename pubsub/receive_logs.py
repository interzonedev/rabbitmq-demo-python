import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

declare_ok = channel.exchange_declare(exchange='logs',
                         type='fanout')
print " [*] declare_ok = %r" % (declare_ok,)

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
print " [*] queue_name = %r" % (queue_name,)

bind_ok = channel.queue_bind(exchange='logs',
                   queue=queue_name)
print " [*] bind_ok = %r" % (bind_ok,)

def callback(ch, method, properties, body):
    print " [x] %r" % (body,)

consumer_tag = channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)
print " [*] consumer_tag = %r" % (consumer_tag,)

print ' [*] Waiting for logs. To exit press CTRL+C'

channel.start_consuming()
