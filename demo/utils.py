import pika

def get_connection(host='localhost', port=pika.spec.PORT):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host, port=port))
    return connection
