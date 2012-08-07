import pika
import time

def get_connection(host='localhost', port=pika.spec.PORT):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host, port=port))
    return connection

def get_current_time_formatted(dateformat='%Y-%m-%d %H:%M:%S'):
    return time.strftime(dateformat, time.gmtime())
