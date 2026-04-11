import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError, MessageMiddlewareDeleteError

PREFETCH_COUNT = 3

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = None
        self.channel = None
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def _is_disconnected(self):
        return (self.connection is None
                or self.connection.is_closed
                or self.channel is None
                or self.channel.is_closed)

    def send(self, message):
        if self._is_disconnected():
            raise MessageMiddlewareDisconnectedError()

        try:
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            if self._is_disconnected():
                raise MessageMiddlewareDisconnectedError()
            raise MessageMiddlewareMessageError()

    def start_consuming(self, on_message_callback):
        if self._is_disconnected():
            raise MessageMiddlewareDisconnectedError()

        try:
            def on_message(ch, method, properties, body):
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

                on_message_callback(body, ack, nack)

            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=on_message, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            if self._is_disconnected():
                raise MessageMiddlewareDisconnectedError()
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        if self._is_disconnected():
            raise MessageMiddlewareDisconnectedError()

        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            if self._is_disconnected():
                raise MessageMiddlewareDisconnectedError()
            pass

    def close(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = None
        self.channel = None
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        try:        
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()
    
    def _is_disconnected(self):
        return (self.connection is None
                or self.connection.is_closed
                or self.channel is None
                or self.channel.is_closed)

    def send(self, message):
        if self._is_disconnected():
            raise MessageMiddlewareDisconnectedError()
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            if self._is_disconnected():
                raise MessageMiddlewareDisconnectedError()
            raise MessageMiddlewareMessageError()

    def start_consuming(self, on_message_callback):
        if self._is_disconnected():
            raise MessageMiddlewareDisconnectedError()
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)

            def on_message(ch, method, properties, body):
                ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
                nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

                on_message_callback(body, ack, nack)

            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self.channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            if self._is_disconnected():
                raise MessageMiddlewareDisconnectedError()
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        if self._is_disconnected():
            raise MessageMiddlewareDisconnectedError()

        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            if self._is_disconnected():
                raise MessageMiddlewareDisconnectedError()
            pass

    def close(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError()