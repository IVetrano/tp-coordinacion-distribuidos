import os
import logging
import threading
import signal
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# Maxima cantidad de intentos para conseguir que coincidan los amount recibidos con el total
MAX_ATTEMPTS = 5

class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.amount_by_fruit = {}
        self.messages_received = 0

        self.total_messages = 0
        self.total_messages_received_by_sums = 0
        self.responses_received = 0
        self.attempts = 0

    def add_fruit(self, fruit, amount):
        if fruit not in self.amount_by_fruit:
            self.amount_by_fruit[fruit] = fruit_item.FruitItem(fruit, 0)
        self.amount_by_fruit[fruit] += fruit_item.FruitItem(fruit, amount)

        self.messages_received += 1
    
    def increase_messages_received_by_sums(self, amount):
        self.responses_received += 1
        self.total_messages_received_by_sums += amount

    def get_final_fruit_items(self):
        return self.amount_by_fruit.values()
    
    def take_leadership(self, total_messages):
        self.total_messages = total_messages
    
    def amount_received_equals_total(self):
        return self.total_messages_received_by_sums == self.total_messages

    def count_failed_attempt(self):
        self.responses_received = 0
        self.total_messages_received_by_sums = 0
        self.attempts += 1

class SumFilter:
    def __init__(self):
        self.lock = threading.Lock()

        # Cola de entrada de datos
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # Entrada de mensajes de control (EOF)
        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, 
            SUM_CONTROL_EXCHANGE, 
            [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )

        # Broadcast de mensajes de control (EOF) a otros Sums desde el hilo de control
        self.control_broadcast_exchange_control = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT)]
        )

        # Broadcast de mensajes de control (EOF) a otros Sums desde el hilo de datos
        self.control_broadcast_exchange_data = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT)]
        )

        # Salida de mensajes de control a otros Sums (para devolver la cantidad de mensajes recibidos al líder)
        self.control_output_exchanges = {}
        for i in range(SUM_AMOUNT):
            self.control_output_exchanges[i] = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [f"{SUM_CONTROL_EXCHANGE}_{i}"]
            )

        # Salida de datos a los Aggregators
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.client_by_id = {}

    def _send_to_aggregator(self, client_id, fruit, amount):
        fruit_hash = hashlib.md5(fruit.encode('utf-8')).hexdigest()
        aggregator_index = int(fruit_hash, 16) % AGGREGATION_AMOUNT
        target_exchange = self.data_output_exchanges[aggregator_index]

        target_exchange.send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.DATA_MESSAGE_TYPE,
                    client_id,
                    fruit,
                    amount
                ]
            )
        )

    def _broadcast_eof_to_aggregators(self, client_id):
        logging.info(f"Broadcasting EOF message to aggregators")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [
                        message_protocol.internal.EOF_MESSAGE_TYPE,
                        client_id
                    ]
                )
            )
    
    def _send_data_to_aggregators(self, client):
        logging.info(f"Broadcasting data messages to aggregators")
        for fruit_item in client.get_final_fruit_items():
            self._send_to_aggregator(
                client.client_id,
                fruit_item.fruit,
                fruit_item.amount
            )

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            if client_id not in self.client_by_id:
                self.client_by_id[client_id] = Client(client_id)
            client = self.client_by_id[client_id]
            client.add_fruit(fruit, int(amount))

    def _flush_client(self, client_id):
        logging.info(f"Flushing client {client_id}")
        with self.lock:
            client = self.client_by_id.pop(client_id, None)

        if client is not None:
            self._send_data_to_aggregators(client)

        self._broadcast_eof_to_aggregators(client_id)

    def _broadcast_max_amount_reached_to_sums(self, client_id):
        logging.info(f"Sending max amount reached message for client {client_id}")
        self.control_broadcast_exchange_control.send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.MAX_AMOUNT_REACHED_MESSAGE_TYPE,
                    client_id
                ]
            )
        )

    def _request_sums_messages_amount_with_exchange(self, client, broadcast_exchange):
        logging.info(f"Requesting amount of messages received from other Sums for client {client.client_id}")
        broadcast_exchange.send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.AMOUNT_REQUEST_MESSAGE_TYPE,
                    client.client_id,
                    ID
                ]
            )
        )

    def _notify_flush_to_sums(self, client_id):
        logging.info(f"Notifying flush to other Sums for client {client_id}")
        self.control_broadcast_exchange_control.send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.FLUSH_MESSAGE_TYPE,
                    client_id
                ]
            )
        )

    def _process_message_amount_response(self, client_id, amount):
        client = None
        with self.lock:
            client = self.client_by_id.get(client_id)

        # Aca ya no hace falta lock porque si soy el lider de este cliente
        # Significa que ya recibi EOF en el hilo de datos, y, por el orden de la queue,
        # Es imposible que reciba otro mensaje de datos para este cliente, 
        # Por lo que el hilo de datos no va a modificar el cliente

        if client is None:
            logging.error(f"Received amount response for unknown client {client_id}")
            return
        client.increase_messages_received_by_sums(int(amount))

        if client.responses_received == SUM_AMOUNT:
            if client.amount_received_equals_total():
                logging.info(f"Received all amount responses for client {client_id} and they match the total. Proceeding to flush.")
                self._notify_flush_to_sums(client_id)
            else:
                client.count_failed_attempt()

                if client.attempts > MAX_ATTEMPTS:
                    logging.error(f"Max attempts reached for client {client_id}.")
                    self._broadcast_max_amount_reached_to_sums(client_id)
                else:
                    logging.warning(f"Received all amount responses for client {client_id} but they do not match the total. Retrying... Attempt {client.attempts}")
                    self._request_sums_messages_amount_with_exchange(client, self.control_broadcast_exchange_control)

    def _process_messages_amount_request(self, client_id, requester_id):
        with self.lock:
            client = self.client_by_id.get(client_id)
            amount = client.messages_received if client is not None else 0

        logging.info(f"Received amount request from Sum {requester_id} for client {client_id}. Responding with amount {amount}")
        self.control_output_exchanges[int(requester_id)].send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.AMOUNT_RESPONSE_MESSAGE_TYPE,
                    client_id,
                    amount
                ]
            )
        )
    
    def _process_max_amount_reached(self, client_id):
        logging.error(f"Received max amount reached message for client {client_id}. Flushing client.")
        with self.lock:
            self.client_by_id.pop(client_id, None)
        self._broadcast_eof_to_aggregators(client_id)
    
    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == message_protocol.internal.FLUSH_MESSAGE_TYPE:
            _, client_id = fields
            self._flush_client(client_id)
        elif msg_type == message_protocol.internal.AMOUNT_REQUEST_MESSAGE_TYPE:
            _, client_id, requester_id = fields
            self._process_messages_amount_request(client_id, requester_id)
        elif msg_type == message_protocol.internal.AMOUNT_RESPONSE_MESSAGE_TYPE:
            _, client_id, amount = fields
            self._process_message_amount_response(client_id, amount)
        elif msg_type == message_protocol.internal.MAX_AMOUNT_REACHED_MESSAGE_TYPE:
            _, client_id = fields
            self._process_max_amount_reached(client_id)
        else:
            logging.error(f"Unknown control message type: {msg_type}")
            nack()
            return

        ack()

    def _process_eof(self, client_id, total_messages):
        logging.info(f"Received EOF message for client {client_id}")
        with self.lock:
            client = self.client_by_id.get(client_id)
            
            if client is None:
                client = Client(client_id)
            
            client.take_leadership(total_messages)
            self.client_by_id[client_id] = client
        self._request_sums_messages_amount_with_exchange(client, self.control_broadcast_exchange_data)

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == message_protocol.internal.DATA_MESSAGE_TYPE:
            _, client_id, fruit, amount = fields
            self._process_data(client_id, fruit, amount)
        elif msg_type == message_protocol.internal.EOF_MESSAGE_TYPE:
            _, client_id, total_messages = fields
            self._process_eof(client_id, total_messages)
        else:
            logging.error(f"Unknown message type: {msg_type}")
            nack()
            return

        ack()

    def start(self):
        # Inicio el hilo de control
        control_thread = threading.Thread(
            target=self.control_input_exchange.start_consuming,
            args=(self.process_control_message,)
        )
        control_thread.start()

        self.input_queue.start_consuming(self.process_data_message)

        self.control_input_exchange.close()
        control_thread.join()
        
        self.input_queue.close()
        self.control_broadcast_exchange_control.close()
        self.control_broadcast_exchange_data.close()

        for control_output_exchange in self.control_output_exchanges.values():
            control_output_exchange.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()
    
    def stop(self):
        logging.info("Stopping SumFilter")
        self.input_queue.stop_consuming()
        self.control_input_exchange.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def sigterm_handler(signum, frame):
        sum_filter.stop()

    signal.signal(signal.SIGTERM, sigterm_handler)

    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
