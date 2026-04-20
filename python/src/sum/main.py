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

class SumFilter:
    def __init__(self):
        self.lock = threading.Lock()

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, 
            SUM_CONTROL_EXCHANGE, 
            [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )

        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT) if i != ID]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            if client_id not in self.amount_by_fruit_by_client:
                self.amount_by_fruit_by_client[client_id] = {}
            self.amount_by_fruit_by_client[client_id][fruit] = self.amount_by_fruit_by_client[client_id].get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        with self.lock:
            client_totals = self.amount_by_fruit_by_client.pop(client_id, {})

        logging.info(f"Broadcasting data messages")
        for final_fruit_item in client_totals.values():
            self._send_to_aggregator(
                client_id,
                final_fruit_item.fruit,
                final_fruit_item.amount
            )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [
                        message_protocol.internal.EOF_MESSAGE_TYPE,
                        client_id
                    ]
                )
            )

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

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == message_protocol.internal.DATA_MESSAGE_TYPE:
            _, client_id, fruit, amount = fields
            self._process_data(client_id, fruit, amount)
        elif msg_type == message_protocol.internal.EOF_MESSAGE_TYPE:
            # Si hay mas Sums, les envio el eof
            if SUM_AMOUNT > 1:
                self.control_output_exchange.send(message)

            _, client_id = fields
            self._process_eof(client_id)
        else:
            logging.error(f"Unknown message type: {msg_type}")
            nack()
            return

        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == message_protocol.internal.EOF_MESSAGE_TYPE:
            _, client_id = fields
            self._process_eof(client_id)
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

        self.input_queue.start_consuming(self.process_data_messsage)

        self.control_input_exchange.close()
        control_thread.join()
        
        self.input_queue.close()
        self.control_output_exchange.close()
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
