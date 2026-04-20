import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_fruit_by_client = {}
        self.sum_eof_received_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        if client_id not in self.amount_by_fruit_by_client:
            self.amount_by_fruit_by_client[client_id] = {}
        if fruit not in self.amount_by_fruit_by_client[client_id]:
            self.amount_by_fruit_by_client[client_id][fruit] = fruit_item.FruitItem(fruit, 0)
        self.amount_by_fruit_by_client[client_id][fruit] += fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        self.sum_eof_received_by_client[client_id] = self.sum_eof_received_by_client.get(client_id, 0) + 1


        if self.sum_eof_received_by_client[client_id] < SUM_AMOUNT:
            logging.info(f"EOF messages for client {client_id}: {self.sum_eof_received_by_client[client_id]}/{SUM_AMOUNT}")
            return
        logging.info(f"All EOF messages received for client {client_id}. Computing top fruits.")

        fruit_top = []

        client_totals = self.amount_by_fruit_by_client.get(client_id)
        if client_totals is not None:
            sorted_fruit_totals = sorted(client_totals.values(), reverse=True)
            fruit_chunk = sorted_fruit_totals[:TOP_SIZE]
            fruit_top = [(fi.fruit, fi.amount) for fi in fruit_chunk]

        self.output_queue.send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.TOP_MESSAGE_TYPE,
                    client_id,
                    fruit_top
                ]
            )
        )
        self.amount_by_fruit_by_client.pop(client_id, None)
        self.sum_eof_received_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == message_protocol.internal.DATA_MESSAGE_TYPE:
            _, client_id, fruit, amount = fields
            self._process_data(client_id, fruit, amount)
        elif msg_type == message_protocol.internal.EOF_MESSAGE_TYPE:
            _, client_id = fields
            self._process_eof(client_id)
        else:
            logging.error("Unknown message type")
            nack()
            return
        
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)
        self.output_queue.close()
        self.input_exchange.close()
    
    def stop(self):
        logging.info("Stopping AggregationFilter")
        self.input_exchange.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()

    def sigterm_handler(signum, frame):
        aggregation_filter.stop()

    signal.signal(signal.SIGTERM, sigterm_handler)

    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
