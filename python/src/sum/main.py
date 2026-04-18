import os
import logging
import threading

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
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
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
        if client_id not in self.amount_by_fruit_by_client:
            self.amount_by_fruit_by_client[client_id] = {}
        self.amount_by_fruit_by_client[client_id][fruit] = self.amount_by_fruit_by_client[client_id].get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit_by_client.get(client_id, {}).values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [
                            message_protocol.internal.DATA_MESSAGE_TYPE,
                            client_id,
                            final_fruit_item.fruit,
                            final_fruit_item.amount
                        ]
                    )
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


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        msg_type = fields[0]

        if msg_type == message_protocol.internal.DATA_MESSAGE_TYPE:
            _, client_id, fruit, amount = fields
            self._process_data(client_id, fruit, amount)
        elif msg_type == message_protocol.internal.EOF_MESSAGE_TYPE:
            _, client_id = fields
            self._process_eof(client_id)
        else:
            logging.error(f"Unknown message type: {msg_type}")
            nack()
            return

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
