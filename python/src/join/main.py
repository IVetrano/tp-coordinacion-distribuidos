import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.tops_received_by_client = {}
        self.current_top_by_client = {}

    def process_messsage(self, message, ack, nack):
        msg_type, client_id, fruit_top = message_protocol.internal.deserialize(message)
        if msg_type != message_protocol.internal.PARTIAL_TOP_MESSAGE_TYPE:
            logging.error(f"Unexpected message type: {msg_type}")
            nack()
            return

        logging.info(f"Received partial top for client {client_id}: {fruit_top}")
        if client_id not in self.tops_received_by_client:
            self.tops_received_by_client[client_id] = 0
            self.current_top_by_client[client_id] = []

        self.tops_received_by_client[client_id] += 1
        top_mapped = [fruit_item.FruitItem(fruit, amount) for fruit, amount in fruit_top]

        # Junto el top recibido con el top actual del cliente y me quedo con el top TOP_SIZE
        merged_top = self.current_top_by_client[client_id] + top_mapped
        sorted_merged_top = sorted(merged_top, reverse=True)
        self.current_top_by_client[client_id] = sorted_merged_top[:TOP_SIZE]

        if self.tops_received_by_client[client_id] < AGGREGATION_AMOUNT:
            logging.info(f"Partial tops received for client {client_id}: {self.tops_received_by_client[client_id]}/{AGGREGATION_AMOUNT}")
            ack()
            return
        
        logging.info(f"All partial tops received for client {client_id}. Sending final top.")
        final_top = [(fi.fruit, fi.amount) for fi in self.current_top_by_client[client_id]]
        self.output_queue.send(
            message_protocol.internal.serialize(
                [
                    message_protocol.internal.TOP_MESSAGE_TYPE,
                    client_id,
                    final_top
                ]
            )
        )
        self.tops_received_by_client.pop(client_id, None)
        self.current_top_by_client.pop(client_id, None)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

        self.output_queue.close()
        self.input_queue.close()

    def stop(self):
        logging.info("Stopping JoinFilter")
        self.input_queue.stop_consuming()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def sigterm_handler(signum, frame):
        join_filter.stop()
    
    signal.signal(signal.SIGTERM, sigterm_handler)

    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
