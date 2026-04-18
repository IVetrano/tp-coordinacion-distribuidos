from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        pass
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            [
                message_protocol.internal.DATA_MESSAGE_TYPE,
                self.client_id,
                fruit,
                amount
            ]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            [
                message_protocol.internal.EOF_MESSAGE_TYPE,
                self.client_id
            ]
        )

    def deserialize_result_message(self, message):
        msg_type, client_id, fruit_top = message_protocol.internal.deserialize(message)

        if msg_type != message_protocol.internal.TOP_MESSAGE_TYPE:
            print(f"Received unexpected message type: {msg_type}")
            return None

        if client_id != self.client_id:
            print(f"Received message for different client: {client_id}")
            return None

        return fruit_top
