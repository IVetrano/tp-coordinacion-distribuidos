from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.query_id = str(uuid.uuid4())
        pass
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(["DATA", self.query_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(["EOF", self.query_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
