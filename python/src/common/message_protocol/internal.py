import json

# Data messages
DATA_MESSAGE_TYPE = "DATA"
EOF_MESSAGE_TYPE = "EOF"
PARTIAL_TOP_MESSAGE_TYPE = "PARTIAL_TOP"
TOP_MESSAGE_TYPE = "TOP"

# Sum control messages
AMOUNT_REQUEST_MESSAGE_TYPE = "AMOUNT_REQUEST"
AMOUNT_RESPONSE_MESSAGE_TYPE = "AMOUNT_RESPONSE"
FLUSH_MESSAGE_TYPE = "FLUSH"
MAX_AMOUNT_REACHED_MESSAGE_TYPE = "MAX_AMOUNT_REACHED"

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
