import json

DATA_MESSAGE_TYPE = "DATA"
EOF_MESSAGE_TYPE = "EOF"
PARTIAL_TOP_MESSAGE_TYPE = "PARTIAL_TOP"
TOP_MESSAGE_TYPE = "TOP"

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
