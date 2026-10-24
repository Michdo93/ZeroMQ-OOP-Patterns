"""OOP pattern 2a: Publisher class with a pluggable serializer.

Messages are sent as two frames: [topic, payload]. Keeping the topic in its
own frame means subscribers filter on the topic only and the payload can be
any binary format (JSON here, MessagePack/Protobuf/raw bytes elsewhere).
"""

import json
import random
import time

import zmq


class Publisher:
    def __init__(self, endpoint: str = "tcp://127.0.0.1:5581", serializer=None):
        self.endpoint = endpoint
        self.serialize = serializer or (lambda obj: json.dumps(obj).encode())
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.PUB)
        # Keep at most 1000 messages per slow subscriber, then drop.
        self.socket.setsockopt(zmq.SNDHWM, 1000)
        self.socket.bind(endpoint)

    def publish(self, topic: str, data) -> None:
        self.socket.send_multipart([topic.encode(), self.serialize(data)])

    def close(self) -> None:
        self.socket.close(linger=500)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


if __name__ == "__main__":
    with Publisher() as pub:
        time.sleep(0.5)  # slow joiner
        for i in range(20):
            pub.publish("sensor.temperature", {"room": "lab", "value": round(20 + random.random() * 3, 2)})
            pub.publish("sensor.humidity", {"room": "lab", "value": round(40 + random.random() * 10, 1)})
            if i % 5 == 0:
                pub.publish("alarm.smoke", {"room": "kitchen", "active": False})
            time.sleep(0.25)
    zmq.Context.instance().term()
