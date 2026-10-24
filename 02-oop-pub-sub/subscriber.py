"""OOP pattern 2b: Subscriber base class with an `on_message()` hook.

The base class handles connect, subscriptions, the poll loop, decoding and
shutdown. Subclasses only implement what should happen with a message -
for example storing it in a database, forwarding it to openHAB or updating
a GUI.
"""

import json
import signal
import time

import zmq


class Subscriber:
    def __init__(self, endpoint: str = "tcp://127.0.0.1:5581", topics=("",), deserializer=None):
        self.endpoint = endpoint
        self.deserialize = deserializer or (lambda raw: json.loads(raw))
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(endpoint)
        for topic in topics:
            self.subscribe(topic)
        self._running = False

    def subscribe(self, topic: str) -> None:
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)

    def unsubscribe(self, topic: str) -> None:
        self.socket.setsockopt_string(zmq.UNSUBSCRIBE, topic)

    def stop(self, *_):
        self._running = False

    def run(self, duration: float = None) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        deadline = time.time() + duration if duration else None
        self._running = True
        try:
            while self._running and (deadline is None or time.time() < deadline):
                if self.socket.poll(200):
                    topic, raw = self.socket.recv_multipart()
                    self.on_message(topic.decode(), self.deserialize(raw))
        finally:
            self.socket.close(linger=0)
            self.on_close()

    # ---- hooks ----------------------------------------------------------
    def on_message(self, topic: str, data) -> None:
        print(f"{topic:<20} {data}")

    def on_close(self) -> None:
        pass


class TemperatureMonitor(Subscriber):
    """Example subclass: computes a running average and raises warnings."""

    def __init__(self, limit: float = 22.5, **kwargs):
        super().__init__(topics=("sensor.temperature", "alarm."), **kwargs)
        self.limit = limit
        self.values = []

    def on_message(self, topic: str, data) -> None:
        if topic.startswith("alarm."):
            print(f"[ALARM] {topic}: {data}")
            return
        self.values.append(data["value"])
        avg = sum(self.values) / len(self.values)
        flag = "  <-- above limit" if data["value"] > self.limit else ""
        print(f"{data['room']}: {data['value']:5.2f} C (avg {avg:5.2f}){flag}")

    def on_close(self) -> None:
        print(f"Monitor stopped after {len(self.values)} temperature values")


if __name__ == "__main__":
    TemperatureMonitor().run()
    zmq.Context.instance().term()
