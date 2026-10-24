"""OOP pattern 3b: Worker node for the load-balancing broker.

The worker uses a REQ socket and *starts* the conversation with "READY".
Afterwards every reply it sends is at the same time the request for the next
job - the REQ state machine (send, recv, send, recv, ...) fits perfectly.

Override `process_task()` for your own business logic (image processing,
ML inference, hardware access, ...).
"""

import argparse
import json
import os
import signal
import time

import zmq


class Worker:
    def __init__(self, name: str, endpoint: str = "tcp://127.0.0.1:5583"):
        self.name = name
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.REQ)
        # Readable identity -> shows up as worker_id in the broker
        self.socket.setsockopt_string(zmq.IDENTITY, name)
        self.socket.connect(endpoint)
        self._running = False

    def stop(self, *_):
        self._running = False

    def process_task(self, task: dict) -> dict:
        """Business logic - override in a subclass."""
        time.sleep(task.get("duration", 0.2))
        return {"result": task.get("value", 0) ** 2}

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.socket.send(b"READY")
        self._running = True
        try:
            while self._running:
                if not self.socket.poll(500):
                    continue
                client_id, empty, request = self.socket.recv_multipart()
                task = json.loads(request)
                result = self.process_task(task)
                result.update(worker=self.name, pid=os.getpid())
                print(f"[{self.name}] {task} -> {result['result']}", flush=True)
                # Reply = envelope (client id) + result
                self.socket.send_multipart([client_id, b"", json.dumps(result).encode()])
        finally:
            self.socket.close(linger=0)


class SlowWorker(Worker):
    """Takes 5x longer - the broker automatically gives it fewer jobs."""

    def process_task(self, task: dict) -> dict:
        time.sleep(task.get("duration", 0.2) * 5)
        return {"result": task.get("value", 0) ** 2}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=f"worker-{os.getpid()}")
    parser.add_argument("--slow", action="store_true")
    args = parser.parse_args()
    cls = SlowWorker if args.slow else Worker
    cls(args.name).run()
    zmq.Context.instance().term()
