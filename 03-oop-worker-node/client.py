"""OOP pattern 3c: Sends a batch of tasks in parallel and shows which worker
handled how many of them."""

import collections
import json
import threading

import zmq


class TaskClient:
    def __init__(self, endpoint: str = "tcp://127.0.0.1:5582"):
        self.endpoint = endpoint
        self.context = zmq.Context.instance()

    def submit(self, task: dict, timeout_ms: int = 10000) -> dict:
        socket = self.context.socket(zmq.REQ)  # one REQ socket per thread
        socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(self.endpoint)
        try:
            socket.send_json(task)
            return json.loads(socket.recv())
        finally:
            socket.close()


if __name__ == "__main__":
    client = TaskClient()
    results = []

    def job(i):
        results.append(client.submit({"value": i, "duration": 0.2}))

    threads = [threading.Thread(target=job, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("Jobs per worker:", dict(collections.Counter(r["worker"] for r in results)))
    zmq.Context.instance().term()
