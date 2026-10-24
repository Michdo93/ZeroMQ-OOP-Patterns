"""OOP pattern 3a: Load-balancing broker (ZeroMQ guide, chapter 3).

    REQ clients --> ROUTER frontend [broker] ROUTER backend <-- REQ workers

Difference to a plain ROUTER/DEALER proxy (round robin):
A worker announces itself with "READY" and is put into a queue of IDLE
workers. A request is forwarded only to an idle worker, so a slow or busy
worker never receives a second job. This is "least recently used" routing.

Envelopes (what a ROUTER sees):
    frontend recv:  [client_id, b"", request]
    backend  send:  [worker_id, b"", client_id, b"", request]
    backend  recv:  [worker_id, b"", client_id, b"", reply]   or
                    [worker_id, b"", b"READY"]
    frontend send:  [client_id, b"", reply]
"""

import collections
import signal

import zmq


class LoadBalancingBroker:
    def __init__(self, frontend="tcp://127.0.0.1:5582", backend="tcp://127.0.0.1:5583"):
        self.context = zmq.Context.instance()
        self.frontend = self.context.socket(zmq.ROUTER)
        self.backend = self.context.socket(zmq.ROUTER)
        self.frontend.bind(frontend)
        self.backend.bind(backend)
        self.idle_workers = collections.deque()
        self._running = False
        print(f"Broker: clients -> {frontend}, workers -> {backend}")

    def stop(self, *_):
        self._running = False

    def run(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        poll_workers = zmq.Poller()
        poll_workers.register(self.backend, zmq.POLLIN)
        poll_both = zmq.Poller()
        poll_both.register(self.backend, zmq.POLLIN)
        poll_both.register(self.frontend, zmq.POLLIN)

        self._running = True
        try:
            while self._running:
                # Accept client requests only while at least one worker is idle.
                poller = poll_both if self.idle_workers else poll_workers
                events = dict(poller.poll(500))

                if self.backend in events:
                    worker_id, _, *rest = self.backend.recv_multipart()
                    self.idle_workers.append(worker_id)
                    if rest[0] == b"READY":
                        print(f"[broker] worker {worker_id.decode()} registered")
                    else:
                        client_id, _, reply = rest
                        self.frontend.send_multipart([client_id, b"", reply])

                if self.frontend in events:
                    client_id, _, request = self.frontend.recv_multipart()
                    worker_id = self.idle_workers.popleft()
                    self.backend.send_multipart([worker_id, b"", client_id, b"", request])
        finally:
            self.frontend.close(linger=0)
            self.backend.close(linger=0)
            print("[broker] stopped")


if __name__ == "__main__":
    LoadBalancingBroker().run()
    zmq.Context.instance().term()
