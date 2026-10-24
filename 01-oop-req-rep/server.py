"""OOP pattern 1a: Reusable REP server class.

Design ideas
  * The class owns context + socket (encapsulated lifecycle).
  * `handle_request()` is the only method a subclass has to override
    (Template Method pattern) - networking stays in the base class.
  * Usable as context manager: `with JsonServer(...) as server: server.serve()`
  * Graceful shutdown on SIGINT/SIGTERM via a poll loop with timeout.
"""

import logging
import signal

import zmq

log = logging.getLogger(__name__)


class JsonServer:
    def __init__(self, endpoint: str = "tcp://127.0.0.1:5580", poll_ms: int = 500):
        self.endpoint = endpoint
        self.poll_ms = poll_ms
        self.context = zmq.Context.instance()
        self.socket = None
        self._running = False

    # ---- lifecycle -----------------------------------------------------
    def __enter__(self):
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(self.endpoint)
        log.info("%s listening on %s", type(self).__name__, self.endpoint)
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def close(self) -> None:
        if self.socket is not None and not self.socket.closed:
            self.socket.close(linger=0)
            log.info("%s closed", type(self).__name__)

    def stop(self, *_):
        self._running = False

    # ---- main loop -----------------------------------------------------
    def serve(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        self._running = True
        while self._running:
            if not dict(poller.poll(self.poll_ms)).get(self.socket):
                continue
            try:
                request = self.socket.recv_json()
                reply = self.handle_request(request)
            except Exception as exc:  # never leave a REP socket without reply
                log.exception("request failed")
                reply = {"ok": False, "error": str(exc)}
            self.socket.send_json(reply)

    # ---- business logic (override me) ----------------------------------
    def handle_request(self, request: dict) -> dict:
        return {"ok": True, "echo": request}


class CalculatorServer(JsonServer):
    """Example subclass: only the business logic changes."""

    OPERATIONS = {
        "add": lambda a, b: a + b,
        "sub": lambda a, b: a - b,
        "mul": lambda a, b: a * b,
        "div": lambda a, b: a / b,
    }

    def handle_request(self, request: dict) -> dict:
        op = self.OPERATIONS.get(request.get("op"))
        if op is None:
            return {"ok": False, "error": f"unknown operation {request.get('op')!r}"}
        return {"ok": True, "result": op(request["a"], request["b"])}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    with CalculatorServer() as server:
        server.serve()
    zmq.Context.instance().term()
