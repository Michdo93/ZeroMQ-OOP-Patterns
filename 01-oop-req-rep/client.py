"""OOP pattern 1b: Reusable REQ client with timeout and retries.

A plain REQ socket gets stuck forever if the server dies between send() and
recv() (the REQ state machine then refuses another send()). This class
implements the "Lazy Pirate" idea from the ZeroMQ guide: on timeout the socket
is closed, recreated and the request is resent - up to `retries` times.
"""

import zmq


class ServerUnavailable(RuntimeError):
    pass


class JsonClient:
    def __init__(self, endpoint: str = "tcp://127.0.0.1:5580", timeout_ms: int = 2000, retries: int = 3):
        self.endpoint = endpoint
        self.timeout_ms = timeout_ms
        self.retries = retries
        self.context = zmq.Context.instance()
        self.socket = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def _connect(self) -> None:
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.connect(self.endpoint)

    def close(self) -> None:
        if self.socket is not None:
            self.socket.close()
            self.socket = None

    def request(self, payload: dict) -> dict:
        for attempt in range(1, self.retries + 1):
            self.socket.send_json(payload)
            if self.socket.poll(self.timeout_ms, zmq.POLLIN):
                return self.socket.recv_json()
            print(f"  no reply (attempt {attempt}/{self.retries}) - reconnecting")
            # The REQ socket is now in "waiting for reply" state -> recreate it.
            self.close()
            self._connect()
        raise ServerUnavailable(f"{self.endpoint} did not answer")

    # Convenience API: hides the message format from the caller
    def calculate(self, op: str, a: float, b: float) -> float:
        reply = self.request({"op": op, "a": a, "b": b})
        if not reply.get("ok"):
            raise ValueError(reply.get("error"))
        return reply["result"]


if __name__ == "__main__":
    with JsonClient() as client:
        try:
            print("3 + 4 =", client.calculate("add", 3, 4))
            print("6 * 7 =", client.calculate("mul", 6, 7))
            print("1 / 0 ->", client.request({"op": "div", "a": 1, "b": 0}))
            print("pow ->", client.request({"op": "pow", "a": 2, "b": 3}))
        except ServerUnavailable as exc:
            print("Error:", exc)
    zmq.Context.instance().term()
