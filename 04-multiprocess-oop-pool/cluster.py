"""OOP pattern 4: A complete mini cluster in one script.

    TaskDistributor (REP, own process)  <--REQ--  N x JsonWorker (own processes)

* Every worker is an object, but it is CREATED INSIDE its child process
  (Process target = small factory function). The context and the socket are
  therefore created after the fork - never inherited.
* Protocol (worker-driven, "pull" style):
      worker -> {"status": "READY"}                 distributor -> task
      worker -> {"status": "COMPLETED", "result"}   distributor -> next task
      ...                                            distributor -> {"command": "SHUTDOWN"}
* A multiprocessing.Event allows Ctrl+C / SIGTERM handling in the main process.
"""

import multiprocessing as mp
import os
import signal
import time

import zmq

ENDPOINT = "tcp://127.0.0.1:5584"


class JsonWorker:
    def __init__(self, worker_id: int, stop: mp.Event):
        self.worker_id = worker_id
        self.stop = stop
        self.context = zmq.Context()  # created in the child process
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(ENDPOINT)

    def process(self, payload: dict) -> int:
        """Business logic: sum of squares (CPU-bound)."""
        n = payload["value"]
        return sum(i * i for i in range(n))

    def run(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)  # main process coordinates shutdown
        done = 0
        self.socket.send_json({"status": "READY", "worker_id": self.worker_id})
        try:
            while not self.stop.is_set():
                if not self.socket.poll(200):
                    continue
                task = self.socket.recv_json()
                if task.get("command") == "SHUTDOWN":
                    break
                result = self.process(task["payload"])
                done += 1
                self.socket.send_json({"status": "COMPLETED", "worker_id": self.worker_id,
                                       "task_id": task["task_id"], "result": result})
        finally:
            self.socket.close(linger=0)
            self.context.term()
        print(f"[worker {self.worker_id}] pid {os.getpid()} finished {done} tasks", flush=True)


def start_worker(worker_id: int, stop: mp.Event) -> None:
    """Process target: build the object inside the child."""
    JsonWorker(worker_id, stop).run()


class TaskDistributor:
    def __init__(self, tasks: list, num_workers: int, stop: mp.Event):
        self.tasks = list(tasks)
        self.num_workers = num_workers
        self.stop = stop

    def run(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(ENDPOINT)
        results, shut_down, next_id = {}, 0, 0
        try:
            while shut_down < self.num_workers:
                if not socket.poll(200):
                    if self.stop.is_set():  # nothing arrived and we are stopping
                        break
                    continue
                msg = socket.recv_json()
                if msg["status"] == "COMPLETED":
                    results[msg["task_id"]] = (msg["worker_id"], msg["result"])
                if self.tasks and not self.stop.is_set():
                    socket.send_json({"task_id": next_id, "payload": {"value": self.tasks.pop(0)}})
                    next_id += 1
                else:
                    socket.send_json({"command": "SHUTDOWN"})
                    shut_down += 1
        finally:
            socket.close(linger=0)
            context.term()
        per_worker = {}
        for worker_id, _ in results.values():
            per_worker[worker_id] = per_worker.get(worker_id, 0) + 1
        print(f"[distributor] {len(results)} results, per worker: {per_worker}", flush=True)


def start_distributor(tasks, num_workers, stop):
    TaskDistributor(tasks, num_workers, stop).run()


def main() -> None:
    stop = mp.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    num_workers = os.cpu_count() or 2
    tasks = [2_000_000 + i * 100_000 for i in range(24)]

    start = time.perf_counter()
    distributor = mp.Process(target=start_distributor, args=(tasks, num_workers, stop))
    distributor.start()
    workers = [mp.Process(target=start_worker, args=(i, stop)) for i in range(num_workers)]
    for w in workers:
        w.start()

    for w in workers:
        w.join()
    distributor.join()
    print(f"[main] cluster with {num_workers} workers finished in {time.perf_counter() - start:.2f} s")


if __name__ == "__main__":
    main()
