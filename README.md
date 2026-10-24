# ZeroMQ OOP Patterns

Object-oriented building blocks for **ZeroMQ in Python**: reusable server, client, publisher, subscriber, broker and worker classes with clean lifecycle handling, hook methods for your own business logic and graceful shutdown.

Companion repository of the book *From Zero to Hero – ZeroMQ in Python meistern*.

| # | Folder | Classes | Ideas shown |
|--:|--------|---------|-------------|
| 1 | [`01-oop-req-rep`](01-oop-req-rep) | `JsonServer`, `CalculatorServer`, `JsonClient` | Template method `handle_request()`, context manager, error replies, *Lazy Pirate* retries |
| 2 | [`02-oop-pub-sub`](02-oop-pub-sub) | `Publisher`, `Subscriber`, `TemperatureMonitor` | Two-frame messages `[topic, payload]`, pluggable (de)serializer, hook `on_message()` |
| 3 | [`03-oop-worker-node`](03-oop-worker-node) | `LoadBalancingBroker`, `Worker`, `SlowWorker`, `TaskClient` | ROUTER/ROUTER broker with READY protocol, envelopes, LRU worker queue |
| 4 | [`04-multiprocess-oop-pool`](04-multiprocess-oop-pool) | `TaskDistributor`, `JsonWorker` | Objects created *inside* child processes, pull protocol, `SHUTDOWN`, Ctrl+C handling |

## Installation

```bash
sudo apt update && sudo apt install -y python3 python3-venv
git clone https://github.com/Michdo93/ZeroMQ-OOP-Patterns.git
cd ZeroMQ-OOP-Patterns
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 1. REQ/REP classes

```bash
python 01-oop-req-rep/server.py      # Terminal A
python 01-oop-req-rep/client.py      # Terminal B
```

```
3 + 4 = 7
6 * 7 = 42
1 / 0 -> {'ok': False, 'error': 'division by zero'}
```

Stop the server and run the client again: it retries three times (`no reply ... reconnecting`) and then raises `ServerUnavailable` instead of hanging forever. Create your own server by subclassing `JsonServer` and overriding `handle_request()`.

## 2. PUB/SUB classes

```bash
python 02-oop-pub-sub/subscriber.py  # Terminal A (start first)
python 02-oop-pub-sub/publisher.py   # Terminal B
```

`TemperatureMonitor` subscribes to `sensor.temperature` and `alarm.` (prefix match) and prints a running average. Override `on_message(topic, data)` for your own reaction, pass `serializer`/`deserializer` to use e.g. MessagePack.

## 3. Worker node + load-balancing broker

```bash
python 03-oop-worker-node/broker.py                       # Terminal A
python 03-oop-worker-node/worker.py --name fast-1         # Terminal B
python 03-oop-worker-node/worker.py --name fast-2         # Terminal C
python 03-oop-worker-node/worker.py --name slow-1 --slow  # Terminal D
python 03-oop-worker-node/client.py                       # Terminal E
# Jobs per worker: {'fast-1': 5, 'fast-2': 5, 'slow-1': 2}
```

Workers announce themselves with `READY`; the broker only forwards a request to an *idle* worker, so the slow worker automatically gets fewer jobs (unlike round-robin DEALER distribution).

## 4. Multiprocess OOP pool

```bash
python 04-multiprocess-oop-pool/cluster.py
```

Starts one distributor and one worker process per CPU core and processes 24 CPU-bound tasks. Press Ctrl+C during the run: the distributor stops handing out tasks, sends `SHUTDOWN` to every worker and all processes exit cleanly.

## Design rules used in all classes

* A class owns its sockets and closes them in `close()` / `__exit__` / `finally`.
* Loops poll with a timeout (`poll(200…500 ms)`) so signals are handled promptly.
* REP sockets always send a reply – also on exceptions – otherwise the state machine is stuck.
* Objects that live in a child process are created **in** that process.
* Business logic lives in overridable methods (`handle_request`, `on_message`, `process_task`, `process`).
