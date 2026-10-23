# ZeroMQ OOP Patterns

Python 3.10+ class-based examples. Install from repository root:

```bash
sudo apt update && sudo apt install -y python3 python3-venv
python3 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Run from root with venv active. Separate terminals for peers. Stop long-running scripts with Ctrl+C. A socket belongs to one thread and should close before its context.

## 1. OOP REQ/REP

Terminal A: `python 01-oop-req-rep/server.py`
Terminal B: `python 01-oop-req-rep/client.py`

Sends status JSON and prints echo. Endpoint `tcp://127.0.0.1:5580`; change constructor endpoint in both peers together. Change client payload to personalize.

## 2. OOP PUB/SUB

Start subscriber: `python 02-oop-pub-sub/subscriber.py`
Then publisher: `python 02-oop-pub-sub/publisher.py`

Sends twenty `updates` messages every 0.5 sec. Change topic in `Publisher.publish()` and `Subscriber('updates')` together; change loop/delay to adjust volume/rate. Endpoint 5581. PUB/SUB may lose startup messages; it is not guaranteed delivery.

## 3. OOP worker node

`python 03-oop-worker-node/worker.py`

This worker component waits for an external peer. It connects as REP to `tcp://127.0.0.1:5582`, expects JSON with optional numeric `value`, and replies with twice that value. Use a compatible ROUTER/DEALER broker/service binding this endpoint to exercise it. Change `Worker.__init__` and peer endpoint together; change calculation in `run()`. Ctrl+C/SIGTERM stops poll loop.

## 4. Multiprocess OOP pool

`python 04-multiprocess-oop-pool/cluster.py`

Self-contained: starts broker, two workers, six jobs; returns input times three. Change `range(2)` workers, `range(6)` jobs, or `Worker.run()` calculation. Ports 5583/5584 are paired; update all endpoints if changed. It terminates children at completion; use graceful shutdown/supervision in production.

## Troubleshooting/customization

If port is in use, stop prior process or update all endpoint strings. REQ/REP alternates send/receive. Keep sender JSON and receiver aligned. Worker-node requires external peer; pool is directly runnable. Demos use plaintext loopback; add validation, timeouts, authentication and CURVE before remote use.
