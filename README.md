# ZeroMQ OOP Patterns

Small Python examples that encapsulate socket lifecycle and application roles in classes. Requires Python 3.10+ and `pip install -r requirements.txt` (or install `pyzmq`). Run scripts from the repository root. Stop long-running programs with Ctrl+C. Each socket is created and used by one thread only.

1. `01-oop-req-rep/`: request/reply server and client classes.
2. `02-oop-pub-sub/`: publisher/subscriber classes with topic filtering.
3. `03-oop-worker-node/`: poll-driven worker class with graceful signal handling.
4. `04-multiprocess-oop-pool/`: ROUTER/DEALER process pool using a class per worker; illustrates local IPC and process cleanup.

The REQ/REP protocol requires alternating send/receive. For production, add timeouts, message validation, authentication, and reconnect policies appropriate to your deployment.
