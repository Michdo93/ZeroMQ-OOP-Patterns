import signal,zmq
class Worker:
 def __init__(self,endpoint='tcp://127.0.0.1:5582'):self.ctx=zmq.Context();self.s=self.ctx.socket(zmq.REP);self.s.connect(endpoint);self.running=True
 def stop(self,*_):self.running=False
 def run(self):
  signal.signal(signal.SIGINT,self.stop);signal.signal(signal.SIGTERM,self.stop);poller=zmq.Poller();poller.register(self.s,zmq.POLLIN)
  try:
   while self.running:
    if dict(poller.poll(500)).get(self.s,0)&zmq.POLLIN:
     job=self.s.recv_json();self.s.send_json({'worker':'oop-worker','result':job.get('value',0)*2})
  finally:self.s.close(0);self.ctx.term()
if __name__=='__main__':Worker().run()
