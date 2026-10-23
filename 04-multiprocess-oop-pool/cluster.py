import multiprocessing as mp,time,zmq
class Worker:
 def __init__(self,endpoint,wid):self.endpoint=endpoint;self.wid=wid
 def run(self):
  ctx=zmq.Context();s=ctx.socket(zmq.REP);s.connect(self.endpoint)
  try:
   while True:
    job=s.recv_json()
    if job.get('stop'):break
    s.send_json({'worker':self.wid,'result':job['value']*3})
  finally:s.close(0);ctx.term()
def broker(front,back):
 c=zmq.Context();a=c.socket(zmq.ROUTER);b=c.socket(zmq.DEALER);a.bind(front);b.bind(back)
 try:zmq.proxy(a,b)
 except KeyboardInterrupt:pass
 finally:a.close(0);b.close(0);c.term()
def main():
 front='tcp://127.0.0.1:5583';back='tcp://127.0.0.1:5584';b=mp.Process(target=broker,args=(front,back));b.start();time.sleep(.3);ws=[mp.Process(target=Worker(back,i).run) for i in range(2)]
 for w in ws:w.start()
 try:
  c=zmq.Context();s=c.socket(zmq.REQ);s.connect(front)
  for n in range(6):s.send_json({'value':n});print(s.recv_json())
  s.close(0);c.term()
 finally:
  for w in ws:w.terminate();w.join()
  b.terminate();b.join()
if __name__=='__main__':mp.freeze_support();main()
