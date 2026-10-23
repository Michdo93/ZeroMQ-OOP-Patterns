import os,zmq
class ZmqServer:
 def __init__(self,endpoint='tcp://127.0.0.1:5580'):self.ctx=zmq.Context();self.s=self.ctx.socket(zmq.REP);self.s.bind(endpoint)
 def serve(self):
  try:
   while True:self.s.send_json({'ok':True,'echo':self.s.recv_json()})
  except KeyboardInterrupt:pass
  finally:self.close()
 def close(self):self.s.close(0);self.ctx.term()
if __name__=='__main__':ZmqServer(os.getenv('ZMQ_ENDPOINT','tcp://127.0.0.1:5580')).serve()
