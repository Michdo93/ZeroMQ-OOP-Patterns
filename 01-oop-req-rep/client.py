import zmq
class ZmqClient:
 def __init__(self,endpoint='tcp://127.0.0.1:5580'):self.ctx=zmq.Context();self.s=self.ctx.socket(zmq.REQ);self.s.setsockopt(zmq.RCVTIMEO,3000);self.s.connect(endpoint)
 def request(self,payload):self.s.send_json(payload);return self.s.recv_json()
 def close(self):self.s.close(0);self.ctx.term()
if __name__=='__main__':
 c=ZmqClient()
 try:print(c.request({'action':'status'}))
 finally:c.close()
