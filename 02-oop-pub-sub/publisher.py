import time,zmq
class Publisher:
 def __init__(self,endpoint='tcp://127.0.0.1:5581'):self.ctx=zmq.Context();self.s=self.ctx.socket(zmq.PUB);self.s.bind(endpoint)
 def publish(self,topic,message):self.s.send_multipart([topic.encode(),message.encode()])
 def close(self):self.s.close(0);self.ctx.term()
if __name__=='__main__':
 p=Publisher();time.sleep(.5)
 try:
  for i in range(20):p.publish('updates',f'update-{i}');time.sleep(.5)
 finally:p.close()
