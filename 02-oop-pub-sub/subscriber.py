import zmq
class Subscriber:
 def __init__(self,topic='',endpoint='tcp://127.0.0.1:5581'):self.ctx=zmq.Context();self.s=self.ctx.socket(zmq.SUB);self.s.setsockopt(zmq.SUBSCRIBE,topic.encode());self.s.connect(endpoint)
 def run(self):
  try:
   while True:
    topic,message=self.s.recv_multipart();print(topic.decode(),message.decode())
  except KeyboardInterrupt:pass
  finally:self.close()
 def close(self):self.s.close(0);self.ctx.term()
if __name__=='__main__':Subscriber('updates').run()
