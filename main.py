import random
import time
import sys
import signal
import json
import uuid
import cloudpickle
import base64
import socket
import traceback

import redis

from threading import Thread

class this():
	pass

class HiveMindBase():
	#get a reply
	def reply(self, cmdid, *args):
		#reply using the command and each input following it
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
			
		print json.dumps(args1)
		self.conn.send(json.dumps(args1))
	
	# send something
	def send(self, channel, cmd, *args):
		cmdid = str(uuid.uuid4())
		
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
		
		args1.insert(0, cmd)
		args1.insert(0, self.hostname)
		args1.insert(0, cmdid)
		print cmdid
		print args1
		self.redis.publish(channel, json.dumps(args1))
		
		#lets take a moment to check if there is anything waiting for us
		
	def recv(self):
		print "meh"
		

class HiveMind(HiveMindBase):
	
	listener = None
	hostname = None
	nodes = dict()
	clients = dict()
	pubsub = dict()
	redis_pool = None
	redis = None
	
	#todo: take my tags now so i send this over the wire
	def __init__(self, hostname=None, **kwargs):
		
		self.redis = redis.ConnectionPool(host="127.0.0.1", port=6379)
		if hostname is None:
			hostname = "{}:{}".format(socket.gethostname(), random.randint(1, 10000))
			
		self.hostname = hostname
		self.pubsub['self'] = redis.Redis(connection_pool=self.redis_pool).pubsub()
		self.pubsub['self'].subscribe(["hivemind", hostname])
		self.redis = redis.Redis(connection_pool=self.redis_pool)
		
		self.join_cluster()
		Thread(target=self.server_listen, name="server_listen", args=()).start()
		
	# wait for something to happen
	def server_listen(self):
		for item in self.pubsub['self'].listen():
			try:
				data = json.loads(item["data"])
				if data[2] == "load_obj": 
					self.load_obj(data[3]["name"], data[3]["obj"])
				elif data[2] == "run_obj":
					self.run_obj(data[0], data[3]["name"], data[3]["args"])
				elif data[2] == "spawn":
					self.spawn(None, data[3]["mod"], data[3]["func"], data[3]["args"])
			except Exception, e:
				
				#print str(e)
				traceback.print_exc()
				
				if item['data'] == "KILL":
					self.pubsub["self"].unsubscribe()
					print self, "unsubscribed and finished"
					break
				else:
					print item
			
			
			
	
	# load data into the system
	#todo: we should encrypt the data when it gets sent over the wire
	#      this way we can decrypt it to make sure its trusted
	def load_obj(self, name, data):
		global this
		try:
			setattr(this, name, cloudpickle.loads(base64.b64decode(data))) #decode the data and store it
			print this
		finally:
			pass
			#self.send((cmdid, True)) #send a reply we did as was asked
			
	def send_obj(self, name, func):
		data = cloudpickle.dumps(func)
		self.send("hivemind", "load_obj", {"name": name, "obj": base64.b64encode(data)})
		
		
	#execute the data you wish to load
	#todo: should make it send a connection outbound just incase we disconnect
	def run_obj(self, cmdid, name, argz):
		res = this[name](*argz)
		res = getattr(this(), name)(*argz)
		#self.reply(cmdid, res)
		
		
	def spawn(self, channel, mod, func, args):
		if channel is None or channel == "self":
			bee = HiveMindBee(self.hostname, self.redis_pool)
			getattr(this(), func)(bee, *args)
		else:
			self.send(channel, "spawn", {"mod":mod, "func":func, "args":args, "from": self.hostname})

	
	# add my self to the nodes in the cluster
	# let all memebers know i am here
	def join_cluster(self):
		self.redis.hset("hivemind_nodes", self.hostname, time.time())
		self.redis.publish("hivemind", json.dumps({"action": "join_cluster", "hostname": self.hostname}))
		
class HiveMindBee(HiveMindBase):
	
	inbox = None
	inbox_name = None
	redis_pool = None
	redis = None
	hostname = None
	threadid = None
	
	def __init__(self, hostname, redis_pool):
		self.redis_pool = redis_pool
		self.redis = redis.Redis(connection_pool=self.redis_pool)
		self.hostname = hostname
		
		
	def do(mod, func, args, caller):
		self.threadid = thread.ident()
		self.inbox_name = "{}:{}".format(hostname, self.threadid)
		self.inbox = self.redis.pubsub()
	
	
	def send(self, channel, cmd, *args):
		cmdid = str(uuid.uuid4())
		
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
		
		args1.insert(0, cmd)
		args1.insert(0, self.inbox_name)
		args1.insert(0, cmdid)
		print cmdid
		print args1
		self.redis.publish(channel, json.dumps(args1))
		
	def __del__ ():
		pass
		

	
		
class HiveMindNode(HiveMindBase):
	
	conn = None
	hostname = None
	tags = None
	
	def __init__(self, name, tags, conn, **kwargs):
		self.conn = conn
		self.hostname = name
		self.tags = tags
		
	#send the object over the wire
	def send_obj(self, name, func):
		data = cloudpickle.dumps(func)
		self.send("load_obj", name, base64.b64encode(data))
	
	#run the object on the other end
	def run_obj(self, obj_name, args=[]):
		self.send("run_obj", {"name": obj_name, "args": args})
	
	
	#self start a thread on the other end
	#def spawn()
	



		
		
		
		
		
if __name__ == "__main__":
	
	
	
	def hand_inter(signum, frame):
		print "trying to exit"
		sys.exit()

	signal.signal(signal.SIGINT, hand_inter)
	
	
	
	hm = HiveMind()
	
	"""
	while True:
		time.sleep(60)
		print "next pass"
    """
	
#conn.close()
#listener.close()
