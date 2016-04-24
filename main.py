import random
import time
import sys
import signal
import json
import uuid
import cloudpickle
import base64
import socket

from multiprocessing.connection import Listener
from multiprocessing.connection import Client
from threading import Thread

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
	def send(self, cmd, *args):
		cmdid = str(uuid.uuid4())
		
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
		
		args1.insert(0, cmd)
		args1.insert(0, cmdid)
		print cmdid
		print args1
		self.conn.send(json.dumps(args1))
		
		#lets take a moment to check if there is anything waiting for us
		
	def recv(self):
		print "meh"
		

class HiveMind():
	
	listener = None
	hostname = None
	nodes = dict()
	clients = dict()
	my_queue = dict()
	out_queue = dict()
	
	#todo: take my tags now so i send this over the wire
	def __init__(self, port, hostname=None, **kwargs):
		address = ('localhost', port)     # family is deduced to be 'AF_INET'
		if hostname is None:
			hostname = "{}:{}".format(socket.gethostname(), port)
			
		self.hostname = hostname
		self.listener = Listener(address, authkey='secret password')
		Thread(target=self.accept_wait, name="accept_wait", args=(self.listener,)).start()
		
	# wait for something to happen
	def accept_wait(self, listener):
		print "waiting for new connection"
		while True:
			conn = listener.accept()
			thread_name = "{}:{}".format("client", str(uuid.uuid4()))
			print "thread hijack: {}".format(thread_name)
			
			
			#wait up to 60 seconds to get the intro info
			print "waiting for intro"
			conn.poll(60)
			cmd = json.loads(conn.recv())
			print cmd
			if cmd[1] == "initialize":
				client = self.recv_intro(conn, cmd)
				Thread(target=client.poll(), name=thread_name, args=()).start()
			else:
				print "took to long to initalize"
				conn.close()
	
	#get a reply
	def reply(self, conn, cmdid, *args):
		#reply using the command and each input following it
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
			
		print json.dumps(args1)
		conn.send(json.dumps(args1))
	
	# send something
	def send(self, conn, cmd, *args):
		cmdid = str(uuid.uuid4())
		
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
		
		args1.insert(0, cmd)
		args1.insert(0, cmdid)
		print cmdid
		print args1
		conn.send(json.dumps(args1))
	
	#todo: this will delete a node if you name them the same
	def add_node(self, name, tags, conn):
		self.nodes[name] = HiveMindNode(name, tags, conn)
		return self.nodes[name]
		
	def add_client(self, name, tags, conn):
		self.clients[name] = HiveMindClient(name, tags, conn)
		return self.clients[name]
		
	# send an intro to  the server you just connected to
	def send_intro(self, conn):
		intro_payload = dict()
		intro_payload["hostname"] = self.hostname
		intro_payload["tags"] = []
		
		print "sending intro paypload"
		print intro_payload
		self.send(conn, "initialize", intro_payload)
		conn.recv()
		
	def recv_intro(self, conn, cmd):
		self.add_client(cmd[2]["hostname"], cmd[2]["tags"], conn)
		intro_payload = dict()
		intro_payload["hostname"] = self.hostname
		intro_payload["tags"] = []
		
		self.reply(conn, cmd[0], "ready", intro_payload)
		return self.clients[cmd[2]["hostname"]]
		
	def about_me(self):
		print "hello world"
		
	def 
	
	def connect(self, host='localhost', port=6000, name=None, tags=[]):
		address = (host, port)
		print address
		conn = Client(address, authkey='secret password')
		
		if name is None:
			name = "{}:{}".format(host, port)
		
		self.send_intro(conn)
		self.add_node(name, tags, conn)
		
		print self.nodes
		
		#return the connection directly
		return self.nodes[name]
		
class HiveMindClient(HiveMindBase):
	
	conn = None
	hostname = None
	tags = None
	this = dict()
	
	def __init__(self, name, tags, conn, **kwargs):
		self.conn = conn
		self.hostname = name
		self.tags = tags
		
	# when something happeens we will try to handle it here
	def poll(self):
		print "start polling"
		while True:
			meh = self.conn.poll(None) # wait here
			print meh
			if meh == False: #odd most likly disconnect
				print "think the other end left us"
				return False
			
			cmd = json.loads(self.conn.recv())
			print cmd
			if cmd[1] == "load_obj":
				self.load_obj(cmd[0], cmd[2], cmd[3])
			elif cmd[1] == "run_obj":
				self.run_obj(cmd[0], cmd[2]['name'], cmd[2]['args'], cmd[3:])
			elif cmd[1] == "bye-bye":
				self.conn.close()
				print "closing connection from bye-bye"
			
	
			
	# load data into the system
	#todo: we should encrypt the data when it gets sent over the wire
	#      this way we can decrypt it to make sure its trusted
	def load_obj(self, cmdid, name, data):
		try:
			self.this[name] = cloudpickle.loads(base64.b64decode(data)) #decode the data and store it
			print self.this
		finally:
			self.conn.send((cmdid, True)) #send a reply we did as was asked
				
	#execute the data you wish to load
	#todo: should make it send a connection outbound just incase we disconnect
	def run_obj(self, cmdid, name, argz, *args, **kwargs):
		#background=True, returnit=False
		
		#return the output back to the caller socket
		#if returnit == True:
		#self.conn.send(("returnit", {"runid": runid, "return": res}))
		res = self.this[name](*argz)
		self.reply(cmdid, res)
		#else:
		#runid = str(uuid.uuid4())
		#start the new thread
		#self.reply(cmdid, runid, True) #send this back if there is no error starting up
	
	
	def run_greet(self, cmdid, payload, conn):
		print payload
		print
		self.add_node(payload['hostname'], [], conn)
		
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
	def spawn()
	



		
		
		
		
		
if __name__ == "__main__":
	listen_port=random.randint(2000, 3000)
	
	
	def hand_inter(signum, frame):
		print "trying to exit"
		sys.exit()

	signal.signal(signal.SIGINT, hand_inter)
	
	print "Listen port: {}".format(listen_port)
	
	hm = HiveMind(port=listen_port)
	
	"""
	while True:
		time.sleep(60)
		print "next pass"
    """
	
#conn.close()
#listener.close()
