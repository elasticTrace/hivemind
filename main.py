import random
import time
import sys
import signal
import json
import uuid
import cloudpickle
import base64

from multiprocessing.connection import Listener
from multiprocessing.connection import Client
from threading import Thread

class Hivemind:
	
	listener = None
	this = dict()
	conn = None
	
	def __init__(self, port, **kwargs):
		address = ('localhost', port)     # family is deduced to be 'AF_INET'
		self.listener = Listener(address, authkey='secret password')
		Thread(target=self.accept_wait, name="accept_wait", args=(self.listener,)).start()

	def accept_handle(self, conn):
		self.conn = conn
		for r in range(10):
			meh = self.conn.poll(None)
			print meh
			if meh == False: #odd most likly disconnect
				print "think the other end left us"
				return False
			
			cmd = json.loads(self.conn.recv())
			print cmd
			if cmd[1] == "this_load":
				self.this_load(cmd[0], cmd[2], cmd[3])
			elif cmd[1] == "this_run":
				self.this_run(cmd[0], cmd[2]['name'], cmd[2]['args'], cmd[3:])
			
		
	def accept_wait(self, listener):
		print "waiting for new connection"
		while True:
			conn = listener.accept()
			thread_name = str(uuid.uuid4())
			print "thread hijack: {}".format(thread_name)
			Thread(target=self.accept_handle, name=thread_name, args=(conn,)).start()
			
	# load data into the system
	#todo: we should encrypt the data when it gets sent over the wire
	#      this way we can decrypt it to make sure its trusted
	def this_load(self, cmdid, name, data):
		try:
			self.this[name] = cloudpickle.loads(base64.b64decode(data)) #decode the data and store it
			print self.this
		finally:
			self.conn.send((cmdid, True)) #send a reply we did as was asked
			
	def this_cp_obj(self, name, func):
		data = cloudpickle.dumps(func)
		self.send("this_load", name, base64.b64encode(data))
		
		
	#execute the data you wish to load
	#todo: should make it send a connection outbound just incase we disconnect
	def this_run(self, cmdid, name, argz, *args, **kwargs):
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
		
	def reply(self, cmdid, *args):
		#reply using the command and each input following it
		if args is not None:
			args1 = list(args)
		else:
			args1 = list()
			
		print json.dumps(args1)
		self.conn.send(json.dumps(args1))
		
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
		
	
	def connect(self, host='localhost', port=6000):
		address = (host, port)
		print address
		self.conn = Client(address, authkey='secret password')
		print self.conn
		return self.conn
		
		
		
		
if __name__ == "__main__":
	listen_port=random.randint(2000, 3000)
	
	
	def hand_inter(signum, frame):
		print "trying to exit"
		sys.exit()

	signal.signal(signal.SIGINT, hand_inter)
	
	print "Listen port: {}".format(listen_port)
	
	hm = Hivemind(port=listen_port)
	
	"""
	while True:
		time.sleep(60)
		print "next pass"
    """
	
#conn.close()
#listener.close()
