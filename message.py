import datetime
import json
from json import JSONEncoder
import yaml

import configure
import copy

class Message(object):
	def __init__(self, contentStr):
		self.toString = contentStr.strip()
		self.sender = ""
		self.time = ""
		self.type = "ack"

	def signName(self, name):
		self.sender = int(name)
		self.toString += " " + str(self.sender)

	def signTime(self):
		self.time = datetime.datetime.now().time().strftime("%H:%M:%S")
		self.toString += " " + self.time

	def __json__(self):
		return dict( \
			type=self.type, \
			sender=self.sender, \
			time=self.time, \
			)
		

class Request(Message):
	def __init__(self, contentStr):
		Message.__init__(self, contentStr)
		components = contentStr.strip().split()
		self.cmd = components[0]
		self.key = int(components[1])
		if self.cmd in ["insert", "update"]:
			self.value = int(components[2])
		else:
			self.value = ""
		self.model = int(components[-1])
		self.type = "request"

	def __json__(self):
		return dict( \
			type=self.type, \
			cmd=self.cmd, \
			key=self.key, \
			value=self.value, \
			model=self.model, \
			sender=self.sender, \
			time=self.time, \
			)

class Repair(Message):
	def __init__(self,kv):
		self.kvStore = copy.deepcopy(kv)
		self.kvStore["cmd"] = "repair"

	def __json__(self):
		return self.kvStore


class ValueResponse(Message):
	def __init__(self, value_timestamp_pair):
		Message.__init__(self, "Value")
		self.value = int(value_timestamp_pair['value'])
		self.timestamp = value_timestamp_pair['timestamp']
		self.type = "ValueResponse"

	def __json__(self):
		return dict( \
			type=self.type, \
			value=self.value, \
			sender=self.sender, \
			timestamp=self.timestamp, \
			)

class MessageEncoder(JSONEncoder):
	def default(self, obj):
		return obj.__json__()

def signNameForJsonStr(json_msg_str, name):
	decoded_msg = yaml.load(json_msg_str)
	if "sender" in decoded_msg:
		decoded_msg['original_sender'] = decoded_msg['sender']
		decoded_msg['sender'] = int(name)
	return json.dumps(decoded_msg)

def isRead(json_msg_str):
	decoded_msg = yaml.load(json_msg_str)
	return decoded_msg['cmd'] == 'get'
