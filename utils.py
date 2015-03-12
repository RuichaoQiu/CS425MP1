import random
import socket

def GenerateRandomDelay(x):
    if x == 0:
        return 0
    return random.randint(1,x)

def CreateClientSockets(num_socket):
    s = []
    for si in xrange(num_socket):
        st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        st.settimeout(2)
        s.append(st)
    return s

def CreateMessageQueues(num_queue):
	return [[] for i in xrange(num_queue)]

def NameToID(node_name):
	return ord(node_name[0])-ord('A')

#Request = namedtuple('Request', 'RequestMsg, Source, Broadcast, ReceiveAck')
def SignMsg(msg, signature):
 	signed_msg = msg.split()
 	signed_msg.append(signature)
 	return " ".join(signed_msg)

class Request(object):
	def __init__(self, Cmd, Key, Value, Model):
		self.Cmd = Cmd
		self.Model = Model
		self.Key = Key
		self.Value = Value

class RequestInfo(object):
	def __init__(self, RequestMsg, Source, Broadcast, ReceiveAck):
		self.RequestMsg = RequestMsg
		self.Source = Source
		self.Broadcast = Broadcast
		self.ReceiveAck = ReceiveAck
