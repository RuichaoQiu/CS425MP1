import datetime
import random
import socket

import configure

def GenerateRandomDelay(x):
    if x == 0:
        return 0
    return random.randint(1,x)

def GenerateRandomPeer(num_nodes, current):
	while True:
		r = random.randint(0,num_nodes-1)
		if r != current:
			return r

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

def TimestampCmp(ts1, ts2):
	dt1 = datetime.datetime.strptime(ts1, "%H:%M:%S")
	dt2 = datetime.datetime.strptime(ts2, "%H:%M:%S")
	return dt1 > dt2

def IsCmdValid(cmd):
    return cmd in configure.Commands
