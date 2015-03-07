CoordinatorPort = 4005
NodePortList = [4001, 4002, 4003, 4004]

CoordinatorDelay = 5
NodeDelayList = [4, 5, 6, 7]

Commands = ["delete", "get", "insert", "update"]

ACK_MSG = "ack"

def GetCoodPortNumber():
	return CoordinatorPort

def GetNodePortNumber(node_name):
	return NodePortList[ord(node_name[0])-ord('A')]

def GetCoodDelay():
	return CoordinatorDelay

def GetNodeDelay(node_name):
	return NodeDelayList[ord(node_name[0]) - ord('A')]
