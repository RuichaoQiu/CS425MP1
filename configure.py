CoordinatorPort = 4005
NodePortList = [4001, 4002, 4003, 4004]

CoordinatorDelay = 5
NodeDelayList = [4, 5, 6, 7]
#DelayList = [[0,5,6,7,8],[5,0,6,7,8],[6,6,0,5,8],[7,7,5,0,8],[5,6,7,8,0]]

def GetCoodPortNumber():
	return CoordinatorPort

def GetNodePortNumber(node_name):
	return NodePortList[ord(node_name[0])-ord('A')]

def GetCoodDelay():
	return CoordinatorDelay

def GetNodeDelay(node_name):
	return NodeDelayList[ord(node_name[0]) - ord('A')]