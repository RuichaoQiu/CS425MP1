PortList = [4001,4002,4003,4004]

DelayList = [[0,5,6,7],[5,0,6,7],[6,6,0,5],[7,7,5,0]]

def GetPortNumber(chr):
	return PortList[ord(chr[0])-ord('A')]

def GetDelay(x,y):
	return DelayList[x][y]
