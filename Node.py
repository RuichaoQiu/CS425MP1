import datetime
import json
import socket, select, string, sys
import threading, time
import yaml

import configure
import message
import utils
import copy

exitFlag = 0
NUM_NODES = 2
NodeName = sys.argv[1][0]
NodeID = utils.NameToID(NodeName)

ClientSockets = utils.CreateClientSockets(NUM_NODES + 1)
MessageQueues = utils.CreateMessageQueues(NUM_NODES + 1)
RequestQueue = [] #Request object
ValueFromDiffNodes = []
AckCnt = 0 #used for model 4
ReadyForNextRequest = True
RequestCompleteTimestamp = 0
DelayTime = 0.0
SearchResult = [[] for si in xrange(NUM_NODES)]
ResultCount = 0

def IsCmdValid(cmd):
    return cmd in configure.Commands

class ServerThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.kvStore = dict()
    
    def run(self):
        self.update()

    def update(self):
        CONNECTION_LIST = []
        RECV_BUFFER = 4096 
        PORT = configure.PortList[NodeID]
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", PORT))
        server_socket.listen(10)
        CONNECTION_LIST.append(server_socket)
        while 1:
            read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])
            for read_socket in read_sockets:
                if read_socket == server_socket:
                    conn, addr = read_socket.accept()
                    CONNECTION_LIST.append(conn)
                else:
                    try:
                        msg = read_socket.recv(RECV_BUFFER)
                        self.processMsg(msg)
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    #msg is json string
    def processMsg(self, msg):
        #print "receive msg: ", msg 
        if msg == "show-all":
            #print "I am server. I am going to show - all!"
            self.showAll()
            return

        if msg[:6] == "search":
            strlist = msg.split()
            if int(strlist[1]) not in self.kvStore:
                tmpch = "#"
            else:
                tmpch = str(self.kvStore[int(strlist[1])]['value'])
            ClientThread.sendMsg("fetch "+strlist[1]+" "+tmpch+" "+str(NodeID),int(strlist[2]))
            return

        if msg[:5] == "fetch":
            strlist = msg.split()
            global SearchResult
            global ResultCount
            ResultCount += 1
            SearchResult[int(strlist[-1])] = [strlist[1],strlist[2]]
            if ResultCount == NUM_NODES:
                for i in xrange(NUM_NODES):
                    ch = chr(i+ord("A"))
                    print "Server %s: <%s, %s>" % (ch,SearchResult[i][0],SearchResult[i][1])
                ResultCount = 0
            return

        if msg[:6] == "repair":
            strlist = msg.split()
            if int(strlist[1]) not in self.kvStore:
                tmpch = "#"
                tmptime = "#"
            else:
                tmpch = str(self.kvStore[int(strlist[1])]['value'])
                tmptime = self.kvStore[int(strlist[1])]['timestamp']
            ClientThread.sendMsg("achieve "+strlist[1]+" "+tmpch+" "+tmptime,int(strlist[2]))
            return

        if msg[:7] == "achieve":
            strlist = msg.split()
            if strlist[2] != "#":
                if int(strlist[1]) not in self.kvStore:
                    self.kvStore[int(strlist[1])]['value'] = strlist[2]
                    self.kvStore[int(strlist[1])]['timestamp'] = strlist[3]
                elif utils.TimestampCmp(strlist[3], self.kvStore[int(strlist[1])]['timestamp']):
                    self.kvStore[int(strlist[1])]['value'] = strlist[2]
                    self.kvStore[int(strlist[1])]['timestamp'] = strlist[3]
            return


        msg_decoded = yaml.load(msg)
        # Finish inconsistency repair

        if msg_decoded['sender'] == NUM_NODES:              # 1: receive from coordinator
            #print "receive msg from coordinator"
            if msg_decoded['type'] == configure.ACK_MSG:       # 1.1: receive ack 
                ClientThread.clientSideOutput({})
            elif msg_decoded['type'] == "request":         # 1.2: receive broadcast request
                print "receive request from coordinator!"
                if self.executeRequest(msg_decoded):
                    if msg_decoded['cmd'] == "get": #read, return value
                        print "client output stage..."
                        key = msg_decoded['key']
                        if msg_decoded['original_sender'] == NodeID:
                            print "on going..."
                            ClientThread.clientSideOutput(self.kvStore[key])
                print "sending ack to coordinator!"
                ack_msg = message.Message("ack")
                ack_msg.signName(NodeID)
                json_str = json.dumps(ack_msg, cls=message.MessageEncoder)
                ClientThread.sendMsg(json_str, NUM_NODES)
            else:                                   # 1.3: receive read result
                key = msg_decoded['key']
                ClientThread.clientSideOutput(self.kvStore[key])
        else:                                   # 2: receive from peer nodes
            #print "receive msg from peers!"
            global RequestQueue
            sender_peer = msg_decoded['sender']
            if msg_decoded['type'] == configure.ACK_MSG:       # 2.1: receive ack 
                #print "let's see request queue", len(RequestQueue), RequestQueue[0], RequestQueue[0].model
                if RequestQueue and RequestQueue[0].model != 4:
                    ClientThread.clientSideOutput({})
                else:
                    global AckCnt
                    AckCnt += 1
                    #print "AckCnt: ", AckCnt
                    if AckCnt == 2:
                        ClientThread.clientSideOutput({})
                        AckCnt = 0
            elif msg_decoded['type'] == "request":           # 2.2: receive peer request
                self.executeRequest(msg_decoded)
                if msg_decoded['cmd'] == 'get':
                    print "receive get request from peer!"
                    value_timestamp = self.kvStore[msg_decoded['key']]
                    value_msg = message.ValueResponse(value_timestamp)
                    value_msg.signName(NodeID)
                    json_str = json.dumps(value_msg, cls=message.MessageEncoder)
                    ClientThread.sendMsg(json_str, sender_peer)
                else:
                    ack_msg = message.Message("ack")
                    ack_msg.signName(NodeID)
                    json_str = json.dumps(ack_msg, cls=message.MessageEncoder)
                    ClientThread.sendMsg(json_str, sender_peer)
            elif msg_decoded['type'] == 'ValueResponse':                               # 2.3: receive read result
                #print "receive value_timestamp from peer!"
                value_ts = {'timestamp':msg_decoded['timestamp'], 'value':msg_decoded['value']}
                #print "peer's:", value_ts
                if RequestQueue and RequestQueue[0].model != 4:     
                    ClientThread.clientSideOutput(value_ts)
                else:
                    global ValueFromDiffNodes
                    ValueFromDiffNodes.append(value_ts)
                    if len(ValueFromDiffNodes) == 2:
                        print "Candidate values are:"
                        for candidate in ValueFromDiffNodes:
                            print candidate['value'], candidate['timestamp']

                        if utils.TimestampCmp(ValueFromDiffNodes[0]['timestamp'], ValueFromDiffNodes[1]['timestamp']):
                            latest_pair = ValueFromDiffNodes[0]
                        else:
                            latest_pair = ValueFromDiffNodes[1]
                        #print "latest_pair:", latest_pair
                        ClientThread.clientSideOutput(latest_pair)
                        ValueFromDiffNodes[:] = []

    #msg is dict decoded from json string
    def executeRequest(self, msg):
        key = msg['key']
        print type(key)
        if msg['cmd'] == "insert":
            self.kvStore[key] = {'timestamp':msg['time'], 'value':int(msg['value'])}
            print "Inserted key {key} value {value}".format(key=key, value=self.kvStore[key]['value'])
        elif msg['cmd'] == "delete":
            if self.validateKey(key):
                del self.kvStore[key]
                print "Key {key} deleted".format(key=key)
            else:
                return False
        elif msg['cmd'] == "update":
            if self.validateKey(key):
                old_value = self.kvStore[key]['value']
            else:
                old_value = "NULL"
            self.kvStore[key] = {'timestamp':msg['time'], 'value':int(msg['value'])}
            print "Key {key} changed from {old_value} to {new_value}".format(key=key, old_value=old_value, new_value = self.kvStore[key]['value'])
        elif msg['cmd'] == "get":
            if self.validateKey(key):
                print "get({key}) = {value}".format(key=key, value=self.kvStore[key]['value'])
            else:
                return False
        return True

    def validateKey(self, key):
        if not key in self.kvStore:
            print "Key {key} does not exist!".format(key=key)
            if RequestQueue:
                RequestQueue.pop(0)
            return False
        return True

    def showAll(self):
        for key, value in self.kvStore.items():
            print "<{key}, {value}>".format(key=key, value=value['value'])


class ClientThread (threading.Thread):
    outConnectFlags = [False for i in xrange(NUM_NODES + 1)]

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global ClientSockets 
        while 1:
            socket_list = [sys.stdin]
            read_sockets, write_sockets, error_sockets = select.select(socket_list , [], []) 
            for sock in read_sockets:
                cmdline_input = sys.stdin.readline()

                if cmdline_input.strip()[:5] == "delay":
                    tmpstr = cmdline_input.strip()[6:]
                    Ltmpstr = tmpstr.split()
                    global DelayTime
                    DelayTime = float(Ltmpstr[0])
                    break

                # utility tool: show-all
                if cmdline_input.strip() == "show-all":
                    #print "going to tell server to print out all <key,value> pairs..."
                    ClientThread.sendMsg("show-all", NodeID)
                    break

                # utility tool: search key
                if cmdline_input.strip()[:6] == "search":
                    for i in xrange(NUM_NODES):
                        ClientThread.sendMsg(cmdline_input.strip()+" "+str(NodeID), i)
                    break

                # replica operation: insert/delete/update/get...
                request = message.Request(cmdline_input)            
                if not IsCmdValid(request.cmd):
                    print "Invalid command!"
                    #TODO: print out help menu
                    break

                print "Received request {request} at {timestamp}".format(request=cmdline_input.strip(), timestamp=datetime.datetime.now().time().strftime("%H:%M:%S"))
                RequestQueue.append(request)
                #print len(RequestQueue), ReadyForNextRequest

    @staticmethod
    #msg is json string
    def sendMsg(msg, dest_id):
        global ClientSockets
        if not ClientThread.outConnectFlags[dest_id]:
            #print "build connect with ", dest_id
            ClientSockets[dest_id].connect(("localhost", configure.PortList[dest_id]))
            ClientThread.outConnectFlags[dest_id] = True
        ClientThread.addQueue(msg, utils.GenerateRandomDelay(configure.DelayList[dest_id][NodeID]), dest_id)

    @staticmethod
    def addQueue(messagestr,delaynum, dest_id):
        #print "add message ", messagestr, " to queue!"
        global MessageQueues
        MessageQueues[dest_id].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

    @staticmethod
    def clientSideOutput(option_value_ts):
        #print "Current requests: ", RequestQueue[0]
        if RequestQueue:
            timestamp = datetime.datetime.now().time().strftime("%H:%M:%S")
            if RequestQueue[0].cmd == "get":
                if RequestQueue[0].model in [1,2]:
                    print "client side: get({key}) = {value} at {time}".format(key=RequestQueue[0].key, value=option_value_ts['value'], time=timestamp) 
                else: #evantual consistency models
                    print "client side: get({key}) = ({value}, {ts}) at {time}".format(key=RequestQueue[0].key, value=option_value_ts['value'], ts=option_value_ts['timestamp'], time=timestamp)            
            elif RequestQueue[0].cmd == "insert":
                print "client side: Inserted key {key} value {value} at {time}".format(key=RequestQueue[0].key, value=RequestQueue[0].value, time=timestamp)
            elif RequestQueue[0].cmd == "delete":
                print "client side: Key {key} deleted at {time}".format(key=RequestQueue[0].key, time=timestamp)
            elif RequestQueue[0].cmd == "update":
                print "client side: Key {key} updated to {value} at {time}".format(key=RequestQueue[0].key, value=RequestQueue[0].value, time=timestamp)
            global ReadyForNextRequest
            ReadyForNextRequest = True
            RequestQueue.pop(0)
            global RequestCompleteTimestamp
            RequestCompleteTimestamp = datetime.datetime.now()

    @staticmethod
    def InconsistencyRepair():
        for i in xrange(NUM_NODES):
            ClientThread.sendMsg("repair "+str(NodeID), i)


class RequestThread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global ReadyForNextRequest
        global RequestCompleteTimestamp
        global DelayTime
        while 1:
            CurTime = datetime.datetime.now()
            #print ReadyForNextRequest
            if ReadyForNextRequest and (DelayTime == 0.0 or RequestCompleteTimestamp+datetime.timedelta(0,DelayTime) <= CurTime):
                if RequestQueue:
                    ReadyForNextRequest = False
                    DelayTime = 0.0
                    #print "turn flag to false :("
                    self.handleRequest(RequestQueue[0])
            time.sleep(0.1)

    def handleRequest(self, request):
        model = request.model
        if request.cmd == "get":
            if model == 1:
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NUM_NODES) #send request to coordinator
            elif model == 2:
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NodeID)
            elif model == 3:
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NodeID)
                ClientThread.InconsistencyRepair()
            elif model == 4:
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NodeID)
                ClientThread.sendMsg(msg, utils.GenerateRandomPeer(NUM_NODES, NodeID))
                ClientThread.InconsistencyRepair()
        elif request.cmd == "insert" or request.cmd == "update":
            if model == 1 or model == 2:
                request.signTime()
                request.signName(NodeID)
                msg = json.dumps(request,cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NUM_NODES)
            elif model == 3:
                request.signTime()
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NodeID)
            elif model == 4:
                request.signTime()
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NodeID)
                ClientThread.sendMsg(msg, utils.GenerateRandomPeer(NUM_NODES, NodeID))
        elif request.cmd == "delete":
            request.signName(NodeID)
            msg = json.dumps(request, cls=message.MessageEncoder)
            ClientThread.sendMsg(msg, NUM_NODES)

class ChannelThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global MessageQueues
        global ClientSockets
        while 1:
            CurTime = datetime.datetime.now()
            for si in xrange(NUM_NODES+1):
                while MessageQueues[si] and MessageQueues[si][0][0] <= CurTime:
                    ClientSockets[si].send(MessageQueues[si][0][1])
                    MessageQueues[si].pop(0)
            time.sleep(0.1)

def main():
    threads = []
    threads.append(ServerThread(1, "ServerThread"))
    threads.append(ClientThread(2, "ClientThread"))
    threads.append(ChannelThread(3, "ChannelThread"))
    threads.append(RequestThread(4, "RequestThread"))

    for thread in threads:
        thread.start()

if __name__ == '__main__':
    main()
