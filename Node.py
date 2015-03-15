import datetime
import json
import socket, select, string, sys
import threading, time
import yaml

import configure
import message
import utils

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

        msg_decoded = yaml.load(msg)
        if msg_decoded['sender'] == NUM_NODES:              # 1: receive from coordinator
            #print "receive msg from coordinator"
            if msg_decoded['type'] == configure.ACK_MSG:       # 1.1: receive ack 
                ClientThread.clientSideOutput("")
            elif msg_decoded['type'] == "request":         # 1.2: receive broadcast request
                print "receive request from coordinator!"
                if self.executeRequest(msg_decoded):
                    if msg_decoded['cmd'] == "get": #read, return value
                        print "client output stage..."
                        key = msg_decoded['key']
                        if msg_decoded['original_sender'] == NodeID:
                            print "on going..."
                            ClientThread.clientSideOutput(self.kvStore[key]['value'])
                print "sending ack to coordinator!"
                ack_msg = message.Message("ack")
                ack_msg.signName(NodeID)
                json_str = json.dumps(ack_msg, cls=message.MessageEncoder)
                ClientThread.sendMsg(json_str, NUM_NODES)
            else:                                   # 1.3: receive read result
                key = msg_decoded['key']
                ClientThread.clientSideOutput(self.kvStore[key]['value'])
        else:                                   # 2: receive from peer nodes
            #print "receive msg from peers!"
            global RequestQueue
            sender_peer = msg_decoded['sender']
            if msg_decoded['type'] == configure.ACK_MSG:       # 2.1: receive ack 
                #print "let's see request queue", len(RequestQueue), RequestQueue[0], RequestQueue[0].model
                if RequestQueue and RequestQueue[0].model != 4:
                    ClientThread.clientSideOutput("")
                else:
                    global AckCnt
                    AckCnt += 1
                    #print "AckCnt: ", AckCnt
                    if AckCnt == 2:
                        ClientThread.clientSideOutput("")
                        AckCnt = 0
            elif msg_decoded['type'] == "request":           # 2.2: receive peer request
                self.executeRequest(msg_decoded)
                if msg_decoded['cmd'] == 'get':
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
                if RequestQueue and RequestQueue[0].model != 4:     
                    ClientThread.clientSideOutput(msg_decoded['value'])
                else:
                    ValueFromDiffNodes.append([msg_decoded['value'], msg_decoded['timestamp']])
                    #print ValueFromDiffNodes
                    if len(ValueFromDiffNodes) == 2:
                        if utils.TimestampCmp(ValueFromDiffNodes[0][1], ValueFromDiffNodes[1][1]):
                            latest_value = ValueFromDiffNodes[0][0]
                        else:
                            latest_value = ValueFromDiffNodes[1][0]
                        ClientThread.clientSideOutput(latest_value)
                        ValueFromDiffNodes[:] = []

    #msg is dict decoded from json string
    def executeRequest(self, msg):
        key = msg['key']
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

                # utility tool: show-all
                if cmdline_input.strip() == "show-all":
                    #print "going to tell server to print out all <key,value> pairs..."
                    ClientThread.sendMsg("show-all", NodeID)
                    break

                # utility tool: search key
                if cmdline_input.strip()[0] == "search":
                    for i in xrange(NUM_NODES):
                        ClientThread.sendMsg(cmdline_input.strip(), i)

                # replica operation: insert/delete/update/get...
                request = message.Request(cmdline_input)            
                if not IsCmdValid(request.cmd):
                    print "Invalid command!"
                    #TODO: print out help menu
                    break

                print "Received request {request} at {timestamp}".format(request=cmdline_input.strip(), timestamp=datetime.datetime.now().time().strftime("%H:%M:%S"))
                RequestQueue.append(request)
                print len(RequestQueue), ReadyForNextRequest

    @staticmethod
    #msg is json string
    def sendMsg(msg, dest_id):
        global ClientSockets
        if not ClientThread.outConnectFlags[dest_id]:
            #print "build connect with ", dest_id
            ClientSockets[dest_id].connect(("localhost", configure.PortList[dest_id]))
            ClientThread.outConnectFlags[dest_id] = True
        ClientThread.addQueue(msg, utils.GenerateRandomDelay(configure.DelayList[dest_id]), dest_id)

    @staticmethod
    def addQueue(messagestr,delaynum, dest_id):
        #print "add message ", messagestr, " to queue!"
        global MessageQueues
        MessageQueues[dest_id].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

    @staticmethod
    def clientSideOutput(option_value):
        #print "Current requests: ", RequestQueue[0]
        if RequestQueue:
            timestamp = datetime.datetime.now().time().strftime("%H:%M:%S")
            if RequestQueue[0].cmd == "get":
                print "client side: get({key}) = {value} at {time}".format(key=RequestQueue[0].key, value=option_value, time=timestamp)           
            elif RequestQueue[0].cmd == "insert":
                print "client side: Inserted key {key} value {value} at {time}".format(key=RequestQueue[0].key, value=RequestQueue[0].value, time=timestamp)
            elif RequestQueue[0].cmd == "delete":
                print "client side: Key {key} deleted at {time}".format(key=RequestQueue[0].key, time=timestamp)
            elif RequestQueue[0].cmd == "update":
                print "client side: Key {key} updated to {value} at {time}".format(key=RequestQueue[0].key, value=RequestQueue[0].value, time=timestamp)
            global ReadyForNextRequest
            ReadyForNextRequest = True
            #print"turn flag!:", ReadyForNextRequest
            RequestQueue.pop(0)

class RequestThread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global ReadyForNextRequest
        while 1:
            #print ReadyForNextRequest
            if ReadyForNextRequest:
                if RequestQueue:
                    ReadyForNextRequest = False
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
            elif model == 4:
                request.signName(NodeID)
                msg = json.dumps(request, cls=message.MessageEncoder)
                ClientThread.sendMsg(msg, NodeID)
                ClientThread.sendMsg(msg, utils.GenerateRandomPeer(NUM_NODES, NodeID))
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
