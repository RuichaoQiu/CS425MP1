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
CoorAck = False

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
                    #print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(configure.GetCoodDelay())+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    #msg is json string
    def processMsg(self, msg):
        #print "receive msg: ", msg 
        msg_decoded = yaml.load(msg)
        if msg_decoded['sender'] == NUM_NODES:              # 1: receive from coordinator
            if msg_decoded['type'] == configure.ACK_MSG:       # 1.1: receive ack 
                ClientThread.clientSideOutput("")
                CoorAck = False
            elif msg_decoded['type'] == "request":         # 1.2: receive broadcast request
                self.executeRequest(msg_decoded)
                if msg_decoded['cmd'] == "get": #read, return value
                    key = msg_decoded['key']
                    if msg_decoded['original_sender'] == NodeID:
                        ClientThread.clientSideOutput(self.kvStore[key]['value'])
                ack_msg = message.Message("ack")
                ack_msg.signName(NodeID)
                json_str = json.dumps(ack_msg, cls=message.MessageEncoder)
                ClientThread.sendMsg(json_str, NUM_NODES)
            else:                                   # 1.3: receive read result
                pass
        else:                                   # 2: receive from peer nodes
            if msg_decoded['type'] == configure.ACK_MSG:       # 2.1: receive ack 
                pass
            elif msg_decoded['type'] == "request":           # 2.2: receive peer request
                pass
            else:                                   # 2.3: receive read result
                pass

    #msg is dict decoded from json string
    def executeRequest(self, msg): #msg: "cmd key (value) model", no source_id
        key = msg['key']
        if msg['cmd'] == "insert":
            self.kvStore[key] = {'timestamp':msg['time'], 'value':int(msg['value'])}
            print "Inserted key {key} value {value}".format(key=key, value=self.kvStore[key])
        elif msg['cmd'] == "delete":
            if self.validateKey(key):
                del self.kvStore[key]
                print "Key {key} deleted".format(key=key)
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
            #todo: call client thread to send value back to coordinator

    def validateKey(self, key):
        if not key in self.kvStore:
            print "Key {key} does not exist!".format(key=key)
            return False
        return True

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
                request = message.Request(sys.stdin.readline())
                
                if not IsCmdValid(request.cmd):
                    print "Invalid command!"
                    #TODO: print out help menu
                    break

                model = request.model
                RequestQueue.append(request)
                if request.cmd == "get":
                    if model == 1:
                        request.signName(NodeID)
                        msg = json.dumps(request, cls=message.MessageEncoder)
                        ClientThread.sendMsg(msg, NUM_NODES) #send request to coordinator
                    elif model == 2:
                        pass
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass
                elif request.cmd == "insert":
                    if model == 1 or model == 2:
                        request.signTime()
                        request.signName(NodeID)
                        msg = json.dumps(request,cls=message.MessageEncoder)
                        ClientThread.sendMsg(msg, NUM_NODES)
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass
                elif request.cmd == "update":
                    if model == 1 or model == 2:
                        request.signTime()
                        request.signName(NodeID)
                        msg = json.dumps(request,cls=message.MessageEncoder)
                        ClientThread.sendMsg(msg, NUM_NODES)
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass
                elif request.cmd == "delete":
                    if model == 1:
                        pass
                    elif model == 2:
                        pass
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass


    @staticmethod
    #msg is json string
    def sendMsg(msg, dest_id):
        global ClientSockets
        if not ClientThread.outConnectFlags[dest_id]:
            print "build connect with ", dest_id
            ClientSockets[dest_id].connect(("localhost", configure.PortList[dest_id]))
            ClientThread.outConnectFlags[dest_id] = True
        ClientThread.addQueue(msg, utils.GenerateRandomDelay(configure.DelayList[dest_id]), dest_id)
        #print "Sent {msg} to {dest}, system time is {time}".format(dest=dest_id, msg=msg, time=datetime.datetime.now().time().strftime("%H:%M:%S"))

    @staticmethod
    def addQueue(messagestr,delaynum, dest_id):
        #print "add message ", messagestr, " to queue!"
        global MessageQueues
        MessageQueues[dest_id].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

    @staticmethod
    def clientSideOutput(option_value):
        #print "Current requests: ", RequestQueue[0]
        if RequestQueue:
            if RequestQueue[0].cmd == "get":
                if RequestQueue[0].model in [1,2]:
                    #pass #TODO
                    print "client side: get({key}) = {value}".format(key=RequestQueue[0].key, value=option_value)
                else:
                    pass
            elif RequestQueue[0].cmd == "insert":
                print "client side: Inserted key {key} value {value}".format(key=RequestQueue[0].key, value=RequestQueue[0].value)
            elif RequestQueue[0].cmd == "delete":
                pass
            elif RequestQueue[0].cmd == "update":
                pass
            RequestQueue.pop(0)


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

    for thread in threads:
        thread.start()

if __name__ == '__main__':
    main()
