import configure
import datetime
import socket, select, string, sys
import threading, time
import utils

exitFlag = 0
NUM_NODES = 2
NodeName = sys.argv[1][0]
NodeID = utils.NameToID(NodeName)

ClientSockets = utils.CreateClientSockets(NUM_NODES + 1)
MessageQueues = utils.CreateMessageQueues(NUM_NODES + 1)
RequestQueue = []
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
                        #print "sending ack..."
                        #ClientThread.sendMsg(configure.ACK_MSG, NUM_NODES)
                    #print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(configure.GetCoodDelay())+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    def processMsg(self, msg):
        print "receive msg: ", msg 
        source_id = int(msg[-1])
        msg_body = msg[:-2]
        msg_split = msg_body.split()
        print "source ", source_id, "msg_body: ", msg_body,
        if source_id == NUM_NODES:              # 1: receive from coordinator
            print "enter 1"
            if msg_body == configure.ACK_MSG:       # 1.1: receive ack 
                print "enter 1.1"
                ClientThread.clientSideOutput()
                CoorAck = False
            elif msg_split[0] in configure.Commands:         # 1.2: receive broadcast request
                self.executeRequest(msg_body)
                ClientThread.sendMsg(configure.ACK_MSG, NUM_NODES)
            else:                                   # 1.3: receive read result
                pass
        else:                                   # 2: receive from peer nodes
            if msg_body == configure.ACK_MSG:       # 2.1: receive ack 
                pass
            elif msg_split[0] in configure.Commands:           # 2.2: receive peer request
                pass
            else:                                   # 2.3: receive read result
                pass

    def executeRequest(self, msg): #msg: "cmd key (value) model", no source_id
        msg_info = msg.split()
        cmd, key = msg_info[0], int(msg_info[1])
        if cmd == "insert":
            self.kvStore[key] = int(msg_info[2])
            print "Inserted key {key} value {value}".format(key=key, value=self.kvStore[key])
        elif cmd == "delete":
            if self.validateKey(key):
                del self.kvStore[key]
                print "Key {key} deleted".format(key=key)
        elif cmd == "update":
            if self.validateKey(key):
                old_value = self.kvStore[key]
            else:
                old_value = "NULL"
            self.kvStore[key] = int(msg_info[2])
            print "Key {key} changed from {old_value} to {new_value}".format(key=key, old_value=old_value, new_value = self.kvStore[key])
        elif cmd == "get":
            if self.validateKey(key):
                print "get({key}) = {value}".format(key=key, value=self.kvStore[key])
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
                request = sys.stdin.readline()
                request_info = request.split()
                cmd = request_info[0].lower()

                if not IsCmdValid(cmd):
                    print "Invalid command!"
                    #TODO: print out help menu
                    break

                model = int(request_info[-1])
                #RequestQueue.append(utils.Request(cmd, model, False))

                if cmd == "get":
                    if model == 1:
                        ClientThread.sendMsg(request, NUM_NODES) #send request to coordinator
                    elif model == 2:
                        pass
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass
                elif cmd == "insert":
                    if model == 1:
                        RequestQueue.append(utils.Request(cmd, int(request_info[1]), int(request_info[2]), model))
                        ClientThread.sendMsg(request, NUM_NODES)
                    elif model == 2:
                        ClientThread.sendMsg(request, NUM_NODES)
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass
                elif cmd == "update":
                    if model == 1:
                        ClientThread.sendMsg(request, NUM_NODES)
                    elif model == 2:
                        ClientThread.sendMsg(request, NUM_NODES)
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass
                elif cmd == "delete":
                    if model == 1:
                        pass
                    elif model == 2:
                        pass
                    elif model == 3:
                        pass
                    elif model == 4:
                        pass

    def isTotalOrdered(self):
        return True

    @staticmethod
    def sendMsg(msg, dest_id):
        global ClientSockets
        if not ClientThread.outConnectFlags[dest_id]:
            print "build connect with ", dest_id
            ClientSockets[dest_id].connect(("localhost", configure.PortList[dest_id]))
            ClientThread.outConnectFlags[dest_id] = True
        ClientThread.addQueue(utils.SignMsg(msg, str(NodeID)), utils.GenerateRandomDelay(configure.DelayList[dest_id]), dest_id)
        print "Sent {msg} to {dest}, system time is {time}".format(dest=dest_id, msg=msg, time=datetime.datetime.now().time().strftime("%H:%M:%S"))

    @staticmethod
    def addQueue(messagestr,delaynum, dest_id):
        print "add message ", messagestr, " to queue!"
        global MessageQueues
        MessageQueues[dest_id].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

    @staticmethod
    def clientSideOutput():
        print "Current requests: ", RequestQueue
        if RequestQueue:
            if RequestQueue[0].Cmd == "get":
                pass
            elif RequestQueue[0].Cmd == "insert":
                print "Inserted key {key} value {value}".format(key=RequestQueue[0].Key, value=RequestQueue[0].Value)
            elif RequestQueue[0].Cmd == "delete":
                pass
            elif RequestQueue[0].Cmd == "update":
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
