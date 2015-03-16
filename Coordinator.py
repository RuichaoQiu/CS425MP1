import datetime
import socket, select, string, sys
import threading, time
import yaml
import json

import configure
import message
import utils

exitFlag = 0
NUM_NODES = 2
OutConnectFlags = [False for i in range(NUM_NODES)] #coordinator acts as client
MessageQueues = [[] for i in range(NUM_NODES)] #coordinator acts as client, send msgs to A/B/C/D nodes
RequestPool= [] #[request, sender]
BroadcastFlag = False
AckFlags = [True for i in range(NUM_NODES)]


ClientSockets = utils.CreateClientSockets(NUM_NODES)

class ServerThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        CONNECTION_LIST = []
        RECV_BUFFER = 4096 
        PORT = configure.PortList[NUM_NODES]
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
                        #print "receive msg: ", msg
                        self.processMsg(msg)
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    # msg is json string format
    def processMsg(self, msg):
        decoded_msg = yaml.load(msg)
        global AckFlags
        if decoded_msg['type'] in ["ack", "ValueResponse"]:
            AckFlags[decoded_msg['sender']] = True
            print AckFlags
        elif decoded_msg['type'] == "request":
            print "caching request from ",decoded_msg['sender']
            self.cacheRequest(msg, decoded_msg['sender'], decoded_msg)
    
    def cacheRequest(self, request, sender, msg):
        global RequestPool
        global AckFlags
        #print "old pool:", RequestPool
        RequestPool.append([request, sender])
        #print "new pool:", RequestPool

class ClientThread(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global AckFlags
        global RequestPool
        global BroadcastFlag 
        while 1:
            if RequestPool:
                #print BroadcastFlag
                if BroadcastFlag:
                    if self.readyForNextRequest():
                        #print "sending ack back to the issue client ", RequestPool[0][1]
                        ack_msg = message.Message("ack")
                        ack_msg.signName(NUM_NODES)
                        self.unicast(json.dumps(ack_msg, cls=message.MessageEncoder), RequestPool[0][1])
                        RequestPool.pop(0)
                        BroadcastFlag = False
                else:
                    print "My client thread will broadcast this request: ", RequestPool[0]
                    BroadcastFlag = True
                    self.resetAckFlags()
                    self.broadcast(message.signNameForJsonStr(RequestPool[0][0], NUM_NODES))
            time.sleep(0.1)

    def readyForNextRequest(self):
        global AckFlags
        for flag in AckFlags:
            if flag == False:
                return False
        return True

    def resetAckFlags(self):
        global AckFlags
        AckFlags = [False for i in range(NUM_NODES)]

    #request is json string format
    def broadcast(self, request):
        for i in range(NUM_NODES):
            self.unicast(request, i)

    #msg is json string format
    def unicast(self, msg, dest_id):
        global ClientSockets
        print "sending msg to {dest}".format(dest=dest_id)
        if not OutConnectFlags[dest_id]:
            ClientSockets[dest_id].connect(("localhost", configure.PortList[dest_id]))
            OutConnectFlags[dest_id] = True
        ClientThread.addQueue(msg, utils.GenerateRandomDelay(configure.DelayList[dest_id][NUM_NODES]), dest_id)

    @staticmethod
    def addQueue(messagestr,delaynum,dest):
        global MessageQueues
        #print "delay: ", delaynum
        MessageQueues[dest].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

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
            for si in xrange(NUM_NODES):
                while MessageQueues[si] and MessageQueues[si][0][0] <= CurTime:
                    #print "actually send to ", si
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
