import configure
import datetime
import socket, select, string, sys
import threading, time
import utils

exitFlag = 0
NUM_NODES = 2
OutConnectFlags = [False for i in range(NUM_NODES)] #coordinator acts as client
MessageQueues = [[] for i in range(NUM_NODES)] #coordinator acts as client, send msgs to A/B/C/D nodes
RequestPool= []
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
                        print "receive msg: ", msg
                        content, source = msg[:-2], int(msg[-1])
                        print "msg is ", content, "source is ", source
                        self.processMsg(content, source)
                    #print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(configure.GetCoodDelay())+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    def parseMsg(self, msg):
        pass

    def processMsg(self, msg, source_id):
        print "haha! msg is: ", msg, "source is ", source_id
        global AckFlags
        if msg == "ack":
            #mark "I receive ack from source"
            AckFlags[source_id] = True
            print AckFlags
        else:
            self.cacheRequest(msg, source_id)

    def cacheRequest(self, request, source_id):
        global RequestPool
        global AckFlags
        print "old pool:", RequestPool
        RequestPool.append(utils.RequestInfo(request, source_id, False, False))
        print "new pool:", RequestPool

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
        while 1:
            if RequestPool:
                if RequestPool[0].Broadcast:
                    if self.readyForNextRequest():
                        RequestPool[0].ReceiveAck = True
                        print "sending ack back to the issue client ", RequestPool[0].Source
                        self.unicast(configure.ACK_MSG, RequestPool[0].Source)
                        RequestPool.pop(0)
                        #self.resetAckFlags()
                else:
                    print "My client thread will broadcast this request: ", RequestPool[0].RequestMsg
                    RequestPool[0].Broadcast = True
                    self.resetAckFlags()
                    self.broadcast(RequestPool[0])

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

    def broadcast(self, request):
        for i in range(NUM_NODES):
            self.unicast(request.RequestMsg, i)

    def unicast(self, msg, dest_id):
        global ClientSockets
        print "sending {msg} to {dest}".format(msg=msg, dest=dest_id)
        #node_name = chr(ord('A') + dest_id)
        if not OutConnectFlags[dest_id]:
            ClientSockets[dest_id].connect(("localhost", configure.PortList[dest_id]))
            OutConnectFlags[dest_id] = True
        ClientThread.addQueue(utils.SignMsg(msg, str(NUM_NODES)), utils.GenerateRandomDelay(configure.DelayList[dest_id]), dest_id)

    @staticmethod
    def addQueue(messagestr,delaynum,dest):
        global MessageQueues
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
