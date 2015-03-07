import configure
import datetime
import socket, select, string, sys
import threading, time
import utils

exitFlag = 0
NUM_NODES = 2
OutConnectFlags = [False for i in range(NUM_NODES)] #coordinator acts as client
#InConnectFlags = [False for i in range(NUM_NODES)] #coordinator acts as server
MessageQueue = [[],[],[],[]] #coordinator acts as client, send msgs to A/B/C/D nodes
RequestPool= []
AckFlags = [True for i in range(NUM_NODES)]

def CreateClientSockets():
    s = []
    for si in xrange(NUM_NODES):
        st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        st.settimeout(2)
        s.append(st)
    return s

ClientSockets = CreateClientSockets()

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
        PORT = configure.GetCoodPortNumber()
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
                        content, source = msg[:-2], msg[-1]
                        self.processMsg(content, source)
                    #print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(configure.GetCoodDelay())+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    def processMsg(self, msg, source):
        print "haha! msg is: ", msg, "source is ", source
        global AckFlags
        if msg == "ack":
            #mark "I receive ack from source"
            AckFlags[ord(source[0])-ord('A')] = True
            print AckFlags
        else:
            self.cacheRequest(msg)

    def cacheRequest(self, request):
        global RequestPool
        global AckFlags
        print "old pool:", RequestPool
        RequestPool.append(request)
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
            if self.readyForNextRequest() and RequestPool:
                self.resetAckFlags()
                print "My client thread will broadcast this request: ", RequestPool[0]
                self.broadcast(RequestPool[0])
                RequestPool.pop(0)
                #if self.readyForNextRequest():
                    #RequestPool.pop(0)
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

    def broadcast(self, msg):
        for i in range(NUM_NODES):
            self.unicast(msg, i)

    def unicast(self, msg, dest_id):
        global ClientSockets
        print "sending msg to ", dest_id
        node_name = chr(ord('A') + dest_id)
        if not OutConnectFlags[dest_id]:
            ClientSockets[dest_id].connect(("localhost", configure.GetNodePortNumber(node_name)))
            OutConnectFlags[dest_id] = True
        ClientThread.addQueue(msg, utils.GenerateRandomDelay(configure.GetNodeDelay(node_name)), dest_id)

    @staticmethod
    def addQueue(messagestr,delaynum,dest):
        global MessageQueue
        MessageQueue[dest].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

class ChannelThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

        '''self.clientSockets = []
        for si in xrange(NUM_NODES):
            st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            st.settimeout(2)
            self.clientSockets.append(st)'''

    def run(self):
        self.update()

    def update(self):
        global MessageQueue
        global ClientSockets
        while 1:
            CurTime = datetime.datetime.now()
            for si in xrange(NUM_NODES):
                while MessageQueue[si] and MessageQueue[si][0][0] <= CurTime:
                    ClientSockets[si].send(MessageQueue[si][0][1])
                    MessageQueue[si].pop(0)
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
