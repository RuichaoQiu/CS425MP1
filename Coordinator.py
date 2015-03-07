import threading
import time
import socket, select, string, sys
import configure
import random
import datetime
exitFlag = 0

NUM_NODES = 2
OutConnectFlags = [False for i in range(NUM_NODES)] #coordinator acts as client
InConnectFlags = [False for i in range(NUM_NODES)] #coordinator acts as server

MessageQueue = [[],[],[],[]] #coordinator acts as client, send msgs to A/B/C/D nodes

s = []
for si in xrange(NUM_NODES):
    st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    st.settimeout(2)
    s.append(st)

RequestPool= []
AckFlags = [True for i in range(NUM_NODES)]

class ServerThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
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
        global AckFlags
        if msg == "ack":
            #mark "I receive ack from source"
            AckFlags[ord(source[0])-ord('A')] = True
        else:
            self.cacheRequest(msg)

    def cacheRequest(self, request):
        global RequestPool
        global AckFlags
        print "old pool:", RequestPool
        RequestPool.append(request)
        print "new pool:", RequestPool


#central server send request to A/B/C/D/ client
'''def SendRequest(request, dest):
    global ConnectFlag
    global s
    print "I am sending request " + request + " to " + dest
    if not ConnectFlag[dest]:
        s[dest].connect(("localhost", 4005))
        ConnectFlag[source] = True
    AddQueue(request, GenerateRandomDelay(configure.GetDelay(source, CENTRAL_SERVER)), CENTRAL_SERVER)
    print "send " + request + " to central server!"'''
        

'''def send():
    global ConnectFlag
    global s
    while 1:
        socket_list = [sys.stdin]
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], []) 
        for sock in read_sockets:
            if sock == sys.stdin:
                print "receive request from stdin!"
                request = sys.stdin.readline()
                if not ConnectFlag[0]:
                    s[CENTRAL_SERVER].connect(("localhost", 4005))
                    ConnectFlag[0] = True
                AddQueue(request, GenerateRandomDelay(configure.GetDelay(0, CENTRAL_SERVER)), CENTRAL_SERVER)
                print "send " + request + " to central server!"
            #l = msg.split()
            #x = ord(sys.argv[1][0])-ord('A')
            #y = ord(l[-1])-ord('A')
            #if not ConnectFlag[x][y]:
            #    s[y].connect(("localhost", configure.GetPortNumber(l[-1])))
            #    ConnectFlag[x][y] = True
            #l = l[1:len(l)-1]
            #l.append(sys.argv[1])
            #tmpstr = " ".join(l)
            #AddQueue(tmpstr,GenerateRandomDelay(configure.GetDelay(x,y)),y)

            #print "send msg "
            #print "Sent "+ " ".join(l[:len(l)-1])+" to "+chr(y+ord('A'))+", system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
   '''

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
                if self.readyForNextRequest():
                    RequestPool.pop(0)
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
        print "sending msg to ", dest_id
        node_name = chr(ord('A') + dest_id)
        if not OutConnectFlags[dest_id]:
            s[dest_id].connect(("localhost", configure.GetNodePortNumber(node_name)))
            OutConnectFlags[dest_id] = True
            AddQueue(msg, GenerateRandomDelay(configure.GetNodeDelay(node_name)), dest_id)


def GenerateRandomDelay(x):
    if x == 0:
        return 0
    return random.randint(1,x)

def AddQueue(messagestr,delaynum,dest):
    global MessageQueue
    MessageQueue[dest].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

class ChannelThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        RunChannel()

def RunChannel():
    global MessageQueue
    global s
    while 1:
        CurTime = datetime.datetime.now()
        for si in xrange(NUM_NODES):
            while MessageQueue[si] and MessageQueue[si][0][0] <= CurTime:
                s[si].send(MessageQueue[si][0][1])
                MessageQueue[si].pop(0)
        time.sleep(0.1)

thread1 = ServerThread(1, "Thread-1")
thread2 = ClientThread(2, "Thread-2")
thread3 = ChannelThread(3, "Thread-3")

thread1.start()
thread2.start()
thread3.start()
