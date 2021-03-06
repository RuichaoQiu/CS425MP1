import threading
import time
import socket, select, string, sys
import delayconfigure
import random
import datetime
exitFlag = 0

# Flag to check whether it's first time to connect the server
ConnectFlag = [[False for i in xrange(4)] for j in xrange(4)]
# Message Queue to implement delay channel.
MessageQueue = [[],[],[],[]]
s = []
for si in xrange(4):
    st = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    st.settimeout(2)
    s.append(st)

# Server side, receive message
class ServerThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        RunServer()

def RunServer():
    CONNECTION_LIST = []
    RECV_BUFFER = 4096 
    PORT = delayconfigure.GetPortNumber(sys.argv[1])
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", PORT))
    server_socket.listen(10)
    CONNECTION_LIST.append(server_socket)
    while 1:
        read_sockets,write_sockets,error_sockets = select.select(CONNECTION_LIST,[],[])
        for sock in read_sockets:
            if sock == server_socket:
                sockfd, addr = server_socket.accept()
                CONNECTION_LIST.append(sockfd)
            else:
                try:
                    data = sock.recv(RECV_BUFFER)
                    tmpl = data.split()
                    print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(delayconfigure.GetDelay(ord(tmpl[-1][0])-ord('A'),ord(sys.argv[1][0])-ord('A')))+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                except:
                    sock.close()
                    CONNECTION_LIST.remove(sock)
                    continue     
    server_socket.close()

# Client Side, send message
class ClientThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        RunClient()

def RunClient():
    global ConnectFlag
    global s
    print "Hello, my name is Server "+sys.argv[1][0]
    while 1:
        socket_list = [sys.stdin]
        read_sockets, write_sockets, error_sockets = select.select(socket_list , [], []) 
        for sock in read_sockets:
            msg = sys.stdin.readline()

            if msg.strip()[:5] == "start":
                print "Start reading file..."
                f = open(sys.argv[1][0]+".txt","r")
                msg = f.readline()
                while msg:
                    ch = raw_input()
                    print "Read from file: "+msg
                    ExeUpdate(msg[:])
                    msg = f.readline()
                print "Reading file Completed!"
            else:
                ExeUpdate(msg[:])

def ExeUpdate(msg):
    global ConnectFlag
    global s
    l = msg.split()
    x = ord(sys.argv[1][0])-ord('A')
    y = ord(l[-1])-ord('A')
    if not ConnectFlag[x][y]:
        s[y].connect(("localhost", delayconfigure.GetPortNumber(l[-1])))
        ConnectFlag[x][y] = True
    l = l[1:len(l)-1]
    l.append(sys.argv[1])
    tmpstr = " ".join(l)
    AddQueue(tmpstr,GenerateRandomDelay(delayconfigure.GetDelay(x,y)),y)
    print "Sent "+ " ".join(l[:len(l)-1])+" to "+chr(y+ord('A'))+", system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))

def GenerateRandomDelay(x):
    if x == 0:
        return 0
    return random.randint(1,x)

def AddQueue(messagestr,delaynum,dest):
    global MessageQueue
    MessageQueue[dest].append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

# Used for check it's the right time to send message
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
        for si in xrange(4):
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
