import threading
import time
import socket, select, string, sys
import configure
import random
import datetime
exitFlag = 0

#ConnectFlag = False
OutConnectFlag = False #node acts as client, coordinator acts as server
InConnectFlag = False #node acts as server, coordinator acts client


client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.settimeout(2)

MessageQueue = []

Commands = ["delete", "get", "insert", "update"]

ACK_MSG = "ack"

class ServerThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.kvStore = dict()
    
    def run(self):
        CONNECTION_LIST = []
        RECV_BUFFER = 4096 
        PORT = configure.GetNodePortNumber(sys.argv[1])
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
                        print "i receive msg: ", msg , " from coordinator"
                        #content, source = msg[:-2], msg[-1]
                        self.processMsg(msg)
                        print "sending ack..."
                        thread2.sendMsgToCoordinator(ACK_MSG)
                    #print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(configure.GetCoodDelay())+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                    except:
                        CONNECTION_LIST.remove(read_socket)
                        read_socket.close()
                        continue     
        server_socket.close()

    def processMsg(self, msg):
        #msg is the request broadcasted by coordinator
        msg_info = msg.split()
        cmd = msg_info[0]
        if cmd == "insert":
            key, value = int(msg_info[1]), int(msg_info[2])
            self.kvStore[key] = value
            print "insert ", key, value
        elif cmd == "delete":
            pass
        elif cmd == "update":
            pass
        elif cmd == "get":
            pass

'''
def RunServer():
    CONNECTION_LIST = []
    RECV_BUFFER = 4096 
    PORT = configure.GetNodePortNumber(sys.argv[1])
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
                    print "Received "+" ".join(tmpl[:-1])+" from "+tmpl[-1]+", Max delay is "+str(configure.GetDelay(ord(tmpl[-1][0])-ord('A'),ord(sys.argv[1][0])-ord('A')))+"s, system time is "+ (datetime.datetime.now().time().strftime("%H:%M:%S"))
                except:
                    sock.close()
                    CONNECTION_LIST.remove(sock)
                    continue     
    server_socket.close()
'''

class ClientThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global OutConnectFlag
        global client_socket
        node_name = sys.argv[1][0]
        while 1:
            socket_list = [sys.stdin]
            read_sockets, write_sockets, error_sockets = select.select(socket_list , [], []) 
            for sock in read_sockets:
                request = sys.stdin.readline()
                request_info = request.split()
                cmd = request_info[0].lower()
                if self.isCmdValid(cmd):
                    self.sendMsgToCoordinator(request)
                else:
                    print "invalid cmd!"
                    break

    def isCmdValid(self, cmd):
        global Commands
        return cmd in Commands

    def sendMsgToCoordinator(self, msg):
        global client_socket
        global OutConnectFlag
        if not OutConnectFlag:
            client_socket.connect(("localhost", configure.GetCoodPortNumber()))
            OutConnectFlag = True
        msg_to_send = msg.split()
        msg_to_send.append(sys.argv[1][0])
        msg_to_send = " ".join(msg_to_send)
        AddQueue(msg_to_send, GenerateRandomDelay(configure.GetCoodDelay()))
        print "Sent %s to coordinator, system time is %s" % (msg_to_send, datetime.datetime.now().time().strftime("%H:%M:%S"))

def GenerateRandomDelay(x):
    if x == 0:
        return 0
    return random.randint(1,x)

def AddQueue(messagestr,delaynum):
    global MessageQueue
    MessageQueue.append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

class ChannelThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        RunChannel()

def RunChannel():
    global MessageQueue
    global client_socket
    while 1:
        CurTime = datetime.datetime.now()
        while MessageQueue and MessageQueue[0][0] <= CurTime:
            client_socket.send(MessageQueue[0][1])
            MessageQueue.pop(0)
        time.sleep(0.1)

thread1 = ServerThread(1, "Thread-1")
thread2 = ClientThread(2, "Thread-2")
thread3 = ChannelThread(3, "Thread-3")

thread1.start()
thread2.start()
thread3.start()
