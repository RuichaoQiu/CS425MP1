import configure
import datetime
import socket, select, string, sys
import threading, time
import utils

exitFlag = 0
NodeName = sys.argv[1][0]
ClientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ClientSocket.settimeout(2)
MessageQueue = []


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
                        print "I receive msg: ", msg , " from coordinator!"
                        self.processMsg(msg)
                        print "sending ack..."
                        ClientThread.sendMsgToCoordinator(configure.ACK_MSG)
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

class ClientThread (threading.Thread):
    outConnectFlag = False

    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global ClientSocket
        while 1:
            socket_list = [sys.stdin]
            read_sockets, write_sockets, error_sockets = select.select(socket_list , [], []) 
            for sock in read_sockets:
                request = sys.stdin.readline()
                request_info = request.split()
                cmd = request_info[0].lower()
                if IsCmdValid(cmd):
                    ClientThread.sendMsgToCoordinator(request)
                else:
                    print "Invalid command"
                    #TODO: print out help menu
                    break

    @staticmethod
    def sendMsgToCoordinator(msg):
        global ClientSocket
        if not ClientThread.outConnectFlag:
            ClientSocket.connect(("localhost", configure.GetCoodPortNumber()))
            ClientThread.outConnectFlag = True
        ClientThread.addQueue(ClientThread.signMsg(msg), utils.GenerateRandomDelay(configure.GetCoodDelay()))
        print "Sent {msg} to coordinator, system time is {time}".format(msg=msg, time=datetime.datetime.now().time().strftime("%H:%M:%S"))

    @staticmethod
    def signMsg(msg):
        signed_msg = msg.split()
        signed_msg.append(NodeName)
        return " ".join(signed_msg)

    @staticmethod
    def addQueue(messagestr,delaynum):
        print "add message ", messagestr, " to queue!"
        global MessageQueue
        MessageQueue.append([datetime.datetime.now()+datetime.timedelta(0,delaynum),messagestr])

class ChannelThread (threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        self.update()

    def update(self):
        global MessageQueue
        global ClientSocket
        while 1:
            CurTime = datetime.datetime.now()
            while MessageQueue and MessageQueue[0][0] <= CurTime:
                ClientSocket.send(MessageQueue[0][1])
                MessageQueue.pop(0)
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
