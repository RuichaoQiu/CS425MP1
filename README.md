-------------------------------------------------
 CS425 MP1: Distributed Key-Value Storage System
-------------------------------------------------

Ruichao Qiu (rqiu3@illinois.edu)
Hao Luo		(haoluo3@illinois.edu)

I. Usage 
	A Simulating network delays
		* usage *
		Open a terminal and run "./DelayChannel.sh"
		Four terminals (A, B, C, D) are opened.
		Type "Send <message> <destination>", where <destination> could be any one among A, B, C and D

	B Key-Value Store and Consistency Models
		* usage *
		Open a terminal and run "./Setup.sh"
		Five terminals are opened. Four of them act as replicas (A,B,C,D) and the other one acts as coordinator.
		
		* input format *
			For terminal input:
				directly type request following * request format *
			For file input:
				save all requests to be executed in terminal <X> in <X.txt> file.
				type "start" in the terminal, indicating file input.
				type any key on keyboard to execute the next request.

		* request format *
		delete <Key>
			Delete the information associated with the specified key from all replicas
		get <Key> <Model>
			Return the value corresponding to the given key
		insert <Key> <Value> <Model>
			Create a new key with the specified value. If the key already exists, then update the value as specified
		update <Key> <Value> <Model> 
			Update the value for the specified key
		show-all
			Displays all the key-value pairs stored on the server performing this command
		search <Key>
			Displays the list of servers that store replicas of the key being searched

		* consistency model *
		1: Linearizability
		2: Sequential consistency
		3: Eventual consistency, W = 1, R = 1
		4: Eventual consistency, W = 2, R = 2

		NOTE: For eventual consistency (Model = 3 or 4 above), when W=k, the client is not sent a response for a
		write operation until the value has been written in at least k replica. For R=k, when a client performs
		read operation, values of the requested key at k replicas and the corresponding timestamps are
		examined, and the value with most recent timestamp is provided in the response to the client


II. File list
	DelayChannel.sh 				shell script for running "Simulating network delays (step 1)" functionality
	delayconfigure.py 				config file for "Simulating network delays (step 1)" functionality
	Server.py 						a skeleton implementation of replica node for "Simulating network delays (step 1)" functionality

	Setup.sh 						shell script for running "Key-Value Store and Consistency Models (step 2)" functionality
	configure.py 					config file for "Key-Value Store and Consistency Models (step 2)" functionality
	Coordinator.py 					implementation of central coordinator for totally ordered broadcast
	Node.py 						implementation of node replicas
	message.py 						implementation of different messages
	utils.py 						some utility function

	README.md                       this file


III. System requirement
	Python version >= 2.7
