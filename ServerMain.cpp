#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include <vector>
#include <string>
#include <list>
#include <signal.h>

#include "ServerSocket.h"
#include "ServerThread.h"

using namespace std;

int server_id;
map<int, Peer> peers;  

// Define the function to be called when ctrl-c (SIGINT) is sent to process
void signal_callback_handler(int signum) {
	Datalake datalake; 
	map<int, Peer> next_best_nodes;
//    cout << "Exiting gracefully, send data to other servers " << signum << endl;
   // find replacement nodes for peers' routing tables
   next_best_nodes = datalake.FindNextBest(server_id, peers);

   thread gracefulexit_thread(&Datalake::GracefulExitThread,
			&datalake, server_id, next_best_nodes, peers);
	gracefulexit_thread.join();

	// transfer data objects to new root
	int newRootId = datalake.FindNewRoot(server_id);
	if (newRootId != server_id) {
		Peer newRoot = peers[newRootId];
		thread transfertables_thread(&Datalake::TransferTablesThread,
				&datalake, server_id, newRoot);
		transfertables_thread.join();
	} else {
		cout << "You are the last one remaining!" << endl;
	}
   

   exit(signum);
}

int main(int argc, char *argv[]) {
	int port;
	int this_id;
	int num_peers;
	ServerSocket socket;
	Datalake datalake; 
	unique_ptr<ServerSocket> new_socket;
	vector<thread> thread_vector;

	if (argc < 7) {
		std::cout << "not enough arguments. Please provide at least one peer details" << std::endl;
		std::cout << argv[0] << "[port #] [unique ID] [# peers ] ( repeat [ID] [IP] [ port #])" << std::endl;
		return 0;
	}

	port = atoi(argv[1]);
	this_id = atoi(argv[2]);
	num_peers = atoi(argv[3]);

	server_id = this_id;

	if (argc != 4 + (3 * num_peers)) {
		std::cout << "Please provide 3 arguments for each peer" << std::endl;
		std::cout << argv[0] << " [port #] [unique ID] [# peers ] ( repeat [ID] [IP] [ port #])" << std::endl;
		return 0;
	}
	// add this server to the routing table first.
	datalake.AddSelfToRoutingTable(this_id);
	int arg_index = 4;
	for (int i = 0; i < num_peers; i++) {
		Peer temp_peer;
		temp_peer.peer_id = atoi(argv[arg_index++]);
		temp_peer.peer_ip = argv[arg_index++];
		temp_peer.peer_port = atoi(argv[arg_index++]);
		peers[temp_peer.peer_id] = temp_peer;

		datalake.AddToRoutingTable(temp_peer.peer_id, this_id);
	} 
	if (!socket.Init(port)) {
		cout << "Socket initialization failed" << endl;
		return 0;
	}
	

	// data lake threads
	int thread_id = 0;
	// Register signal and signal handler
   	signal(SIGINT, signal_callback_handler);
	while ((new_socket = socket.Accept())) {		
		thread datalake_thread(&Datalake::DataLakeThread, //datalakeThread class defined in ServerThread
				&datalake, move(new_socket), this_id, thread_id++, peers, num_peers);
		thread_vector.push_back(move(datalake_thread));
	}
	return 0;
}
