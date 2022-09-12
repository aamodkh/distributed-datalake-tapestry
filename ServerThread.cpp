#include <iostream>
#include <memory>
#include "ServerThread.h"
#include "ServerStub.h"
#include "NodeStub.h"
#include "Messages.h"
#include <vector>
#include <string>
#include <list>
using namespace std;
std::mutex mtx, mtx_routing, mtx_bckptr;

int routing_table[4][10];
map<int, int> back_pointers; // server_id: 0 or 1
int updatedBackpointers = 0;
map<int, MetadataValue>store_metadata;

int Datalake::
Hash(int table_id) {
	// simple hash
	return table_id % 10000; // target node we are searching for
}

int Datalake::GetFirstDigit(int num) {
	int rem;
	int rev = 0;
	if (num % 10 == 0) {
		return rev;
	} else {
		while(num != 0) {
			rem = num % 10;
			rev = rev * 10 + rem;
			num = num / 10;
		}
		return rev % 10;
	}
}

void Datalake::AddSelfToRoutingTable(int server_id) {
	int col;
	mtx_routing.lock();
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < 10; j++) {
			routing_table[i][j] = -1;
		}
	}
	for(int i = 0; i < 4; i ++) {
		col = server_id % (int)pow(10, 4-i);
		if (col < pow(10,3-i)) {
			col = 0;
		} else {
			col = GetFirstDigit(col);
		}
		routing_table[i][col] = server_id;
	}
	mtx_routing.unlock();
}

void Datalake::PrintRoutingTable() {
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < 10; j ++) {
			cout << routing_table[i][j] <<"\t";
		}
		cout<<endl;
	}
}

void Datalake::UpdateGlobalVariables(int r_table[4][10], map<int, int> b_pointers) {
	mtx_routing.lock();
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < 10; j++) {
			routing_table[i][j] = r_table[i][j];
		}
	}
	mtx_routing.unlock();
	mtx_bckptr.lock();
	back_pointers = b_pointers;
	mtx_bckptr.unlock();
}

int Datalake::
AddToRoutingTable(int node_id, int this_server_id) {
	// return success - 1 or 0
	int success = 0;
	int self_nid = this_server_id;
	int target_nid = node_id;
	int sharedPrefix = SharedPrefixLength(self_nid, target_nid);
	// first, get the value that is already in that slot
	int col = target_nid % (int)pow(10, 4-sharedPrefix);
	if (col < pow(10,3-sharedPrefix)) {
		col = 0;
	} else {
		col = GetFirstDigit(col);
	}
	mtx_routing.lock();
	int existingValue = routing_table[sharedPrefix][col];
	// cout<<"level: "<< sharedPrefix << " col: "<< col<< endl;
	if (existingValue == -1) {
		routing_table[sharedPrefix][col] = node_id;
		success = node_id;
	} else {
		int isCloser = Closer(self_nid, target_nid, existingValue);
		if (isCloser) {
			routing_table[sharedPrefix][col] = node_id;
			success = node_id;
		} else {
			success = existingValue;
		}
	}
	mtx_routing.unlock();
	return success;
}

void Datalake::
RemoveFromRoutingTable(int node_id) {
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < 10; j ++) {
			if (routing_table[i][j] == node_id) {
				routing_table[i][j] = -1;
			}
			
		}
	}
}

int Datalake::
PrefixRouting(int node_id, int this_server_id) {
	// return closest node to that node_id

	int self_nid = this_server_id; //4352
	int target_nid = node_id; //table hash eg. 7845 (sp=0)	//4375 (sp=2)
	int sharedPrefix = SharedPrefixLength(self_nid, target_nid);
	int col = target_nid % (int)pow(10, 4-sharedPrefix);
	if (col < pow(10,3-sharedPrefix)) {
		col = 0;
	} else {
		col = GetFirstDigit(col);
	}
	mtx_routing.lock();
	int value = routing_table[sharedPrefix][col];
	while (value == -1) {
		col ++;
		if (col == 10) {
			col = 0;
		}
		value = routing_table[sharedPrefix][col];	
	}
	mtx_routing.unlock();
	return value;
}

int Datalake::
FindNewRoot(int server_id) {
	// TODO: Check!
	// iterate backwards through routing table to find nearest neighbor
	int newRoot = -1;
	int col;
	int i = 3;
	mtx_routing.lock();
	while (i >= 0) {
		// similar logic to PrefixRouting, but finds next nearest
		col = server_id % (int)pow(10, 4-i);
		if (col < pow(10,3-i)) {
			col = 0;
		} else {
			//get i'th digit of server_id
			col = GetFirstDigit(col);
		}
		// col = GetFirstDigit(col); 
		newRoot = routing_table[i][col];
		int iteration = 0;
		while (newRoot == -1 || (iteration == 0 && newRoot == server_id)) {
			col ++;
			if (col == 10) {
				col = 0;
			}
			newRoot = routing_table[i][col];	
			iteration++;
		}
		if (newRoot == server_id) {
			i--;
		} else {
			break;
		}
	}
	mtx_routing.unlock();	
	return newRoot;
}

map<int, Peer> Datalake::
FindNextBest(int server_id, map<int, Peer> peers) {
	map<int, Peer> next_best_nodes;
	Peer peerinPeers;
	mtx_routing.lock();
	int count = 0;
	for (int i = 0; i < 4; i++) {
		for (int j = 0; j < 10; j++) {
			Peer nextPeer;
			int nextInRouting = routing_table[i][j];
			if (nextInRouting != -1 && nextInRouting != server_id) {
				peerinPeers = peers[nextInRouting];
				nextPeer.peer_id = peerinPeers.peer_id;
				nextPeer.peer_ip = peerinPeers.peer_ip;
				nextPeer.peer_port = peerinPeers.peer_port;
				
			} else {
				nextPeer.peer_id = 10000;
				nextPeer.peer_ip = "256.256.256.256";
				nextPeer.peer_port = 10000;
			}
			next_best_nodes[count] = nextPeer;
			count++;
		}
	}
	mtx_routing.unlock();
	return next_best_nodes;
}

int Datalake::
SharedPrefixLength(int first_nid, int sec_nid) {
	// one-indexed
	int numShared = 0;
	int temp1;
	int temp2;
	for (int i = 1; i < 4; i ++) { // we are storing IDs of length 4
		temp1 = first_nid/pow(10, 4 - i);
		temp2 = sec_nid/pow(10, 4 - i);

		if (temp1 == temp2) {
			numShared = i;
		} else {
			break;
		}

	}
	return numShared;
}

int Datalake::
Closer(int source_nid, int first_nid, int sec_nid) {
	// return 1 if first_nid is closer than sec_nid, 0 if not
	int first_diff = abs(source_nid - first_nid);
	int sec_diff = abs(source_nid - sec_nid);
	if (first_diff < sec_diff) {
		return 1;
	}
	return 0;
}

int Datalake::
UpdatePeerBackpointer(int this_id, Peer peer, int exited) {
	NodeStub next_stub;
	if (!next_stub.Init(peer.peer_ip, peer.peer_port)) {
		return 0;
	}
	Identification identification;
	int ident = 4;
	if (exited) {
		ident = 5;
	}
	identification.SetIdent(ident);
	next_stub.SendIDMessage(identification);
	Identification identConfirm = next_stub.ReceiveConfirmation();
	if (!identConfirm.IsValid()) {
		// set socket = -1
		next_stub.SetSocket();
		return 0;
	}
	
	next_stub.SendServerYId(this_id); // server Y -> server X

	if (!next_stub.PrimaryReceiveResponse()) {
		return 0;
	}

	updatedBackpointers = 1;
	return 1;
}

int Datalake::
AddBackpointer(int node_id, int server_id) {
	int success = 0;
	mtx_bckptr.lock();
	if (node_id != server_id) {
		back_pointers[node_id] = 1;
	}
	mtx_bckptr.unlock();
	success = 1;
	return success;
	//backPoints.nodePointers[node_id] = 1;
}

void Datalake::
RemoveBackpointer(int node_id) {
	mtx_bckptr.lock();
	back_pointers[node_id] = 0;
	mtx_bckptr.unlock();
}

int Datalake::
IsBackpointer(int node_id) {
	// return 1 if node_id is a backpointer
	mtx_bckptr.lock();
	int status = back_pointers[node_id];
	mtx_bckptr.unlock();
	return status;
} 

void Datalake::
PrintBackpointers() {
	for (auto it = back_pointers.begin(); it != back_pointers.end(); it++) {
		if (it->second == 1) {
			cout << it->first << "\t";
		}
	}
	cout << endl;
}

int Datalake::CheckStoreLocation(int table_id, int server_id, int cs) {
	int hash_of_tid = Hash(table_id); 
	int target_server = PrefixRouting(hash_of_tid, server_id); 
	return target_server;
}

AckTableWrite Datalake::
WriteTableMetadata(TableMetadata request, int server_id,
			int thread_id, std::map<int, Peer> peers, int num_peers, int cs) {
	AckTableWrite write_ack;
	int key = request.GetTableId();
	MetadataValue value;
	value.num_of_rows = request.GetNumOfRows();
	value.num_of_cols = request.GetNumOfCols();
	value.table_size = request.GetTableSize();
	value.num_of_nulls = request.GetNumOfNulls();
	value.table_format = request.GetTableFormat();
	
	int store_location = CheckStoreLocation(key, server_id, cs);
	int current_remaining_hops;
	current_remaining_hops = request.GetNumOfHops();
	if (store_location == server_id) {
		mtx.lock();
		store_metadata[key] = value;
		mtx.unlock();
		write_ack.SetAck(key, thread_id, server_id, current_remaining_hops); 			
	} else {
		request.SetNumOfHops(current_remaining_hops + 1);
		Peer next_peer;
		next_peer = peers[store_location];
		string next_server_ip = next_peer.peer_ip;
		int next_server_port = next_peer.peer_port;
		NodeStub next_stub;
		if (!next_stub.Init(next_server_ip, next_server_port)) {
			write_ack.SetTableId(-1);
			return write_ack;
		}
		Identification identification;
		identification.SetIdent(1);
		next_stub.SendIDMessage(identification);
		Identification identConfirm = next_stub.ReceiveConfirmation();
		if (!identConfirm.IsValid()) {
			write_ack.SetTableId(-1);
			return write_ack;
		}
		write_ack = next_stub.ForwardWriteRequest(request);
		write_ack.SetRemainingHops(current_remaining_hops);
	}
	return write_ack;
}

ReturnReadRequest Datalake::
ReadTableMetadata(TableMetadata request, int server_id, 
					std::map<int, Peer> peers, int num_peers, int cs) {
	ReturnReadRequest record;
	int key = request.GetTableId();
	MetadataValue value;

	int store_location = CheckStoreLocation(key, server_id, cs);
	int current_remaining_hops;
	current_remaining_hops = request.GetNumOfHops();
	if (store_location == server_id) {
		mtx.lock();
		if (store_metadata.find(key) == store_metadata.end()) {
			record.SetReturnReadRequest(-1, -1, -1, -1, -1, -1, -1, server_id, current_remaining_hops);
		} else {
			//found
			value = store_metadata.at(key);
			record.SetReturnReadRequest(key, value.num_of_rows, value.num_of_cols,
			 value.table_size, value.num_of_nulls, value.table_format, server_id, 
			 										server_id, current_remaining_hops);
		}
		mtx.unlock();	
	} else {
		request.SetNumOfHops(current_remaining_hops + 1);
		Peer next_peer;
		next_peer = peers[store_location];
		string next_server_ip = next_peer.peer_ip;
		int next_server_port = next_peer.peer_port;
		NodeStub next_stub;
		if (!next_stub.Init(next_server_ip, next_server_port)) {
			record.SetTableId(-1);
			return record;
		}
		Identification identification;
		identification.SetIdent(1);
		next_stub.SendIDMessage(identification);
		Identification identConfirm = next_stub.ReceiveConfirmation();
		if (!identConfirm.IsValid()) {
			record.SetTableId(-1);
			return record;
		}
		record = next_stub.ForwardReadRequest(request);
		record.SetRemainingHops(current_remaining_hops);
	}
	return record;
}

void Datalake::DataLakeThread(std::unique_ptr<ServerSocket> socket, int server_id, int thread_id, 
		std::map<int, Peer> peers, int num_peers) {

	int request_type;
	Identification identification;
	ServerStub stub;
	TableMetadata request;

	stub.Init(move(socket));

	identification = stub.ReceiveIdent();
	if (!identification.IsValid()) {
		cout << "ID Request is not valid" << endl;
	}
	if (!stub.RespondToRequest(identification)) {
		cout << "Failed to Respond to Request" << endl;
	}
	// 4 Identifications:
	// 0 -> client
	// 1 -> server for read write operation
	// 2 -> exiting server sending its routing table and data it had.
	// 3 -> server to store the exiting node's data
	int client_or_server = identification.GetIdent();
	// ---------------------------------------- 
	// Responding to the r/w requests
	// ---------------------------------------- 
	if (client_or_server == 0 || client_or_server == 1) {
		while (true) {
			request = stub.ReceiveTableRequest();
			if (!request.IsValid()) {
				break;	
			}
			if (client_or_server == 0) {
				request.SetNumOfHops(0);
				request.SetFirstServerId(server_id);
				if (thread_id == 0) {
					for (auto it = peers.begin(); it != peers.end(); it++) {
						if (!UpdatePeerBackpointer(server_id, it->second, 0)) {
							continue;
						}
					}
				}
			} 
			request_type = request.GetRequestType();

			switch (request_type) {
				case 1:
				{	 // write table
					AckTableWrite write_ack;
					write_ack = WriteTableMetadata(request, server_id, 
							thread_id, peers, num_peers, client_or_server);
					stub.ReturnWriteAck(write_ack);
					break;
				}
				case 2: // read record
				{
					ReturnReadRequest record;
					record = ReadTableMetadata(request, server_id, peers, 
											num_peers, client_or_server);
					stub.ReturnTableRecord(record);
					break;
				}
				default:
					cout << "Undefined request type: "
						<< request_type << endl;

			}
			if (client_or_server == 1) {
				break;
			} 
		}
	} else if (client_or_server==2) { // receive routing table and data from exiting server.
		int success = 0;
		map<int, Peer> replacements;
		int exitNode_id;

		replacements = stub.ReceiveReplacements();
		exitNode_id = replacements[41].peer_id;

		//remove backpointer
		RemoveBackpointer(exitNode_id);
		//update routing table
		RemoveFromRoutingTable(exitNode_id);
		for (auto it = replacements.begin(); it != replacements.end(); it++) {
			if (it->second.peer_id != exitNode_id) {
				int replaced = AddToRoutingTable(it->second.peer_id, server_id);
				if (replaced == it->first) {
					success = 1;
					UpdatePeerBackpointer(server_id, it->second, 1);
				}
			}
			
		}
		
		stub.ReturnReplacementAck(success);
	} else if (client_or_server == 3) {
		// chosen as the new root: receive data from exiting server. Need to integrate objects to its data
		int success = 0;
		while (true) {
			map<int, MetadataValue> adoptedData;
			adoptedData = stub.ReceiveOtherData();
			if (adoptedData.begin()->first == -1) {
				break;
			}
			// store new data
			mtx.lock();
			
			store_metadata[adoptedData.begin()->first] = adoptedData.begin()->second;
			mtx.unlock();
			success = 1;
			stub.ReturnReplacementAck(success);
		}
	} else if (client_or_server == 4) {
		// update backpointers
		int success = 0;
		int serverYId;

		serverYId = stub.ReceiveServerYId();
		success = AddBackpointer(serverYId, server_id);
		// PrintBackpointers();
		stub.ReturnReplacementAck(success);
		if (!updatedBackpointers) {
			for (auto it = peers.begin(); it != peers.end(); it++) {
				if (!UpdatePeerBackpointer(server_id, it->second, 0)) {
					continue;
				}
			}
		}
	} else if (client_or_server == 5) {
		// update backpointers
		int success = 0;
		int serverYId;

		serverYId = stub.ReceiveServerYId();
		success = AddBackpointer(serverYId, server_id);
		stub.ReturnReplacementAck(success);
	}
}


void Datalake::GracefulExitThread(int server_id, map<int, Peer> next_best_nodes, map<int, Peer> peers) {
	int numBackpointers = 0;
	for (auto it = back_pointers.begin(); it != back_pointers.end(); it++) {
		if (it->second == 1) {
			numBackpointers++;
		}
	}
	NodeStub stubs [numBackpointers];
	int i = 0;
	for (auto it = back_pointers.begin(); it != back_pointers.end(); it++) {
		Peer peerConnect;
		if (it->second == 0) {
			continue;
		}
		peerConnect = peers[it->first];
		if (!stubs[i].Init(peerConnect.peer_ip, peerConnect.peer_port)) {
			cout << "GracefulExitThread Failed to connect to: " << peerConnect.peer_id << "backpointer serverId: " << it->first << endl;
			break;
		}
		Identification identification;
		identification.SetIdent(2);
		if (!stubs[i].SendIDMessage(identification)) {
			stubs[i].SetSocket();
			cout << "GracefulExitThread Failed to send ID message 2 to: " << peerConnect.peer_id << endl;
			continue;
		}
		Identification identConfirm = stubs[i].ReceiveConfirmation();
		if (!identConfirm.IsValid()) {
			// set socket = -1
			stubs[i].SetSocket();
			cout << "GracefulExitThread Invalid ID COnfirmation from: " << peerConnect.peer_id << endl;
			continue;
		}
		// send map
		stubs[i].SendReplacements(server_id, next_best_nodes);
		stubs[i].PrimaryReceiveResponse();
		i++;
	}
	
}

void Datalake::TransferTablesThread(int server_id, Peer newRoot) {
	NodeStub stub;
	stub.Init(newRoot.peer_ip, newRoot.peer_port);
	Identification identification;
	identification.SetIdent(3);
	if (!stub.SendIDMessage(identification)) {
		stub.SetSocket();
	}
	Identification identConfirm = stub.ReceiveConfirmation();
	if (!identConfirm.IsValid()) {
		// set socket = -1
		stub.SetSocket();
	}
	for (auto it = store_metadata.begin(); it != store_metadata.end(); it++) {
		// sending tables one-by-one
		stub.SendTableData(it->first, it->second);
		stub.PrimaryReceiveResponse();
	}
}
