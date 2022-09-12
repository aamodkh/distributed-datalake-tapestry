#include <iostream>
#include <cstring>
#include <string>
#include <arpa/inet.h>
#include "ServerStub.h"

using namespace std;

ServerStub::ServerStub() {
	mop.opcode = -1;
	mop.arg1 = -1;
	mop.arg2 = -1;
}
//ServerStub used by regular engineer thread in ServerThread
void ServerStub::Init(std::unique_ptr<ServerSocket> ssocket) {
	socket = std::move(ssocket);
}

TableMetadata ServerStub::ReceiveTableRequest() {
	// return customer requests
	char buffer[64];
	TableMetadata request;
	if (socket->Recv(buffer, request.Size(), 0)) {
		request.Unmarshal(buffer);
	}
	return request;	
}

int ServerStub::ReturnWriteAck(AckTableWrite info) {
	// return write information
	char buffer[64];
	info.Marshal(buffer);
	return socket->Send(buffer, info.Size(), 0);
}

int ServerStub::ReturnTableRecord(ReturnReadRequest record) { 
	// take a customer record and send the customer record
	char buffer[64];
	record.Marshal(buffer);

	return socket->Send(buffer, record.Size(), 0);
}

Identification ServerStub::ReceiveIdent() {
	// return customer requests
	char buffer[64];
	Identification ident;
	int recvStatus = socket->Recv(buffer, ident.Size(), 0);
	if (recvStatus) {
		ident.Unmarshal(buffer);
	} else if (recvStatus == 0) {
		ident.SetIdent(-2);
	}
	return ident;	
}

map<int, Peer> ServerStub::ReceiveReplacements() {
	char buffer[1156];
	map<int, Peer> next_best_nodes;
	int nodes_size = 40;
	int server_id;
	int level;
	Peer nextPeer;
	int ip_size = 16;
	int size = sizeof(nodes_size) + sizeof(server_id) +
		nodes_size*(sizeof(level)+sizeof(nextPeer.peer_id)+
		sizeof(nextPeer.peer_port) + ip_size); 
	
	int net_nodes_size;
	int net_server_id;
	int net_level;
	int net_peer_id;
	string net_peer_ip;
	int net_peer_ip_1;
	int net_peer_ip_2;
	int net_peer_ip_3;
	int net_peer_ip_4;
	int net_peer_port;

	int offset = 0;
	if (socket->Recv(buffer, size, 0)) {
		memcpy(&net_nodes_size, buffer + offset, sizeof(net_nodes_size));
		offset += sizeof(net_nodes_size);
		memcpy(&net_server_id, buffer + offset, sizeof(net_server_id));
		offset += sizeof(net_server_id);
		nodes_size = ntohl(net_nodes_size);
		server_id = ntohl(net_server_id);
		for (int i = 0; i < nodes_size; i++) {
			memcpy(&net_level, buffer + offset, sizeof(net_level));
			offset += sizeof(net_level);
			memcpy(&net_peer_id, buffer + offset, sizeof(net_peer_id));
			offset += sizeof(net_peer_id);
			
			memcpy(&net_peer_port, buffer + offset, sizeof(net_peer_port));
			offset += sizeof(net_peer_port);
			
			memcpy(&net_peer_ip_1, buffer + offset, sizeof(net_peer_ip_1)); 
			offset += sizeof(net_peer_ip_1); 
			memcpy(&net_peer_ip_2, buffer + offset, sizeof(net_peer_ip_2)); 
			offset += sizeof(net_peer_ip_2); 
			memcpy(&net_peer_ip_3, buffer + offset, sizeof(net_peer_ip_3)); 
			offset += sizeof(net_peer_ip_3); 
			memcpy(&net_peer_ip_4, buffer + offset, sizeof(net_peer_ip_4)); 
			offset += sizeof(net_peer_ip_4); 
			

			level = ntohl(net_level);
			nextPeer.peer_id = ntohl(net_peer_id);
			nextPeer.peer_ip = to_string(ntohl(net_peer_ip_1)) + "." + to_string(ntohl(net_peer_ip_2))
				 + "." + to_string(ntohl(net_peer_ip_3)) + "." + to_string(ntohl(net_peer_ip_4));
			nextPeer.peer_port = ntohl(net_peer_port);
			if (nextPeer.peer_id == 10000) {
				continue;
			}
			next_best_nodes[level] = nextPeer;

		}
		Peer exitNode;
		exitNode.peer_id = server_id;
		next_best_nodes[41] = exitNode;
	}
	return next_best_nodes;
}

int ServerStub::ReceiveServerYId() {
	char buffer[64];
	int server_id;
	int net_server_id;
	int size = sizeof(server_id);

	int offset = 0;
	socket->Recv(buffer, size, 0);
	memcpy(&net_server_id, buffer + offset, sizeof(net_server_id));
	offset += sizeof(net_server_id);
	
	server_id = ntohl(net_server_id);
	return server_id;
}

map<int, MetadataValue> ServerStub::ReceiveOtherData() {
	char buffer[64];
	map<int, MetadataValue> adoptedData;
	int table_id;
	MetadataValue metadata;
	int size = sizeof(table_id)+sizeof(metadata.num_of_rows) + sizeof(metadata.num_of_cols) + 
		sizeof(metadata.table_size) + sizeof(metadata.num_of_nulls) + sizeof(metadata.table_format);

	int net_table_id;
	int net_num_rows;
	int net_num_cols;
	int net_table_size;
	int net_num_nulls;
	int net_format;

	int offset = 0;
	if (socket->Recv(buffer, size, 0)) {
		memcpy(&net_table_id, buffer + offset, sizeof(net_table_id));
		offset += sizeof(net_table_id);
		memcpy(&net_num_rows, buffer + offset, sizeof(net_num_rows));
		offset += sizeof(net_num_rows);
		memcpy(&net_num_cols, buffer + offset, sizeof(net_num_cols));
		offset += sizeof(net_num_cols);
		memcpy(&net_table_size, buffer + offset, sizeof(net_table_size));
		offset += sizeof(net_table_size);
		memcpy(&net_num_nulls, buffer + offset, sizeof(net_num_nulls));
		offset += sizeof(net_num_nulls);
		memcpy(&net_format, buffer + offset, sizeof(net_format));
		offset += sizeof(net_format);

		table_id = ntohl(net_table_id);
		metadata.num_of_rows = ntohl(net_num_rows);
		metadata.num_of_cols = ntohl(net_num_cols);
		metadata.table_size = ntohl(net_table_size);
		metadata.num_of_nulls = ntohl(net_num_nulls);
		metadata.table_format = ntohl(net_format);
		adoptedData[table_id] = metadata;
	} else {
		metadata.num_of_rows = -1;
		metadata.num_of_cols = -1;
		metadata.table_size = -1;
		metadata.num_of_nulls =-1;
		metadata.table_format =-1;
		adoptedData[-1] = metadata;
	}
	return adoptedData;
}


int ServerStub::ReturnReplacementAck(int success) {
	char buffer[64];
    int size;

    int net_success = htonl(success);
    memcpy(buffer, &net_success, sizeof(net_success));
    size = sizeof(success);
    if (socket->Send(buffer, size, 0)) {
        return 1;
    }
    return 0;
}

int ServerStub::RespondToRequest(Identification ident) {
	// return customer requests
	char buffer[64];
	ident.Marshal(buffer);
	return socket->Send(buffer, ident.Size(), 0);
}