#include <iostream>
#include <cstring>
#include <bits/stdc++.h>
#include <arpa/inet.h>
#include "NodeStub.h"
#include "Messages.h"

using namespace std;

NodeStub::NodeStub() {}

int NodeStub::Init(std::string ip, int port) {
	return socket.Init(ip, port);
}

void NodeStub::SetId(int server_id) {
    connected_server = server_id;
}

int NodeStub::GetId() {
	return connected_server;
}

AckTableWrite NodeStub::ForwardWriteRequest(TableMetadata request) {
	AckTableWrite write_ack;
	char send_buffer[64];
	char recv_buffer[64];
	int size;
	request.Marshal(send_buffer);
	size = request.Size();
	if (socket.Send(send_buffer, size, 0)) {
		size = write_ack.Size();
		int recvStatus = socket.Recv(recv_buffer, size, 0);
		if (recvStatus) {
			write_ack.Unmarshal(recv_buffer);
		} else if (recvStatus == 0) {
			write_ack.SetTableId(-2);
		}
	} else {
		write_ack.SetTableId(-2);
	}
	return write_ack;
}

ReturnReadRequest NodeStub::ForwardReadRequest(TableMetadata request) {
	ReturnReadRequest record;
	char send_buffer[64];
	char recv_buffer[64];
	int size;
	request.Marshal(send_buffer);
	size = request.Size();
	if (socket.Send(send_buffer, size, 0)) {
		size = record.Size();
		int recvStatus = socket.Recv(recv_buffer, size, 0);
		if (recvStatus) {
			record.Unmarshal(recv_buffer);
		} else if (recvStatus == 0) {
			record.SetTableId(-2);
		}
	} else {
		record.SetTableId(-2);
	}
	return record;
}

int NodeStub::PrimaryReceiveResponse() {
    char buffer[64];
    int success;
    int net_success;
	int recvStatus = socket.Recv(buffer, sizeof(success), 0);
    if (recvStatus) {
        memcpy(&net_success, buffer, sizeof(net_success));
        success = ntohl(net_success);
    } else {
		success = 0;
	}

    return success;
}

int NodeStub::SendIDMessage(Identification ident) {
	char send_buffer[64];
	int size;
	ident.Marshal(send_buffer);
	size = ident.Size();
	if (socket.Send(send_buffer, size, 0)) {
		return 1;
	}
	return 0;
}

Identification NodeStub::ReceiveConfirmation() {
	char buffer[64];
	Identification ident;
	int recvStatus = socket.Recv(buffer, ident.Size(), 0);
	if (recvStatus) {
		ident.Unmarshal(buffer);
	} else if (recvStatus == 0) {
		ident.SetIdent(-2);
	}
	return ident;	
}

int NodeStub::SendReplacements(int server_id, map<int, Peer> next_best_nodes) {
	//send number of nodes in next_best_nodes; server_id; and peer_id, peer_ip, peer_port of each peer
	char send_buffer[1156];
	int size;
	int offset = 0;
	int nodes_size = next_best_nodes.size();
	int net_nodes_size = htonl(nodes_size);
	int net_server_id = htonl(server_id);
	memcpy(send_buffer+offset, &net_nodes_size, sizeof(net_nodes_size));
	offset += sizeof(net_nodes_size);
	memcpy(send_buffer+offset, &net_server_id, sizeof(net_server_id));
	offset += sizeof(net_server_id);

	int level;
	int net_level;
	Peer net_peer;
	int net_peer_id;
	string net_peer_ip;
	int ip_size = 16;
	int net_peer_port;
	size = sizeof(nodes_size) + sizeof(server_id) + 
		nodes_size*(sizeof(level)+sizeof(net_peer.peer_id)+
		sizeof(net_peer.peer_port) + ip_size); 

	for (auto it = next_best_nodes.begin(); it != next_best_nodes.end(); it++) {
		level = it->first;
		net_level = htonl(level);
		net_peer = it->second;
		net_peer_id = htonl(net_peer.peer_id);
		net_peer_ip = net_peer.peer_ip;
		net_peer_port = htonl(net_peer.peer_port);

		memcpy(send_buffer + offset, &net_level, sizeof(net_level));
		offset += sizeof(net_level);
		memcpy(send_buffer + offset, &net_peer_id, sizeof(net_peer_id));
		offset += sizeof(net_peer_id);
		
		memcpy(send_buffer + offset, &net_peer_port, sizeof(net_peer_port));
		offset += sizeof(net_peer_port);

		vector<string> v;
		stringstream ipss(net_peer_ip);
		while (ipss.good()) {
			string substr;
			getline(ipss, substr, '.');
			v.push_back(move(substr));
		}
		int ip_part;
		for (size_t i = 0; i < v.size(); i++) {
			ip_part = htonl(atoi(v[i].c_str()));
			memcpy(send_buffer + offset, &ip_part, sizeof(ip_part));
			offset += sizeof(ip_part);
		}
	}
	size = sizeof(nodes_size) + sizeof(server_id) + 
		nodes_size*(sizeof(level)+sizeof(net_peer.peer_id)+
		sizeof(net_peer.peer_port) + ip_size); 
	if (socket.Send(send_buffer, size, 0)) {
		return 1;
	}
	return 0;
}

int NodeStub::SendServerYId(int server_id) {
	char send_buffer[64];
	int size;
	int offset = 0;
	int net_server_id = htonl(server_id);
	memcpy(send_buffer+offset, &net_server_id, sizeof(net_server_id));
	offset += sizeof(net_server_id);
	
	size = sizeof(server_id);
	if (socket.Send(send_buffer, size, 0)) {
		return 1;
	}
	return 0;
}

// int NodeStub::SendTableData(map<int, MetadataValue>store_metadata) {
int NodeStub::SendTableData(int table_id, MetadataValue table_metadata) {
	char send_buffer[64]; 
	int size;
	int offset = 0;

	int net_table_id;
	MetadataValue net_metadata;
	int net_num_rows;
	int net_num_cols;
	int net_table_size;
	int net_num_nulls;
	int net_format;
	net_table_id = htonl(table_id);
	net_metadata = table_metadata;
	net_num_rows = htonl(net_metadata.num_of_rows);
	net_num_cols = htonl(net_metadata.num_of_cols);
	net_table_size = htonl(net_metadata.table_size);
	net_num_nulls = htonl(net_metadata.num_of_nulls);
	net_format = htonl(net_metadata.table_format);

	memcpy(send_buffer + offset, &net_table_id, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(send_buffer + offset, &net_num_rows, sizeof(net_num_rows));
	offset += sizeof(net_num_rows);
	memcpy(send_buffer + offset, &net_num_cols, sizeof(net_num_cols));
	offset += sizeof(net_num_cols);
	memcpy(send_buffer + offset, &net_table_size, sizeof(net_table_size));
	offset += sizeof(net_table_size);
	memcpy(send_buffer + offset, &net_num_nulls, sizeof(net_num_nulls));
	offset += sizeof(net_num_nulls);
	memcpy(send_buffer + offset, &net_format, sizeof(net_format));
	offset += sizeof(net_format);

	size = sizeof(table_id)+sizeof(net_metadata.num_of_rows) + sizeof(net_metadata.num_of_cols) + 
		sizeof(net_metadata.table_size) + sizeof(net_metadata.num_of_nulls) + sizeof(net_metadata.table_format);
	if (socket.Send(send_buffer, size, 0)) {
		return 1;
	}
	return 0;
}


void NodeStub::SetSocket() {
	socket.SetSocket();
}


void NodeStub::SetLocalId(int server_id) {
	localId = server_id;
}

int NodeStub::GetLocalId() {
	return localId;
}