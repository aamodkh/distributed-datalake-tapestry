#include <iostream>
#include <string.h>
#include <arpa/inet.h>
#include "ClientStub.h"

using namespace std;

ClientStub::ClientStub() {}

int ClientStub::Init(std::string ip, int port) {
	return socket.Init(ip, port);	
}

AckTableWrite ClientStub::WriteTable(TableMetadata request) {
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


ReturnReadRequest ClientStub::ReadTable(TableMetadata request) {
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

int ClientStub::SendIDMessage(Identification ident) {
	char send_buffer[64];
	int size;
	ident.Marshal(send_buffer);
	size = ident.Size();
	if (socket.Send(send_buffer, size, 0)) {
		return 1;
	}
	return 0;
}

Identification ClientStub::ReceiveConfirmation() {
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