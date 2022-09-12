#include "ClientThread.h"
#include "Messages.h"

#include <iostream>

using namespace std;

ClientThreadClass::ClientThreadClass() {}

void ClientThreadClass::
ThreadBody(std::string ip, int port, int id, int per_thread, int type) {
	customer_id = id;
	tables_per_thread = per_thread;
	request_type = type;

	if (!stub.Init(ip, port)) {
		cout << "Thread " << customer_id << " failed to connect" << endl;
		return;
	}
	// Part 2: Client should send an identification message after connecting to the server and before issuing the first request.
	Identification identification;
	identification.SetIdent(0);
	stub.SendIDMessage(identification);
	Identification identConfirm = stub.ReceiveConfirmation();
	if (!identConfirm.IsValid()) {

		exit(1);
	} 
	switch (request_type) {
			case 1: //write table
				{
					int start = customer_id * tables_per_thread;
					int end = start + tables_per_thread;
					// Randomly generated table metadata for now. 
					// In real implementation, we get them from the command line.
					int num_of_rows = (customer_id + 1) * 100;
					int num_of_cols = (customer_id + 1) * 5;
					int table_size = (customer_id + 1) * 1000;
					int num_of_nulls = (customer_id + 1) * 20;
					for (int i = start; i < end; i++) {
						TableMetadata metadata;
						AckTableWrite write_ack;
						int table_format = (i % 3) + 1; // 1, 2, 3
						metadata.SetTableMetadata(customer_id, i, num_of_rows, 
													num_of_cols, table_size, num_of_nulls,
													table_format, -1, -1, request_type);
						timer.Start();
						write_ack = stub.WriteTable(metadata);
						timer.EndAndMerge();
						if (!write_ack.IsValid()) {
							std::cout << "Invalid laptop " << i << endl;
							break;	
						}
					} 
					break;
				}

			case 2: //read record
				{
					TableMetadata read_request;
					ReturnReadRequest metadata;
					read_request.SetTableMetadata(customer_id, tables_per_thread, -1, 
													-1, -1, -1, -1, -1, -1, request_type);
					metadata = stub.ReadTable(read_request);
					if (!metadata.IsValid()) {
							std::cout << "Record not found" << tables_per_thread << std::endl;
							//break;	
					}
					else {
						metadata.Print();
					} 
					break;	
				}
			case 3: //check record
				{	
					TableMetadata read_request;
					ReturnReadRequest metadata;
					for (int i = 0; i< tables_per_thread ; i ++) {
						read_request.SetTableMetadata(customer_id, i, -1, 
													-1, -1, -1, -1, -1, -1, 2);
						metadata = stub.ReadTable(read_request);
						if (!metadata.IsValid()) {
								std::cout << "Record not found" << i << std::endl;
								//break;	
						} else {
							metadata.Print();
						} 
					}
					break;	
				}
			default:
				std::cout << "Undefined request type: "
					<< request_type << std::endl;

		}
}


ClientTimer ClientThreadClass::GetTimer() {
	return timer;	
}

