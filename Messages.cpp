#include <cstring>
#include <iostream>

#include <arpa/inet.h>
#include "Messages.h"

using namespace std;

//----------------------------------------
// TableMetadata
//----------------------------------------

TableMetadata::TableMetadata() {
	customer_id = -1;
	table_id = -1;
	num_of_rows = -1;
	num_of_cols = -1;
	table_size = -1;
	num_of_nulls = -1;
	table_format = -1;
	num_of_hops = -1;
	first_server_id = -1;
	request_type = -1;
}

void TableMetadata::SetTableMetadata(int cid, int tid, int rows, int cols, 
	int size, int nulls, int format, int hops, int first_server, int type) {
	customer_id = cid;
	table_id = tid;
	num_of_rows = rows;
	num_of_cols = cols;
	table_size = size;
	num_of_nulls = nulls;
	table_format = format;
	num_of_hops = hops;
	first_server_id = first_server;
	request_type = type;
}
void TableMetadata::SetNumOfHops(int hops) { num_of_hops = hops;}
void TableMetadata::SetFirstServerId(int first_server) {
	first_server_id = first_server;
}
int TableMetadata::GetCustomerId() { return customer_id;}
int TableMetadata::GetTableId() {return table_id;}
int TableMetadata::GetNumOfRows() {return num_of_rows;}
int TableMetadata::GetNumOfCols() {return num_of_cols;}
int TableMetadata::GetTableSize() {return table_size;}
int TableMetadata::GetNumOfNulls() {return num_of_nulls;}
int TableMetadata::GetTableFormat() {return table_format;}
int TableMetadata::GetNumOfHops() {return num_of_hops;}
int TableMetadata::GetFirstServerId() {return first_server_id;}
int TableMetadata::GetRequestType() { return request_type;}

int TableMetadata::Size() {
	return sizeof(customer_id) + sizeof(table_id) + sizeof(num_of_rows) 
			+ sizeof(num_of_cols) + sizeof(table_size)
			+ sizeof(num_of_nulls) + sizeof(table_format)
			+ sizeof(num_of_hops) + sizeof(first_server_id) + sizeof(request_type);
}

void TableMetadata::Marshal(char *buffer) {
	int net_customer_id = htonl(customer_id);
	int net_table_id = htonl(table_id);
	int net_num_of_rows = htonl(num_of_rows);
	int net_num_of_cols = htonl(num_of_cols);
	int net_table_size = htonl(table_size);
	int net_num_of_nulls = htonl(num_of_nulls);
	int net_table_format = htonl(table_format);
	int net_num_of_hops = htonl(num_of_hops);
	int net_first_server_id = htonl(first_server_id);
	int net_request_type = htonl(request_type);
	int offset =0;
	memcpy(buffer + offset, &net_customer_id, sizeof(net_customer_id));
	offset += sizeof(net_customer_id);
	memcpy(buffer + offset, &net_table_id, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(buffer + offset, &net_num_of_rows, sizeof(net_num_of_rows));
	offset += sizeof(net_num_of_rows);
	memcpy(buffer + offset, &net_num_of_cols, sizeof(net_num_of_cols));
	offset += sizeof(net_num_of_cols);
	memcpy(buffer + offset, &net_table_size, sizeof(net_table_size));
	offset += sizeof(net_table_size);
	memcpy(buffer + offset, &net_num_of_nulls, sizeof(net_num_of_nulls));
	offset += sizeof(net_num_of_nulls);
	memcpy(buffer + offset, &net_table_format, sizeof(net_table_format));
	offset += sizeof(net_table_format);
	memcpy(buffer + offset, &net_num_of_hops, sizeof(net_num_of_hops));
	offset += sizeof(net_num_of_hops);
	memcpy(buffer + offset, &net_first_server_id, sizeof(net_first_server_id));
	offset += sizeof(net_first_server_id);
	memcpy(buffer + offset, &net_request_type, sizeof(net_request_type));
}

void TableMetadata::Unmarshal(char *buffer) {
	int net_customer_id;
	int net_table_id;
	int net_num_of_rows;
	int net_num_of_cols;
	int net_table_size;
	int net_num_of_nulls;
	int net_table_format;
	int net_num_of_hops;
	int net_first_server_id;
	int net_request_type;
	int offset = 0;

	memcpy(&net_customer_id, buffer + offset, sizeof(net_customer_id));
	offset += sizeof(net_customer_id);
	memcpy(&net_table_id, buffer + offset, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(&net_num_of_rows, buffer + offset, sizeof(net_num_of_rows));
	offset += sizeof(net_num_of_rows);
	memcpy(&net_num_of_cols, buffer + offset, sizeof(net_num_of_cols));
	offset += sizeof(net_num_of_cols);
	memcpy(&net_table_size, buffer + offset, sizeof(net_table_size));
	offset += sizeof(net_table_size);
	memcpy(&net_num_of_nulls, buffer + offset, sizeof(net_num_of_nulls));
	offset += sizeof(net_num_of_nulls);
	memcpy(&net_table_format, buffer + offset, sizeof(net_table_format));
	offset += sizeof(net_table_format);
	memcpy(&net_num_of_hops, buffer + offset, sizeof(net_num_of_hops));
	offset += sizeof(net_num_of_hops);
	memcpy(&net_first_server_id, buffer + offset, sizeof(net_first_server_id));
	offset += sizeof(net_first_server_id);
	memcpy(&net_request_type, buffer + offset, sizeof(net_request_type));
	
	customer_id = ntohl(net_customer_id);
	table_id = ntohl(net_table_id);
	num_of_rows = ntohl(net_num_of_rows);
	num_of_cols = ntohl(net_num_of_cols);
	table_size = ntohl(net_table_size);
	num_of_nulls = ntohl(net_num_of_nulls);
	table_format = ntohl(net_table_format);
	num_of_hops = ntohl(net_num_of_hops);
	first_server_id = ntohl(net_first_server_id);
	request_type = ntohl(net_request_type);
}

bool TableMetadata::IsValid() {
	return (table_id != -1);
}

void TableMetadata::Print() {
	cout << "cid: " << customer_id << " ";
	cout << "Table id " << table_id << " ";
	cout << "rows " << num_of_rows << " ";
	cout << "cols " << num_of_cols << " ";
	cout << "size " << table_size << " ";
	cout << "nulls " << num_of_nulls << " ";
	cout << "format " << table_format << " ";
	cout << "hops " << num_of_hops << " ";
	cout << "first server " << first_server_id << " ";
	cout << "type " << request_type << endl;
}

//-----------------------------------------
// AckTableWrite class
//-----------------------------------------
AckTableWrite::AckTableWrite() {
	table_id = -1;
	written_by = -1;
	written_at = -1;
	remaining_hops = -1;
}

void AckTableWrite::SetAck(int tid, int by, int at, int hops) {
	table_id = tid;
	written_by = by;
	written_at = at;
	remaining_hops = hops;
}
void AckTableWrite::SetTableId(int tid) {table_id = tid;}
void AckTableWrite::SetWrittenAt(int at) { written_at = at;}
void AckTableWrite::SetWrittenBy(int by) { written_by = by;}
void AckTableWrite::SetRemainingHops(int hops) {remaining_hops = hops;}
int AckTableWrite::GetTableId() { return table_id;}
int AckTableWrite::GetWrittenBy() { return written_by;}
int AckTableWrite::GetWrittenAt() { return written_at;}
int AckTableWrite::GetRemainingHops() { return remaining_hops;}
int AckTableWrite::Size() {
	return sizeof(table_id) + sizeof(written_by) + sizeof(written_at) + sizeof(remaining_hops);
}

void AckTableWrite::Marshal(char *buffer) {
	int net_table_id = htonl(table_id);
	int net_written_by = htonl(written_by);
	int net_written_at = htonl(written_at);
	int net_remaining_hops = htonl(remaining_hops);
	int offset = 0;
	memcpy(buffer + offset, &net_table_id, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(buffer + offset, &net_written_by, sizeof(net_written_by));
	offset += sizeof(net_written_by);
	memcpy(buffer + offset, &net_written_at, sizeof(net_written_at));
	offset += sizeof(net_written_at);
	memcpy(buffer + offset, &net_remaining_hops, sizeof(net_remaining_hops));
	offset += sizeof(net_remaining_hops);	
}

void AckTableWrite::Unmarshal(char *buffer) {
	int net_table_id;
	int net_written_by;
	int net_written_at;
	int net_remaining_hops;
	int offset = 0;

	memcpy(&net_table_id, buffer + offset, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(&net_written_by, buffer + offset, sizeof(net_written_by));
	offset += sizeof(net_written_by);
	memcpy(&net_written_at, buffer + offset, sizeof(net_written_at));
	offset += sizeof(net_written_at);
	memcpy(&net_remaining_hops, buffer + offset, sizeof(net_remaining_hops));
	
	table_id = ntohl(net_table_id);
	written_by = ntohl(net_written_by);
	written_at = ntohl(net_written_at);
	remaining_hops = ntohl(net_remaining_hops);
}

bool AckTableWrite::IsValid() {
	return (table_id != -1);
}

void AckTableWrite::Print() {
	cout << "Table id " << table_id << " ";
	cout << "by " << written_by << " ";
	cout << "at " << written_at << " ";
	cout << "remaining hops " << remaining_hops << endl;
}



// ----------------------------------------
// ReturnReadRequest Class
// ----------------------------------------
ReturnReadRequest::ReturnReadRequest() {
	table_id = -1;
	num_of_rows = -1;
	num_of_cols = -1;
	table_size = -1;
	num_of_nulls = -1;
	table_format = -1;
	written_at = -1;
	first_server_id = -1;
	remaining_hops = -1;
}

void ReturnReadRequest::SetReturnReadRequest(int tid, int rows, int cols, 
	int size, int nulls, int format, int at, int first_server, int hops) {
	
	table_id = tid;
	num_of_rows = rows;
	num_of_cols = cols;
	table_size = size;
	num_of_nulls = nulls;
	table_format = format;
	written_at = at;
	first_server_id = first_server;
	remaining_hops = hops;
}
void ReturnReadRequest::SetTableId(int tid) {table_id = tid;}
void ReturnReadRequest::setFirstServerId(int first_server) {first_server_id = first_server;}
void ReturnReadRequest::SetRemainingHops(int hops) {remaining_hops = hops;}

int ReturnReadRequest::GetTableId() { return table_id;}
int ReturnReadRequest::GetNumOfRows() { return num_of_rows;}
int ReturnReadRequest::GetNumOfCols() { return num_of_cols;}
int ReturnReadRequest::GetTableSize() { return table_size;}
int ReturnReadRequest::GetNumOfNulls() { return num_of_nulls;}
int ReturnReadRequest::GetTableFormat() { return table_format;}
int ReturnReadRequest::GetWrittenAt() { return written_at;}
int ReturnReadRequest::GetFirstServerId() { return first_server_id;}
int ReturnReadRequest::GetRemainingHops() { return remaining_hops;}

int ReturnReadRequest::Size() {
	return sizeof(table_id) + sizeof(num_of_rows) + sizeof(num_of_cols) + sizeof(table_size)
	 + sizeof(num_of_nulls) + sizeof(table_format) + sizeof(written_at) + sizeof(first_server_id)
	 + sizeof(remaining_hops);
}

void ReturnReadRequest::Marshal(char *buffer) {
	int net_table_id = htonl(table_id);
	int net_num_of_rows = htonl(num_of_rows);
	int net_num_of_cols = htonl(num_of_cols);
	int net_table_size = htonl(table_size);
	int net_num_of_nulls = htonl(num_of_nulls);
	int net_table_format = htonl(table_format);
	int net_written_at = htonl(written_at);
	int net_first_server_id = htonl(first_server_id);
	int net_remaining_hops = htonl(remaining_hops);
	int offset =0;
	memcpy(buffer + offset, &net_table_id, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(buffer + offset, &net_num_of_rows, sizeof(net_num_of_rows));
	offset += sizeof(net_num_of_rows);
	memcpy(buffer + offset, &net_num_of_cols, sizeof(net_num_of_cols));
	offset += sizeof(net_num_of_cols);
	memcpy(buffer + offset, &net_table_size, sizeof(net_table_size));
	offset += sizeof(net_table_size);
	memcpy(buffer + offset, &net_num_of_nulls, sizeof(net_num_of_nulls));
	offset += sizeof(net_num_of_nulls);
	memcpy(buffer + offset, &net_table_format, sizeof(net_table_format));
	offset += sizeof(net_table_format);
	memcpy(buffer + offset, &net_written_at, sizeof(net_written_at));
	offset += sizeof(net_written_at);
	memcpy(buffer + offset, &net_first_server_id, sizeof(net_first_server_id));
	offset += sizeof(net_first_server_id);
	memcpy(buffer + offset, &net_remaining_hops, sizeof(net_remaining_hops));
}

void ReturnReadRequest::Unmarshal(char *buffer) {
	int net_table_id;
	int net_num_of_rows;
	int net_num_of_cols;
	int net_table_size;
	int net_num_of_nulls;
	int net_table_format;
	int net_written_at;
	int net_first_server_id;
	int net_remaining_hops;
	int offset = 0;

	memcpy(&net_table_id, buffer + offset, sizeof(net_table_id));
	offset += sizeof(net_table_id);
	memcpy(&net_num_of_rows, buffer + offset, sizeof(net_num_of_rows));
	offset += sizeof(net_num_of_rows);
	memcpy(&net_num_of_cols, buffer + offset, sizeof(net_num_of_cols));
	offset += sizeof(net_num_of_cols);
	memcpy(&net_table_size, buffer + offset, sizeof(net_table_size));
	offset += sizeof(net_table_size);
	memcpy(&net_num_of_nulls, buffer + offset, sizeof(net_num_of_nulls));
	offset += sizeof(net_num_of_nulls);
	memcpy(&net_table_format, buffer + offset, sizeof(net_table_format));
	offset += sizeof(net_table_format);
	memcpy(&net_written_at, buffer + offset, sizeof(net_written_at));
	offset += sizeof(net_written_at);
	memcpy(&net_first_server_id, buffer + offset, sizeof(net_first_server_id));
	offset += sizeof(net_first_server_id);
	memcpy(&net_remaining_hops, buffer + offset, sizeof(net_remaining_hops));
	
	table_id = ntohl(net_table_id);
	num_of_rows = ntohl(net_num_of_rows);
	num_of_cols = ntohl(net_num_of_cols);
	table_size = ntohl(net_table_size);
	num_of_nulls = ntohl(net_num_of_nulls);
	table_format = ntohl(net_table_format);
	written_at = ntohl(net_written_at);
	first_server_id = ntohl(net_first_server_id);
	remaining_hops = ntohl(net_remaining_hops);
}


bool ReturnReadRequest::IsValid() {
	return (table_id != -1);
}

void ReturnReadRequest::Print() {
	cout << "Table id " << table_id << " ";
	cout << "rows " << num_of_rows << " ";
	cout << "cols " << num_of_cols << " ";
	cout << "size " << table_size << " ";
	cout << "nulls " << num_of_nulls << " ";
	cout << "format " << table_format << " ";
	cout << "at " << written_at << " ";
	cout << "first server " << first_server_id << " ";
	cout << "remaining hops " << remaining_hops << endl;
}


// -----------------------------------
// Identification class
// -----------------------------------
Identification::Identification() {
	client_or_server = -1;
}

void Identification::SetIdent(int cs) {
	// if custOrPfa = 0, then customer. If custOrPfa = 1, then PFA
	client_or_server = cs;
}
int Identification::GetIdent() { return client_or_server; }

int Identification::Size() {
	return sizeof(client_or_server);
}

void Identification::Marshal(char *buffer) {
	int net_client_or_server = htonl(client_or_server);
	int offset = 0;

	memcpy(buffer + offset, &net_client_or_server, sizeof(net_client_or_server));
}

void Identification::Unmarshal(char *buffer) {
	int net_client_or_server;
	int offset = 0;

	memcpy(&net_client_or_server, buffer + offset, sizeof(net_client_or_server));
	client_or_server = ntohl(net_client_or_server);
}

bool Identification::IsValid() {
	return (client_or_server != -1);
}

void Identification::Print() {
	cout << "identity:" << client_or_server << endl;
}


// ---------------------------------------- 
// IdMsg class
// ---------------------------------------- 
IdMsg::IdMsg() {
	rootId = -1;
	id = -1;
	level = -1;
}

void IdMsg::SetIdMsg(int mroot, int mid, int mlevel) {
	rootId = mroot;
	id = mid;
	level = mlevel;
}

void IdMsg::SetRoot(int mroot) {
	rootId = mroot;
}

void IdMsg::SetId(int mid) {
	id = mid;
}

void IdMsg::SetLevel(int mlevel) {
	level = mlevel;
}

int IdMsg::GetRoot() { return rootId; }
int IdMsg::GetId() { return id; }
int IdMsg::GetLevel() { return level; }

int IdMsg::Size() {
	return sizeof(rootId) + sizeof(id) + sizeof(level);
}

void IdMsg::Marshal(char *buffer) {
	int net_root = htonl(rootId);
	int net_id = htonl(id);
	int net_level = htonl(level);
	int offset = 0;

	memcpy(buffer + offset, &net_root, sizeof(net_root));
	offset += sizeof(net_root);
	memcpy(buffer + offset, &net_id, sizeof(net_id));
	offset += sizeof(net_id);
	memcpy(buffer + offset, &net_level, sizeof(net_level));
}

void IdMsg::Unmarshal(char *buffer) {
	int net_root;
	int net_id;
	int net_level; 
	int offset = 0;
	
	memcpy(&net_root, buffer + offset, sizeof(net_root));
	offset += sizeof(net_root);
	memcpy(&net_id, buffer + offset, sizeof(net_id));
	offset += sizeof(net_id);
	memcpy(&net_level, buffer + offset, sizeof(net_level));

	rootId = ntohl(net_root);
	id = ntohl(net_id);
	level = ntohl(net_level);
}

bool IdMsg::IsValid() {
	return (rootId != -1);
}

void IdMsg::Print() {
	cout << "IdMsg rootId: " << rootId << " ";
	cout << "ID: " << id << " ";
	cout << "Level: " << level << endl;
}