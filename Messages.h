
#ifndef __MESSAGES_H__
#define __MESSAGES_H__

#include <string>
#include <map>
#include <vector>

using namespace std;

struct MapOp {
	int	opcode;	// operation code: 1 - update value
	int arg1;	// customer_id to apply the operation
	int arg2;	// parameter for the operation
}; 

//This struct is the value we store as metadata in each server
struct MetadataValue {
	int num_of_rows;    
    int num_of_cols;    
    int table_size;     
    int num_of_nulls;   
    int table_format;     
};
//To handle the information about all the peers received from the command line arguments
struct Peer {
	int peer_id;
	std::string peer_ip;
	int peer_port;
};
//we don't need a separate struct. We can directly initialize an array at serverthread globally.
 struct RoutingTable {
	int localId;  //we already have local id intiated on serverThread. Therefore, we don't need to pass local id here.
	int prefixTable[4][10]; // Routing table has 4 rows (4 digits), 10 columns (0-9 for each digit)
 };

// //we don't need a separate struct. We can directly initialize an array at serverthread globally.
struct Backpointers {
	int localId;  //we already have local id intiated on serverThread. Therefore, we don't need to pass local id here.
	map<int, int> nodePointers;//for each server ID stored, 1 (true) or 0 (false) if it points to this node
 };

class TableMetadata {
private:
	int customer_id;	 // thread making the request
	int table_id;        // unique id of the table to be written in the data lake
	int num_of_rows;     // total number of rows in the table
	int num_of_cols;     // total number of columns in the table
	int table_size;      // table size in kb
	int num_of_nulls;    // total number of nulls in the table
	int table_format;    // csv = 1, pdf = 2, json = 3
	int num_of_hops;     // how far the write request reached from the first server.
	int first_server_id; // the server's id who is communicating with the client.
	int request_type;	 // 0: write request, 1: read this table, 2 : read all tables

public:
	TableMetadata();
	void operator = (const TableMetadata &request) {
		customer_id = request.customer_id;
		table_id = request.table_id;
		num_of_rows = request.num_of_rows;
		num_of_cols = request.num_of_cols;
		table_size = request.table_size;
		num_of_nulls = request.num_of_nulls;
		table_format = request.table_format;
		num_of_hops = request.num_of_hops;
		first_server_id = request.first_server_id;
		request_type = request.request_type;
	}

	void SetTableMetadata(int cid, int tid, int rows, int cols, 
							int size, int nulls, int format, 
							int hops, int first_server, int type);
	void SetNumOfHops(int hops);
	void SetFirstServerId(int first_server);

	int GetCustomerId();
	int GetTableId();
	int GetNumOfRows();
	int GetNumOfCols();
	int GetTableSize();
	int GetNumOfNulls();
	int GetTableFormat();
	int GetNumOfHops();
	int GetFirstServerId();
	int GetRequestType();
	int Size();
	void Marshal(char *buffer);
	void Unmarshal(char *buffer);
	bool IsValid();
	void Print();
};

class AckTableWrite {
private:
	int table_id;       // copied from writeTable
	int written_by;     // thread that handled this table
	int written_at;     // server at which the table is stored
	int remaining_hops; // how far the first server is

public:
	AckTableWrite();
	void operator = (const AckTableWrite &request) {
		table_id = request.table_id;
		written_by = request.written_by;
		written_at = request.written_at;
		remaining_hops = request.remaining_hops;
	}

	void SetAck(int tid, int by, int at, int hops);
	void SetTableId(int tid);
	void SetWrittenAt(int at);
	void SetWrittenBy(int by);
	void SetRemainingHops(int hops);
	int GetTableId();
	int GetWrittenBy();
	int GetWrittenAt();
	int GetRemainingHops();

	int Size();
	void Marshal(char *buffer);
	void Unmarshal(char *buffer);
	bool IsValid();
	void Print();
};

class ReturnReadRequest
{
private:
    int table_id;       // copied from request
    int num_of_rows;    // copied from stored data
    int num_of_cols;    // copied from stored data
    int table_size;     // copied from stored data
    int num_of_nulls;   // copied from stored data
    int table_format;   // copied from stored data
    int written_at;     // copied from current server id
    int first_server_id;// copied from request.
    int remaining_hops; // copied from request
public:
	ReturnReadRequest();
	void operator = (const ReturnReadRequest &request) {
		table_id = request.table_id;
		num_of_rows = request.num_of_rows;
		num_of_cols = request.num_of_cols;
		table_size = request.table_size;
		num_of_nulls = request.num_of_nulls;
		table_format = request.table_format;
		written_at = request.written_at;
		first_server_id = request.first_server_id;
		remaining_hops = request.remaining_hops;
	}
	void SetReturnReadRequest(int tid, int rows, int cols, 
								int size, int nulls, int format,
									int at, int first_server, int hops);
	void SetTableId(int tid);
	void setFirstServerId(int first_server);
	void SetRemainingHops(int hops);
	int GetTableId();
	int GetNumOfRows();
	int GetNumOfCols();
	int GetTableSize();
	int GetNumOfNulls();
	int GetTableFormat();
	int GetWrittenAt();
	int GetFirstServerId();
	int GetRemainingHops();
	int Size();
	void Marshal(char *buffer);
	void Unmarshal(char *buffer);
	bool IsValid();
	void Print();

};

class Identification {
private:
    int client_or_server;
public:
	Identification();
	void operator = (const Identification &identify) {
		client_or_server = identify.client_or_server;
	}
    void SetIdent(int cs);
    int GetIdent();

    int Size();

	void Marshal(char *buffer);
	void Unmarshal(char *buffer);

	bool IsValid();
	void Print();
};

class Protocol {
private:
	int last_index ; 		
	int committed_index ; 	
	int factory_id ;
	MapOp mapop;

public:
	Protocol();
	void operator = (const Protocol &protocol) {
		last_index = protocol.last_index;
		committed_index = protocol.committed_index;
		factory_id = protocol.factory_id;
		mapop = protocol.mapop;
	}
	void SetProtocol(int lst_index, int cmtd_index, int fctry_id, MapOp mpop);
	int GetLastIndex();
	int GetCommittedIndex();
	int GetFactoryId();
	MapOp GetMapOp();
	int Size();
	void Marshal(char *buffer);
	void Unmarshal(char *buffer);
	bool IsValid();
	void Print();
};

class IdMsg {
private:
	int rootId;
	int id;
	int level;

public:
	IdMsg();
	void SetIdMsg(int mroot, int mid, int mlevel);
	void SetRoot(int mroot);
	void SetId(int mid);
	void SetLevel(int mlevel);
	int GetRoot();
	int GetId();
	int GetLevel();

	int Size();

	void Marshal(char *buffer);
	void Unmarshal(char *buffer);

	bool IsValid();

	void Print();
	
};


#endif // #ifndef __MESSAGES_H__
