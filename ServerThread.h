#ifndef __SERVERTHREAD_H__
#define __SERVERTHREAD_H__

#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <map>
#include<cmath>
#include <list>
#include "Messages.h"
#include "ServerSocket.h"

using namespace std;



class Datalake {
private:
	void UpdateGlobalVariables(int r_table[4][10], map<int, int> b_pointers);
	int GetFirstDigit(int num);
	AckTableWrite WriteTableMetadata(TableMetadata request, int server_id, int thread_id,
										std::map<int, Peer> peers, int num_peers, int cs);
	ReturnReadRequest ReadTableMetadata(TableMetadata request, int server_id,
										std::map<int, Peer> peers, int num_peers, int cs);
	int CheckStoreLocation(int table_id, int server_id, int cs);
	int Hash(int table_id);
	int PrefixRouting(int node_id, int this_server_id);
	int SharedPrefixLength(int first_nid, int sec_nid);
	int Closer(int source_nid, int first_nid, int sec_nid);
	void RemoveFromRoutingTable(int node_id);
	int UpdatePeerBackpointer(int this_id, Peer peer, int exited);
	int AddBackpointer(int node_id, int server_id);
	void RemoveBackpointer(int node_id);
	int IsBackpointer(int node_id);

public:
	void PrintRoutingTable();
	void PrintBackpointers();
	int FindNewRoot(int server_id);
	map<int, Peer> FindNextBest(int server_id, map<int, Peer> peers);
	void AddSelfToRoutingTable(int server_id);
	int AddToRoutingTable(int node_id, int this_server_id);
	void DataLakeThread(std::unique_ptr<ServerSocket> socket, int server_id, int thread_id, 
		std::map<int, Peer> peers, int num_peers);
	void GracefulExitThread(int server_id, map<int, Peer> next_best_nodes, map<int, Peer> peers);
	void TransferTablesThread(int server_id, Peer newRoot);

};

#endif // end of #ifndef __SERVERTHREAD_H__

