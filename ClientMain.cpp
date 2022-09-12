#include <array>
#include <iostream> 
#include <iomanip> 
#include <thread> 
#include <vector> 

#include "ClientSocket.h"
#include "ClientThread.h"
#include "ClientTimer.h"

using namespace std;

//Arguments from command line: ./client [first_server_ip] [first_server_port] [num_of_threads] [tables_per_thread] [request_type]
int main(int argc, char *argv[]) {
	string ip;
	int port;
	int num_of_threads;
	int tables_per_thread;
	int request_type;
	ClientTimer timer;

	vector<shared_ptr<ClientThreadClass>> client_vector;
	vector<thread> thread_vector;
	
	if (argc < 5) {
		cout << "not enough arguments" << endl;
		cout << argv[0] << " [first_server_ip] [first_server_port] ";
		cout << "[num_of_threads] [tables_per_thread] [request_type]" << endl;
		return 0;
	}

	ip = argv[1];
	port = atoi(argv[2]);
	num_of_threads = atoi(argv[3]);
	tables_per_thread = atoi(argv[4]);
	request_type = atoi(argv[5]);

	timer.Start();
	for (int i = 0; i < num_of_threads; i++) {
		auto client_cls = std::shared_ptr<ClientThreadClass>(new ClientThreadClass());
		std::thread client_thread(&ClientThreadClass::ThreadBody, client_cls,
				ip, port, i, tables_per_thread, request_type);
		client_vector.push_back(std::move(client_cls));
		thread_vector.push_back(std::move(client_thread));
	}
	for (auto& th : thread_vector) {
		th.join();
	}
	timer.End();

	for (auto& cls : client_vector) {
		timer.Merge(cls->GetTimer());	
	}
	if (request_type == 1) {
		timer.PrintStats();
	}

	return 1;
}
