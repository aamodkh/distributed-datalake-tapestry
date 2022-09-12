#ifndef __CLIENT_THREAD_H__
#define __CLIENT_THREAD_H__

#include <chrono>
#include <ctime>
#include <string>

#include "ClientStub.h"
#include "ClientTimer.h"

class ClientThreadClass {
	int customer_id;
	int num_orders;
	int tables_per_thread;
	int request_type;
	ClientStub stub;
	ClientTimer timer;
public:
	ClientThreadClass();
	void ThreadBody(std::string ip, int port, int id, int per_thread, int type);

	void ReadThreadBody(string ip, int port, int orders);

	ClientTimer GetTimer();
};


#endif // end of #ifndef __CLIENT_THREAD_H__
