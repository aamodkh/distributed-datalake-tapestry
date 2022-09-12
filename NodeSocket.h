#ifndef __NODESOCKET_H__
#define __NODESOCKET_H__

#include <string>

#include "Socket.h"


class NodeSocket: public Socket {
public:
	NodeSocket() {}
	~NodeSocket() {}

	int Init(std::string ip, int port);

	void SetSocket();
};


#endif // end of #ifndef __NODESOCKET_H__
