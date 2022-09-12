#ifndef __CLIENT_STUB_H__
#define __CLIENT_STUB_H__

#include <string>

#include "ClientSocket.h"
#include "Messages.h"

class ClientStub {
private:
	ClientSocket socket;
public:
	ClientStub();
	int Init(std::string ip, int port);
	AckTableWrite WriteTable(TableMetadata request);
	ReturnReadRequest ReadTable(TableMetadata request);
	int SendIDMessage(Identification ident);
	Identification ReceiveConfirmation();
};


#endif // end of #ifndef __CLIENT_STUB_H__
