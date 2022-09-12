#ifndef __SERVER_STUB_H__
#define __SERVER_STUB_H__

#include <memory>

#include "ServerSocket.h"
#include "Messages.h"

class ServerStub {
private:
	std::unique_ptr<ServerSocket> socket;
	MapOp mop;
public:
	ServerStub();
	void Init(std::unique_ptr<ServerSocket> socket);
	// int isCustomer();
	TableMetadata ReceiveTableRequest();
	Identification ReceiveIdent();
	map<int, Peer> ReceiveReplacements();
	map<int, MetadataValue> ReceiveOtherData();
	int ReceiveServerYId();
	int RespondToRequest(Identification ident);
	int ReturnWriteAck(AckTableWrite write_ack);
	int ReturnTableRecord(ReturnReadRequest record);
	int ReturnReplacementAck(int success);
};

#endif // end of #ifndef __SERVER_STUB_H__
