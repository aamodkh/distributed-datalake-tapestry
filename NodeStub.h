#ifndef __NODE_STUB_H__
#define __NODE_STUB_H__

#include <memory>
#include <string>

#include "NodeSocket.h"
#include "Messages.h"

class NodeStub {
private:
	NodeSocket socket;
    int localId;
    int connected_server;
    MapOp mop;
public:
	NodeStub();
    int Init(std::string ip, int port);
    void SetId(int server_id);
    int GetId();
    AckTableWrite ForwardWriteRequest(TableMetadata request);
    ReturnReadRequest ForwardReadRequest(TableMetadata request);
    int PrimaryReceiveResponse();
	int SendIDMessage(Identification ident);
    Identification ReceiveConfirmation();

    int SendReplacements(int server_id, map<int, Peer> next_best_nodes);
    int SendTableData(int table_id, MetadataValue table_metadata);
    int SendServerYId(int server_id);
    void SetSocket();

    void SetLocalId(int server_id);
    int GetLocalId();
};

#endif // end of #ifndef __NODE_STUB_H__
