package message;

import util.NodeAddress;

public class RequestDataMessage extends Message {
    private byte[] id;
    private NodeAddress nodeAddress;

    public RequestDataMessage(byte[] id, NodeAddress nodeAddress){
        this.id = id;
        this.nodeAddress = nodeAddress;
    }

    public byte[] getID() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    @Override
    public int getMsgType() {
        return REQUEST_DATA_MSG;
    }
}
