package message;

import message.Message;
import util.NodeAddress;

public class NearByNodeInfoMsg extends Message {
    private byte[] id;
    private NodeAddress nodeAddress;

    public NearByNodeInfoMsg(byte[] id, NodeAddress nodeAddress) {
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
        return NEARBY_NODE_INFO_MSG;
    }
}
