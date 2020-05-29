package message;

import util.NodeAddress;

public class LeaveNodeInfoMsg extends Message {

    private byte[] id;
    private int prefixLen;
    private NodeAddress nodeAddress;

    public LeaveNodeInfoMsg(byte[] id, int prefixLen, NodeAddress nodeAddress) {
        this.id = id;
        this.prefixLen = prefixLen;
        this.nodeAddress = nodeAddress;
    }

    public byte[] getID() {
        return id;
    }

    @Override
    public int getMsgType() {
        return LEAVE_NODE_INFO_MSG;
    }

    public int getPrefixLen() {return prefixLen;}

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

}
