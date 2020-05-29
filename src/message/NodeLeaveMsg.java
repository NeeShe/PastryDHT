package message;

import util.NodeAddress;
import util.Util;

import java.util.LinkedList;
import java.util.List;

public class NodeLeaveMsg extends Message{
    private byte[] id;
    private int prefixLength;
    private NodeAddress nodeAddress;

    public NodeLeaveMsg(byte[] id, int prefixLength, NodeAddress nodeAddress) {
        this.id = id;
        this.prefixLength = prefixLength;
        this.nodeAddress = nodeAddress;
    }

    @Override
    public int getMsgType() {
        return NODE_LEAVE_MSG;
    }

    @Override
    public String toString() {
        StringBuilder strBldr = new StringBuilder();
        strBldr.append("ID:" + Util.convertBytesToHex(id));

        return strBldr.toString();
    }

    public byte[] getID() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public int getPrefixLength() { return prefixLength; }
}
