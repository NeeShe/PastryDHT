package message;

import util.NodeAddress;

import java.util.LinkedList;
import java.util.List;

import static message.Message.LOOKUP_NODE_MSG;
import static util.Util.convertBytesToHex;

public class LookupNodeMsg extends Message{
    private byte[] id;
    private NodeAddress nodeAddress;
    private int prefixLength;
    private List<NodeAddress> hops;

    public LookupNodeMsg(byte[] id, NodeAddress nodeAddress, int prefixLength) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.prefixLength = prefixLength;
        this.hops = new LinkedList();
    }

    public byte[] getID() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public void setLongestPrefixMatch(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public void addHop(NodeAddress nodeAddress) {
        hops.add(nodeAddress);
    }

    @Override
    public int getMsgType() {
        return LOOKUP_NODE_MSG;
    }

    @Override
    public String toString() {
        StringBuilder strBldr = new StringBuilder();
        strBldr.append("ID:" + convertBytesToHex(id) + " HOPS:" + hops.size() + " ");
        strBldr.append("PATH:" + nodeAddress.toString());
        for(NodeAddress nodeAddress : hops) {
            strBldr.append(" -> " + nodeAddress.toString());
        }

        return strBldr.toString();
    }
}
