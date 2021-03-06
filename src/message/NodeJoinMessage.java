package message;

import util.NodeAddress;
import util.Util;

import java.util.LinkedList;
import java.util.List;

public class NodeJoinMessage extends Message{
    private byte[] id;
    private int prefixLength;
    private NodeAddress nodeAddress;
    private List<NodeAddress> hops;

    public NodeJoinMessage(byte[] id, int prefixLength, NodeAddress nodeAddress) {
        this.id = id;
        this.prefixLength = prefixLength;
        this.nodeAddress = nodeAddress;
        hops = new LinkedList();
    }

    public byte[] getID() {
        return id;
    }

    public void setLongestPrefixMatch(int prefixLength) {
        System.out.println("setting the prefixLength of node join msg");
        this.prefixLength = prefixLength;
        System.out.println("done setting");
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public void addHop(NodeAddress nodeAddress) {
        hops.add(nodeAddress);
    }

    public boolean hopContains(NodeAddress nodeAddress) {
        return hops.contains(nodeAddress);
    }

    public int getHopsSize(){
        return hops.size();
    }

    @Override
    public int getMsgType() {
        return NODE_JOIN_MSG;
    }

    @Override
    public String toString() {
        StringBuilder strBldr = new StringBuilder();
        strBldr.append("ID:" + Util.convertBytesToHex(id) + " HOPS:" + hops.size() + " ");
        strBldr.append("PATH:" + nodeAddress.toString());
        for(NodeAddress nodeAddress : hops) {
            strBldr.append(" -> " + nodeAddress.toString());
        }

        return strBldr.toString();
    }
}
