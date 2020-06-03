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
    private int requestType; //0-read, 1-write
    public static final int read = 0;
    public static final int write = 1;

    public LookupNodeMsg(byte[] id, NodeAddress nodeAddress, int prefixLength, int requestType) {
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.prefixLength = prefixLength;
        this.hops = new LinkedList();
        this.requestType = requestType;
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

    public int getRequestType(){return requestType;}

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
        strBldr.append(" REQUEST TYPE:"+requestType);
        return strBldr.toString();
    }
}
