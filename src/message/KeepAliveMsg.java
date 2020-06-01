package message;

import util.NodeAddress;


import java.sql.Timestamp;
import java.util.Map;

public class KeepAliveMsg extends Message {
    private byte[] id;
    private NodeAddress nodeAddress;
    private Map<byte[], NodeAddress> leafSet;
    private Timestamp timestamp;


    public KeepAliveMsg(byte[] id, NodeAddress nodeAddress,Map<byte[],NodeAddress> leafSet, Timestamp timestamp){
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.leafSet = leafSet;
        this.timestamp = timestamp;
    }

    public byte[] getID() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public Map<byte[],NodeAddress> getLeafSet() {
        return leafSet;
    }

    public Timestamp getTimestamp(){ return  timestamp;}

    @Override
    public int getMsgType() {
        return KEEP_ALIVE_MSG;
    }
}
