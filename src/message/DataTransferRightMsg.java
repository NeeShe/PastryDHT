package message;

import util.NodeAddress;

import java.util.Map;

public class DataTransferRightMsg extends Message {
    private byte[] id;
    private NodeAddress nodeAddress;
    public Map<String, byte[]> data;

    public DataTransferRightMsg(byte[] id, NodeAddress nodeAddress, Map<String, byte[]> data){
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.data = data;
    }

    public byte[] getID() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public Map<String, byte[]> getData(){ return data;}

    @Override
    public int getMsgType() {
        return DATA_TRANSFER_RIGHT_MSG;
    }
}
