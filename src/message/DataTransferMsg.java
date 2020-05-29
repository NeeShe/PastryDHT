package message;

import util.NodeAddress;

import java.util.HashMap;
import java.util.Map;

public class DataTransferMsg extends Message {
    private byte[] id;
    private NodeAddress nodeAddress;
    public HashMap<String, byte[]> data;

    public DataTransferMsg(byte[] id, NodeAddress nodeAddress, HashMap<String, byte[]> data){
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

    public HashMap<String, byte[]> getData(){ return data;}
    @Override
    public int getMsgType() {
        return DATA_TRANSFER_MSG;
    }
}
