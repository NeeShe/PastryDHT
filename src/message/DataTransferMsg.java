package message;

import util.NodeAddress;

import java.util.HashMap;
import java.util.Map;

public class DataTransferMsg extends Message {
    private byte[] id;
    private NodeAddress nodeAddress;
    public HashMap<String, byte[]> data;
    public HashMap<String, byte[]> replicatedData;

    public DataTransferMsg(byte[] id, NodeAddress nodeAddress, HashMap<String, byte[]> data, HashMap<String, byte[]> replicatedData){
        this.id = id;
        this.nodeAddress = nodeAddress;
        this.data = data;
        this.replicatedData = replicatedData;
    }

    public byte[] getID() {
        return id;
    }

    public NodeAddress getNodeAddress() {
        return nodeAddress;
    }

    public HashMap<String, byte[]> getData(){ return data;}

    public HashMap<String, byte[]> getReplicatedData(){ return replicatedData;}
    @Override
    public int getMsgType() {
        return DATA_TRANSFER_MSG;
    }
}
