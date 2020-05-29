package message;

import data.DataStore;

import java.util.Map;

public class NodeLeaveDataTransferMsg extends Message {
    private byte[] id;

    private DataStore data;

    public NodeLeaveDataTransferMsg(byte[] id, DataStore data) {
        this.id = id;
        this.data = data;
    }

    public Map<String, byte[]> getOwnedData() {
        return this.data.ownedData;
    }

    public Map<String, byte[]> getReplicatedData() {
        return this.data.replicatedData;
    }

    @Override
    public int getMsgType() {
        return NODE_LEAVE_DATA_TRANSFER_MSG;
    }
}
