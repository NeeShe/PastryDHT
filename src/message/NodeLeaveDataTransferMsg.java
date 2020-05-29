package message;

public class NodeLeaveDataTransferMsg extends Message {
    private byte[] id;
    

    @Override
    public int getMsgType() {
        return NODE_LEAVE_DATA_TRANSFER_MSG;
    }
}
