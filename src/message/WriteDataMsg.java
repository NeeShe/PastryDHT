package message;

public class WriteDataMsg extends Message {
    private byte[] id;
    private byte[] data;

    public WriteDataMsg(byte[] id, byte[] data) {
        this.id = id;
        this.data = data;
    }

    public byte[] getID() {
        return id;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public int getMsgType() {
        return WRITE_DATA_MSG;
    }
}