package message;

public class ReadDataMsg extends Message {
    private byte[] id;

    public ReadDataMsg(byte[] id) {
        this.id = id;
    }

    public byte[] getID() {
        return id;
    }

    @Override
    public int getMsgType() {
        return READ_DATA_MSG;
    }
}