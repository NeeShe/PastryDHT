package message;

import util.NodeAddress;

import java.sql.Timestamp;
import java.util.Map;

public class NodeFailNotifyMsg extends Message{
    private byte[] id;

    public NodeFailNotifyMsg(byte[] id){
        this.id = id;
    }

    public byte[] getID() {
        return id;
    }

    @Override
    public int getMsgType() {
        return NODE_FAIL_NOTIFY_MSG;
    }
}
