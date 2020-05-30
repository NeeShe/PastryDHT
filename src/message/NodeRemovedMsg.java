package message;

public class NodeRemovedMsg extends Message {


    @Override
    public int getMsgType() {
        return NODE_REMOVED_MSG;
    }
}
